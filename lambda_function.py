import asyncio
import discord
import aiohttp
import requests
import os
import boto3

check_feed_delay = 60
feed_url = "https://www.waze.com/live-map/api/georss?env=na&ma=600&mj=100&mu=100&types=traffic&left=-133&top=53&right=-114&bottom=48"
feed_url2 = "https://www.waze.com/live-map/api/georss?env=na&ma=600&mj=100&mu=100&types=traffic&left=-133&top=58&right=-114&bottom=53"
discord_webhook_url = os.environ['DISCORD_WEBHOOK_URL']
discord_webhook_log_url = os.environ['DISCORD_WEBHOOK_LOG_URL']
pushover_api_token = os.environ['PUSHOVER_API_TOKEN']
pushover_user_key = os.environ['PUSHOVER_USER_KEY']
uuids = []
road_names = ["Hwy 1", "Hwy 4", "Hwy 14", "Hwy 17", "Hwy 18", "Hwy 19", "Hwy 4a", "Hwy 1a", "Nanaimo Pky", "Sooke Rd"]
reporter_whitelist = ["District of Oak Bay", "City of Langford", "RyckNe", "Trexer0", "ThatVictoriaGuy", "BryceCampbell"]

def lambda_handler(event, context):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start())
    finally:
        loop.close()
    return {
        'statusCode': 200,
        'body': 'Lambda function executed successfully'
    }

async def start():
    # Create database clients
    dynamodb = boto3.resource('dynamodb')
    database_name_active = os.environ['DB_NAME_ACTIVE']
    global table_active
    table_active = dynamodb.Table(database_name_active)

    uuids2: dict = table_active.scan()['Items']
    for uuid in uuids2:
        uuids.append(uuid.get('uuid'))

    # await do_full_api_check(-122.333, -122.308, 37.934, 37.915)
    # await do_full_api_check(-128.4, -122.6, 51.0, 48.0)
    # await do_full_api_check(-122.6, -116.8, 51.0, 48.0)
    # await do_full_api_check(-128.4, -122.6, 54.0, 51.0)
    # await do_full_api_check(-122.6, -116.8, 54.0, 51.0)
    # await do_full_api_check(-128.4, -122.6, 57.0, 54.0)
    # await do_full_api_check(-122.6, -116.8, 57.0, 54.0)
    # await do_full_api_check(-128.4, -122.6, 60.0, 57.0)
    # await do_full_api_check(-122.6, -116.8, 60.0, 57.0)

    # South Island
    await do_full_api_check(-124.6, -123.3, 48.3, 49.0)
    await do_full_api_check(-125.7, -124.4, 48.3, 49.0)

    # Mid Island
    await do_full_api_check(-124.6, -123.3, 49.0, 49.7)
    await do_full_api_check(-125.7, -124.4, 49.0, 49.7)

    # North Island
    await do_full_api_check(-126.0, -124.4, 49.7, 50.3)
    await do_full_api_check(-128.0, -126.0, 49.7, 50.3)

    # for uuid in uuids:
    #     print(f"Removing {uuid}")
    #     table_active.delete_item(Key={'uuid': uuid})

    await send_log("Script Completed")

async def do_full_api_check(left, right, top, bottom):
    try:
        url = f"https://www.waze.com/live-map/api/georss?env=na&format=1&types=traffic,alerts&acotu=true&left={left}&top={top}&right={right}&bottom={bottom}"
        print(f"Loading {url}")
        api = requests.get(url)
    except ConnectionError:
        await send_log("Unable to connect to api")
        return

    print(api.status_code)
    if api.status_code != 200:
        await send_log(f"API Response Code {api.status_code}")
        return

    parsed_api = api.json()
    print(parsed_api)
    await check_for_yukon(parsed_api)

async def check_for_yukon(parsed_api):
    try:
        jams = parsed_api['alerts']
    except KeyError:
        return

    for event in jams:
        try:
            uuid = event.get('id', None)
            event_type = event.get('type', None)
            road_type = event.get('roadType', None)
            street_name = event.get('street', None)
            city = event.get('city', None)
            reporter = event.get('reportBy', None)

            # if event_type == "ACCIDENT" and (road_type == 2 or road_type == 3 or road_type == 4 or road_type == 1): # Freeways or Ramps or Primary Streets
            if event_type == "ACCIDENT" and any(name.lower() in street_name.lower() for name in road_names): # Freeways or Ramps or Primary Streets
                if not uuid in uuids:
                    print(f"Adding {uuid}")
                    table_active.put_item(Item={'uuid': str(uuid)})
                    await send_webhook(event, 0, f"Crash at {street_name}, {city}")

            if event_type == "ROAD_CLOSED" and reporter not in reporter_whitelist and (road_type == 3 or road_type == 4 or road_type == 6 or road_type == 7):
                if not uuid in uuids:
                    print(f"Adding {uuid}")
                    uuids.append(uuid)
                    table_active.put_item(Item={'uuid': str(uuid)})
                    await send_webhook(event, 1, f"New user closure: {street_name}, {city} ({roadtype_to_string(road_type)})")
        except KeyError:
            pass

def roadtype_to_string(road_type: int) -> str:
    if road_type == 1:
        return "Street"
    elif road_type == 2:
        return "Primary Street"
    elif road_type == 3:
        return "Freeway"
    elif road_type == 4:
        return "Ramp"
    elif road_type == 5:
        return "Trail"
    elif road_type == 6:
        return "Major Highway"
    elif road_type == 7:
        return "Minor Highway"
    elif road_type == 17:
        return "Private Road"
    elif road_type == 20:
        return "Parking lot road"
    else:
        return "Unknown"

async def send_webhook(event, notify_type, title=""):
    async with aiohttp.ClientSession() as session:
        data = {
            "token": pushover_api_token,
            "user": pushover_user_key,
            "title": f"{title}",
            "message": f"WME: https://www.waze.com/en-US/editor?env=usa&lat={event['location']['y']}&lon={event['location']['x']}&zoomLevel=16, Livemap: https://www.waze.com/live-map/directions?to=ll.{event['location']['y']}%2C{event['location']['x']}",
            "priority": 1 if notify_type == 1 else 0,
            "sound": "climb" if notify_type == 1 else "cosmic",
        }

        await session.post("https://api.pushover.net/1/messages.json", data=data)

        embed = discord.Embed(title=title)
        if notify_type == 0:
            embed.add_field(name="Links",
                            value=f"https://www.waze.com/en-US/editor?env=usa&lat={event['location']['y']}&lon={event['location']['x']}&zoomLevel=16")
        webhook = discord.Webhook.from_url(discord_webhook_url, session=session)
        await webhook.send(embed=embed)

async def send_log(text):
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=text)
        webhook = discord.Webhook.from_url(discord_webhook_log_url, session=session)
        await webhook.send(embed=embed)

# if __name__ == "__main__":
    # asyncio.run(send_log("Script started"))
    # asyncio.run(start())

    # while True:
    #     asyncio.run(start())
    #     time.sleep(check_feed_delay)