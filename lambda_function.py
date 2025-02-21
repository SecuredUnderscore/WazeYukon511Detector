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
uuids = []

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

    await do_full_api_check(-128.4, -122.6, 51.0, 48.0)
    await do_full_api_check(-122.6, -116.8, 51.0, 48.0)
    await do_full_api_check(-128.4, -122.6, 54.0, 51.0)
    await do_full_api_check(-122.6, -116.8, 54.0, 51.0)
    await do_full_api_check(-128.4, -122.6, 57.0, 54.0)
    await do_full_api_check(-122.6, -116.8, 57.0, 54.0)
    await do_full_api_check(-128.4, -122.6, 60.0, 57.0)
    await do_full_api_check(-122.6, -116.8, 60.0, 57.0)

    # for uuid in uuids:
    #     print(f"Removing {uuid}")
    #     table_active.delete_item(Key={'uuid': uuid})

    await send_log("Script Completed")

async def do_full_api_check(left, right, top, bottom):
    try:
        url = f"https://www.waze.com/live-map/api/georss?env=na&types=traffic&left={left}&top={top}&right={right}&bottom={bottom}"
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
    await check_for_yukon(parsed_api)

async def check_for_yukon(parsed_api):
    try:
        jams = parsed_api['jams']
    except KeyError:
        return

    for event in jams:
        try:
            uuid = str(event['causeAlert']['id'])
            reporter = event['causeAlert']['reportBy']

            if reporter == "Yukon 511":
                print(f"Found Yukon 511 for {uuid}")
                if not uuid in uuids:
                    print(f"Adding {uuid}")
                    table_active.put_item(Item={'uuid': str(uuid)})
                    await send_webhook(event)
                # try:
                #     uuids.remove(uuid)
                # except ValueError:
                #     pass
        except KeyError:
            pass

async def send_webhook(event):
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=f"New Yukon511 Closure")
        embed.add_field(name="Road", value=event['street'])

        embed.add_field(name="Links", value=f"https://www.waze.com/en-US/editor?env=usa&lat={event['causeAlert']['location']['y']}&lon={event['causeAlert']['location']['x']}&zoomLevel=16&segments={event['segments'][0]['ID']}")
        webhook = discord.Webhook.from_url(discord_webhook_url, session=session)
        await webhook.send(embed=embed)

async def send_log(text):
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=text)
        webhook = discord.Webhook.from_url(discord_webhook_log_url, session=session)
        await webhook.send(embed=embed)

if __name__ == "__main__":
    # asyncio.run(send_log("Script started"))
    asyncio.run(start())

    # while True:
    #     asyncio.run(start())
    #     time.sleep(check_feed_delay)