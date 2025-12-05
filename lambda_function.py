import asyncio
import time
import discord
import aiohttp
import requests
import os
import boto3
import json
from botocore.exceptions import ClientError

check_feed_delay = 60
feed_url = "https://www.waze.com/live-map/api/georss?env=na&ma=600&mj=100&mu=100&types=traffic&left=-133&top=53&right=-114&bottom=48"
feed_url2 = "https://www.waze.com/live-map/api/georss?env=na&ma=600&mj=100&mu=100&types=traffic&left=-133&top=58&right=-114&bottom=53"
discord_webhook_url = os.environ['DISCORD_WEBHOOK_URL']
discord_webhook_log_url = os.environ['DISCORD_WEBHOOK_LOG_URL']
pushover_api_token = os.environ['PUSHOVER_API_TOKEN']
pushover_user_key = os.environ['PUSHOVER_USER_KEY']
uuids = []
collected_alerts = []
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

    global collected_alerts
    collected_alerts = []

    uuids2: dict = table_active.scan()['Items']

    uuids2: dict = table_active.scan()['Items']
    for uuid in uuids2:
        uuids.append(uuid.get('uuid'))

    await do_full_api_check(-122.333, -122.308, 37.934, 37.915, False)
    await do_full_api_check(-128.4, -122.6, 51.0, 48.0, False)
    await do_full_api_check(-122.6, -116.8, 51.0, 48.0, False)
    await do_full_api_check(-128.4, -122.6, 54.0, 51.0, False)
    await do_full_api_check(-122.6, -116.8, 54.0, 51.0, False)
    await do_full_api_check(-128.4, -122.6, 57.0, 54.0, False)
    await do_full_api_check(-122.6, -116.8, 57.0, 54.0, False)
    await do_full_api_check(-128.4, -122.6, 60.0, 57.0, False)
    await do_full_api_check(-122.6, -116.8, 60.0, 57.0, False)

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

    #     table_active.delete_item(Key={'uuid': uuid})

    update_s3_geojson(collected_alerts)

    await send_log("Script Completed")

async def do_full_api_check(left, right, top, bottom, alert=True):
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

    global collected_alerts
    if 'alerts' in parsed_api:
        collected_alerts.extend(parsed_api['alerts'])
    print(parsed_api)
    if alert:
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

def update_s3_geojson(new_alerts):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    file_key = os.environ.get('S3_FILE_KEY', 'waze_alerts.geojson')

    if not bucket_name:
        print("S3_BUCKET_NAME not set, skipping S3 update.")
        return

    # Try to load existing GeoJSON
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        geojson_content = response['Body'].read().decode('utf-8')
        geojson_data = json.loads(geojson_content)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            geojson_data = {"type": "FeatureCollection", "features": []}
        else:
            print(f"Error reading S3: {e}")
            return
    except Exception as e:
        print(f"Error parsing S3 content: {e}")
        geojson_data = {"type": "FeatureCollection", "features": []}

    # Create a dict for faster lookup by ID
    existing_features = {f['properties']['id']: f for f in geojson_data.get('features', []) if 'properties' in f and 'id' in f['properties']}
    
    current_time = int(time.time() * 1000)
    new_alert_ids = set(a.get('id') for a in new_alerts if a.get('id'))

    for feature_id, feature in existing_features.items():
        if feature_id not in new_alert_ids:
            if 'expired' not in feature['properties']:
                feature['properties']['expired'] = current_time

    updated_count = 0
    new_count = 0
    
    for alert in new_alerts:
        alert_id = alert.get('id')
        if not alert_id:
            continue
            
        # Create Feature
        # Ensure location exists
        if 'location' not in alert:
            continue

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [alert['location']['x'], alert['location']['y']]
            },
            "properties": alert
        }
        
        if alert_id in existing_features:
            existing_features[alert_id] = feature
            updated_count += 1
        else:
            existing_features[alert_id] = feature
            new_count += 1
            
    # Reconstruct FeatureCollection
    geojson_data['features'] = list(existing_features.values())
    
    # Save back to S3
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(geojson_data),
            ContentType='application/json'
        )
        print(f"Updated S3 GeoJSON: {new_count} new, {updated_count} updated.")
    except Exception as e:
        print(f"Error writing to S3: {e}")

if __name__ == "__main__":
    asyncio.run(send_log("Script started"))
    asyncio.run(start())

    # while True:
    #     asyncio.run(start())
    #     time.sleep(check_feed_delay)