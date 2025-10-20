import requests
import pymongo
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import schedule
import time


logging.basicConfig(
    level= logging.INFO,
    format = '%(asctime)s | %(levelname)s | %(name)s| %(message)s',
    handlers=[
        logging.FileHandler(f"fetch_store_logs_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

PARAMS = {
    'hourly': 'pm2_5,pm10,ozone,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,uv_index',
    "cities": {
        "nairobi": {"lat": -1.286389, "lon": 36.817223},
        "mombasa": {"lat": -4.043477, "lon": 39.668206},
    }
}

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality" 

client = pymongo.MongoClient(MONGO_URI)
db = client.city_air_quality
collection = db.air_quality_data



def fetch_and_store():
    # replace the minutes and seconds to only get the rime now 
    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)


    logging.info(f'Getting the weather data at {current_hour}')

    for city, coords in PARAMS["cities"].items():
        logging.info(f'Getting data for {city}')
        
        params = {}
        params["hourly"] = PARAMS["hourly"]
        params["latitude"] = coords["lat"]
        params["longitude"] = coords["lon"]
        response = requests.get(BASE_URL, params=params )

        if response.status_code != 200:
            logging.debug("failed with the url")
            return
        
        data = response.json()
        logging.info(f"Retreived data {data}")
    
        logging.info("Simulating an hour result of data")
        
        for i, timestamp in enumerate(data["hourly"]["time"]):

            record_time = datetime.fromisoformat(timestamp)

            if current_hour == record_time:
                record = {
                    "city": city,
                    "timestamp": record_time,
                    "pm2_5": data["hourly"]["pm2_5"][i],
                    "pm10": data["hourly"]["pm10"][i],
                    "ozone": data["hourly"]["ozone"][i],
                    "carbon_monoxide": data["hourly"]["carbon_monoxide"][i],
                    "nitrogen_dioxide": data["hourly"]["nitrogen_dioxide"][i],
                    "sulphur_dioxide": data["hourly"]["sulphur_dioxide"][i],
                    "uv_index": data["hourly"]["uv_index"][i],
                }

                # insert only when the record doesnt exist 
                if not collection.find_one({"city": city,"timestamp": record_time}):
                    collection.insert_one(record)
                    logging.info(f"inserted record {record}")
                else:
                    logging.info(f'the record already exists try again in the next hour ')


# --- Schedule every hour ---
schedule.every(1).hours.do(fetch_and_store)

# Run immediately at start
fetch_and_store()

# Keep running
while True:
    schedule.run_pending()
    time.sleep(600)