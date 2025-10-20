# youll need to set up your cassandra in Astra db and the kafka connector explained in confluence_setup folder to run this script
# downoad the secure connect bundle from astra db and set the path in the .env file
# you can find instructions here 
# https://docs.datastax.com/en/astra-db-classic/databases/secure-connect-bundle.html#:~:text=SCB%20never%20expires.-,Download%20SCBs%20with%20the%20Astra%20Portal,an%20archive%20(zip%20file).

from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import time
import os
from dotenv import load_dotenv
import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s|%(levelname)s|%(name)s|%(message)s',
    handlers = [
        logging.FileHandler(f'consume_to_cassandra_{datetime.now().strftime('%Y%m%d')}.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
# 
ASTRA_DB_BUNDLE = os.getenv("ASTRA_DB_BUNDLE")
KEY_SPACE = os.getenv("KEY_SPACE")

#kafka set up
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET")

logging.info("Connecting to Astra db...\n")



def consume_and_load():
    cloud_config = {'secure_connect_bundle': ASTRA_DB_BUNDLE}
    auth_provider = PlainTextAuthProvider('token', ASTRA_DB_TOKEN)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect(KEY_SPACE)

    logging.info("Successfully connected")

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS air_quality_data (
        city text,
        timestamp timestamp,
        pm2_5 double,
        pm10 double,
        ozone double,
        carbon_monoxide double,
        nitrogen_dioxide double,
        sulphur_dioxide double,
        uv_index double,
        PRIMARY KEY (city, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    session.execute(create_table_query)
    logging.info("Table air_quality_data ready")


    # define the insert query to reuse later
    insert_query = """
    INSERT INTO air_quality_data(
    city, timestamp, pm2_5, pm10, ozone, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, uv_index
    ) VALUES (?,?,?,?,?,?,?,?,?)
    """

    prepared_statement = session.prepare(insert_query)

    conf = {
        "bootstrap.servers" : KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        'sasl.username': CONFLUENT_API_KEY,
        'sasl.password': CONFLUENT_API_SECRET,
        "group.id": "airquality-consumer-group",
        "auto.offset.reset": "earliest",
        }

    consumer  = Consumer(conf)

    topic = "air_quality.city_air_quality.air_quality_data"
    consumer.subscribe([topic])

    logging.info(f"Listening to messages at {topic}")


    while True:
        msg = consumer.poll(1.0)
        logging.info(f"message received as ")
        if msg is None:
            continue
        if msg.error():
            logging.info(f"Kafka error: {msg.error()}")
            continue
        try:
            logging.info("Message received: loading into cassandra")
            print(msg.value())
            # decode the json value
            data = json.loads(msg.value().decode("utf-8"))
            data = data["fullDocument"]
            #city, timestamp, pm2_5, pm10, ozone, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, uv_index
            values = (
                data["city"],
                data["timestamp"],
                data["pm2_5"],
                data["pm10"],
                data["ozone"],
                data["carbon_monoxide"],
                data["nitrogen_dioxide"],
                data["sulphur_dioxide"],
                data["uv_index"],
            )

            session.execute(prepared_statement, values)

            logging.info(f'inserted data for city {data["city"]} at {data["timestamp"]}')

            

        except Exception as e:
            logging.info(f"there was an error logging into cassandra : {e}")
            logging.info(f"Failed values: {values}")
        time.sleep(1)

