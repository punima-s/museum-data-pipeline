"""Stream processing kiosk data from Kafka into the database"""

import logging
import json
from argparse import ArgumentParser
from datetime import datetime
from psycopg2.extensions import connection, cursor
from confluent_kafka import Consumer, KafkaException
from pipeline import insert_hist_values, get_connection, get_cursor
from extract import get_config


def cli_setup() -> bool:
    """takes in optional -f argument for logging into a file option"""
    parser = ArgumentParser()
    parser.add_argument(
        "-f", "--file", action='store_true', help="True for logs to be stored in a file")
    return vars(parser.parse_args())['file']


def log_setup(log_to_file: bool) -> logging.Logger:
    """Set up logging in file or in terminal."""
    if log_to_file:
        logging.basicConfig(filename="log_record.txt", level=logging.INFO,
                            format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)


def valid_message(message: dict) -> str:
    """Returns invalid str message if message is not valid."""
    keys = ["at", "site", "val"]
    key_values = {"site": ["1", "2", "3", "4", "5", "0"],
                  "val": [0, 1, 2, 3, 4, -1],
                  "type": [0, 1]}

    for key in keys:
        if key not in message.keys():
            return f"INVALID: missing {key} values"

        if key in ["site", "val"]:
            if message[key] not in key_values[key]:
                return f"INVALID: {key} values are invalid"

    if message["val"] == -1:
        if "type" not in message.keys():
            return "INVALID: missing type values"
        if message["type"] not in key_values["type"]:
            return "INVALID: type values are invalid"
    return message


def get_kafka_config(config: dict) -> dict:
    """Returns config for Consumer instance"""
    return {'bootstrap.servers': config.get('BOOTSTRAP_SERVERS'),
            'security.protocol': config.get('SECURITY_PROTOCOL'),
            'sasl.mechanisms': config.get('SASL_MECHANISM'),
            'sasl.username': config.get('USERNAME'),
            'sasl.password': config.get('PASSWORD'),
            'group.id': config.get('GROUP'),
            'auto.offset.reset': config.get('AUTO_OFFSET')}


def format_date(message: dict) -> list:
    """Changes the format and type of datetime str in a message."""
    message['at'] = datetime.strptime(
        message['at'], "%Y-%m-%dT%H:%M:%S.%f%z").replace(microsecond=0, tzinfo=None)
    return [message]


def main(consumer: Consumer, logger: logging.Logger, conn: connection, cur: cursor) -> None:
    """Consuming data from kafka and inserting it into the database tables."""
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            message = json.loads(msg.value().decode())
            result = valid_message(message)
            if "INVALID" in result:
                logger.error(
                    f"Message: {message}, Reason: {result}")
            else:
                insert_hist_values(conn, cur, format_date(message))
    except KeyboardInterrupt:
        print(KeyboardInterrupt)
    finally:
        consumer.close()
        cur.close()
        conn.close()


if __name__ == "__main__":

    to_file = cli_setup()
    logs = log_setup(to_file)
    configs = get_config()
    kafka_config = get_kafka_config(configs)

    consume = Consumer(kafka_config)

    consume.subscribe([configs.get('TOPIC')])
    conn_to_db = get_connection(configs)
    cur_for_db = get_cursor(conn_to_db)
    main(consume, logs, conn_to_db, cur_for_db)
