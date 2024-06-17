import os
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor
import csv
import json
from datetime import datetime
import extract
from argparse import ArgumentParser


def get_connection(config) -> connection:
    return psycopg2.connect(user=config.get("DATABASE_USERNAME"),
                            password=config.get("DATABASE_PASSWORD"),
                            host=config.get("DATABASE_IP"),
                            port=config.get("DATABASE_PORT"),
                            database=config.get("DATABASE_NAME"))
    # return psycopg2.connect("dbname=museum user=punimashahi host=localhost")


def get_cursor(conn: connection) -> cursor:
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def load_hist_data(file_path: str, n: int) -> list[dict]:
    """Loading csv file data"""
    rows = []
    with open(file_path) as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            rows.append(row)
            if len(rows) == n:
                break
    print(rows)
    return rows


def load_json(folder_path: str) -> list[dict]:
    """Loading all exhibition json files and appending it to a list."""
    rows = []
    files = [file_name for file_name in os.listdir(
        folder_path) if '.json' in file_name]
    for file in files:
        with open(f'{folder_path}/{file}') as json_file:
            data = json.load(json_file)
            rows.append(data)
    return rows


def get_floor_ids(cur: cursor) -> dict:
    cur.execute("SELECT * FROM floors;")
    floors = cur.fetchall()
    floor_dict = {n[1]: n[0]for n in floors}
    return floor_dict


def get_department_ids(cur: cursor) -> dict:
    cur.execute("SELECT * FROM departments;")
    departments = cur.fetchall()
    department_dict = {n[1]: n[0]for n in departments}
    return department_dict


def insert_exhibition_table(conn: connection, cur: cursor, floor_dict: dict, department_dict: dict, data: list[dict]):
    """Insert exhibition data into exhibition table"""
    for exhibit in data:
        # get the floor id, department id
        floor_id = floor_dict[exhibit["FLOOR"]]
        department_id = department_dict[exhibit["DEPARTMENT"]]
        exhibit_id = int(exhibit["EXHIBITION_ID"][-2:])
        start_date = datetime.strptime(
            exhibit["START_DATE"], "%d/%m/%y").date()
        query = """
                INSERT INTO exhibitions(exhibition_id, exhibition_name, 
                floor_id, department_id, start_date, description)
                VALUES(%s,%s,%s,%s,%s,%s);
                """
        cur.execute(query,
                    (exhibit_id, exhibit["EXHIBITION_NAME"], floor_id,
                     department_id, start_date, exhibit["DESCRIPTION"]))
    conn.commit()


def insert_hist_values(conn: connection, cur: cursor, hist_data: list[dict]):
    for row in hist_data:
        score_id = int(row['val'])
        exhibit_id = int(row['site'])
        date_time = datetime.strptime(
            row['at'], "%Y-%m-%d %H:%M:%S") if isinstance(row['at'], str) else row['at']
        if score_id == -1:
            score_id = int(float(row['type']))
            query = """
                    INSERT INTO special_requests(help_type_id, 
                    exhibition_id, date)
                    VALUES (%s, %s, %s);
                    """
        else:
            query = """
                    INSERT INTO rating(score_id, exhibition_id, date)
                    VALUES (%s, %s, %s);
                    """
        cur.execute(query,
                    (score_id, exhibit_id, date_time))
    conn.commit()


"""
To upload the data into the database, For lmnh hist data, we have to split it.
One is dict of data for score review table, the other is my special request table.
1. Way is Row by row, if the value is -1, add insert data to special request, else insert data to score review
2. Split the dict of rows into two dict, one for score review and other for special request, then insert it into their respective table.
"""


def main(logger: logging.Logger, filepath: dict, n: int, config):

    hist = load_hist_data(
        f'{filepath["hist"]}/lmnh_hist_data_merged.csv', n)

    exhibition = load_json(filepath["exhibit"])
    logger.info("Data imported to the program")

    conn = get_connection(config)
    logger.info("Connected to the database")
    cur = get_cursor(conn)
    logger.info("Cursor created")
    floors = get_floor_ids(cur)
    departments = get_department_ids(cur)
    insert_exhibition_table(conn, cur, floors, departments, exhibition)
    logger.info("Exhibition data inserted to the table")
    insert_hist_values(conn, cur, hist)
    logger.info("Kiosk data inserted to the table")
    cur.close()
    logger.info("Cursor closed")
    conn.close()
    logger.info("Connection closed")


def cl_args() -> dict:
    """Returns command line argument values"""
    parser = ArgumentParser()
    parser.add_argument(
        "-b", "--bucket", nargs="?", help="enter bucket name")
    parser.add_argument(
        "-r", "--row", nargs="?", default='-1', help="enter no. or rows to upload to database")
    parser.add_argument(
        "-f", "--file", action='store_true', help="True for logs to be stored in a file")
    return vars(parser.parse_args())


def log_setup(to_file: bool) -> logging.Logger:
    """Set up logging."""
    if to_file:
        logging.basicConfig(filename='myapp.log', level=logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)


def initial_setup(config, logger: logging.Logger) -> dict:
    """Sets up files and returns the filepath of the folders"""
    fp_info = {"hist": config.get("HIST_FOLDER_PATH"),
               "exhibit": config.get("EXHIBITION_FOLDER_PATH")}
    s3_client = extract.get_client(config)
    extract.main(fp_info["hist"], fp_info["exhibit"], bucket_name, s3_client)
    logger.info("All files downloaded")
    return fp_info


if __name__ == "__main__":

    args = cl_args()
    print(args)
    log_to_file = args['file']
    bucket_name = args['bucket']

    logger = log_setup(log_to_file)

    try:
        num_rows = int(args['row'])
    except ValueError as e:
        logger.exception("Invalid integer")

    # Running extract script

    config = extract.get_config()
    files = initial_setup(config, logger)

    try:
        main(logger, files, num_rows, config)
    except FileNotFoundError as e:
        logger.exception(e)
    except psycopg2.OperationalError as ops:
        logger.exception(ops)
    else:
        logger.info("Program finished")

    # configs = extract.get_config()
    # conn = get_connection(configs)
    # cur = get_cursor(conn)

    # cur.execute(
    #     "SELECT * FROM rating ORDER BY rating_id DESC LIMIT 5;")
    # result = cur.fetchall()
    # print(result)
