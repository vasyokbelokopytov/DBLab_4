import os
import csv
import logging
import datetime

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from config import config

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="logs.log",
    level=logging.INFO,
    format="%(asctime)s: %(message)s"
)

def query(data_coll: Collection, filename):
    logger.info("Query execution...")
    print("Query execution...")
    query = data_coll.aggregate(
        [
            {"$match": {"UkrTestStatus": "Зараховано"}},
            {"$group": {"_id": {"year": "$year",
                                "region": "$REGNAME"},
                        "UkrMinBall": {"$min": "$UkrBall100"}}},
        ]
    )

    with open(filename, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["year", "region", "UkrMinBall"])
        writer.writeheader()
        for row in query:
            row["year"] = row["_id"]["year"]
            row["region"] = row["_id"]["region"]
            del row["_id"]
            print(row)
            writer.writerow(row)
    logger.info(f"Query data was recorded.")
    print("Query data was recorded.")


def create_rows_counter_collection(db: Database):
    collections = db.list_collection_names()
    if "counter" not in collections:
        rows_counter_coll = db["counter"]
        rows_counter_coll.insert_one({"row": 0})
    return 0


def get_formatted_row(row, year):
    row["year"] = year
    for key in row:
        try:
            new_value = float(row[key].replace(",", "."))
            row[key] = new_value
        except Exception:
            pass
    return row


def insert_data(data_coll: Collection, rows_counter_coll: Collection, filename, current_row, chunk_size):
    print(f"Inserting data from {filename}...")
    start_time = datetime.datetime.now()
    year = int(filename[5:9])
    logger.info(f"Inserting data from file for with year {year}")

    with open(config["data_path"] + filename, encoding="windows-1251") as f:
        reader = csv.DictReader(f, delimiter=';')

        i = 0
        chunk = []
        for row in reader:
            i += 1
            if i <= current_row:
                continue

            formatted_row = get_formatted_row(row, year)
            chunk.append(formatted_row)

            if i % chunk_size == 0:
                try:
                    if(chunk):
                        data_coll.insert_many(chunk)
                        rows_counter_coll.update_one({}, {
                            "$set": {
                                "row": i
                            }
                        })
                except Exception as e:
                    logger.info("Database is shutted down.")
                    raise e
                chunk = []

    if i % chunk_size != 0:
        try:
            if(chunk):
                data_coll.insert_many(chunk)
                rows_counter_coll.update_one({}, {
                    "$set": {
                        "row": i
                    }
                })
        except Exception as e:
            logger.info("Database is shutted down.")
            raise e

    end_time = datetime.datetime.now()
    rows_counter_coll.update_one({}, {"$set": {"row": 0}})
    logger.info(f"Inserting {filename} data is finished in {end_time-start_time}")


def main():
    start_time = datetime.datetime.now()
    logger.info(f"Start time {start_time}")

    client = MongoClient(port=config["port"])
    db = client.zno_data

    data_coll = db.data

    create_rows_counter_collection(db)
    rows_counter_coll = db.counter
    rows_counter_coll_values = rows_counter_coll.find_one()

    insert_data(data_coll, rows_counter_coll, "Odata2019File.csv", rows_counter_coll_values["row"], chunk_size=100)

    insert_data(data_coll, rows_counter_coll, "Odata2020File.csv", rows_counter_coll_values["row"], chunk_size=100)


    end_time = datetime.datetime.now()
    logger.info(f"End time {end_time}")
    logger.info(f"All files Inserting time {end_time - start_time}")

    query(data_coll, "result.csv")

    client.close()

    logger.info("Program is finished")

if __name__ == "__main__":
    main()