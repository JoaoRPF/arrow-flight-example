import argparse
import time

import pyarrow as pa
import pyarrow.compute as compute

from pyarrow import flight as fl

def create_table_marvel():
    data = [
        pa.array(["Thor", "Spiderman", "Captain America", "Ironman", "Black Widow", "Wanda"]),
        pa.array(["hammer", "web", "shield", "tech", None, "some magic stuff"]),
        pa.array([9, 6, 8, 7, 5, 10]),
    ]
    return pa.Table.from_arrays(data, names=["name", "weapon", "power"])

def main():

    client = fl.connect("grpc://0.0.0.0:8816")
    table = create_table_marvel()
    print("=========TABLE DATA========")
    print(table)
    print("===========================")
    sleepT(2)

    writer, _ = client.do_put(fl.FlightDescriptor.for_path("superheroes"), table.schema)

    print("Starting to send batches...")
    batches = table.to_batches(max_chunksize=6)
    for batch in batches:
        print("========BATCH==========")
        print(batch.to_pydict())
        sleepT(1)
        writer.write_batch(batch)

    writer.close()
    print("no more records to send...")

def sleep():
    sleepT(1)

def sleepT(t):
    print("Sleeping for " + str(t) + " secs...")
    time.sleep(t)


if __name__ == '__main__':
    main()