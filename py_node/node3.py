import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.compute as compute

def filterPower(table, power):
    return table.filter(compute.greater(table['power'], 8))

class FlightServer(fl.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8817", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        print("Running in port 8817...")
        self.full_table = None

    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all();
        table_filtered = filterPower(table, 8)
        if self.full_table is None:
            self.full_table = table_filtered
        else:
            self.full_table = pa.concat_tables([self.full_table, table_filtered])
        print("=========TABLE========")
        print(self.full_table)
        print("======================")

def main():
    FlightServer().serve()

if __name__ == '__main__':
    main()