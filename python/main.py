import pytdslib
import os

print(pytdslib.__file__)

ctx = pytdslib.PyClientContext(
    server_name="saurabhsingh.database.windows.net",
    port=1433,
    user_name="saurabh",
    password=os.environ.get("AZURE_SQL_PASSWORD"),
    database="master"
)

conn = pytdslib.create_connection_sync(ctx)
batch_results = conn.execute_sync("SELECT * FROM sys.databases; select * from sys.columns;")

stream = batch_results.stream()
while True:
    res = stream.next_result()
    if res is None:
        print("No more results. Exiting program.")
        break


    with res as result:
        print("Got result")
        if not result:
            print(1)
            break
        else:
            if result.has_resultset():
                with result.get_row_stream() as row_stream:
                    row = row_stream.next()
                    while row is not None:
                        print(row.get_value())
                        row = row_stream.next()
            
