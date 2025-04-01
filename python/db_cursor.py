import os


class DBCursor:
    def __init__(self, connection):
        self.connection = connection
        self.batch_results = None
        self.current_result = None
        self.row_stream = None

    def execute(self, query):
        self.batch_results = self.connection.execute_sync(query)
        self.current_result = None
        self.row_stream = None

    def _ensure_result(self):
        if self.current_result is None:
            self.current_result = self.batch_results.stream().next_result()
            if self.current_result and self.current_result.has_resultset():
                self.row_stream = self.current_result.get_row_stream()

    def fetchone(self):
        self._ensure_result()
        if self.row_stream:
            row = self.row_stream.next()
            if row:
                return [row.get_value()]
        return None

    def fetchall(self):
        # if self.current_result is None:
        stream = self.batch_results.stream()

        result = stream.next_result()
        if result is None:
            return None
        results = []
        with result as res:
            if not res:
                return results
            if res.has_resultset():
                with res.get_row_stream() as row_stream:
                    row = row_stream.next()
                    while row is not None:
                        results.append(row.get_value())
                        row = row_stream.next()
        return results

    def close(self):
        self.current_result = None
        self.row_stream = None

# Usage example
if __name__ == "__main__":
    import pytdslib

    ctx = pytdslib.PyClientContext(
        server_name="saurabhsingh.database.windows.net",
        port=1433,
        user_name="saurabh",
        password=os.environ.get("AZURE_SQL_PASSWORD"),
        database="master"
    )

    conn = pytdslib.create_connection_sync(ctx)
    cursor = DBCursor(conn)
    cursor.execute("SELECT * FROM sys.databases;")
    print(cursor.fetchall())
    cursor.close()
