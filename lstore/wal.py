import os
import json

class WriteAheadLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.buffer = []

    def write(self, operation, data):
        self.buffer.append((operation, data))

# functino to read in and flush after use immediately
    def flush(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                json.dump([], f)

        with open(self.log_file, 'r') as f:
            logs = json.load(f)

        logs.extend(self.buffer)

        with open(self.log_file, 'w') as f:
            json.dump(logs, f)

        self.buffer = []

    def replay(self, callback):
        with open(self.log_file, 'r') as f:
            logs = json.load(f)

        for operation, data in logs:
            callback(operation, data)


    # possible example usage
wal = WriteAheadLogger('database_wal.log')
wal.write('insert', {'id': 1, 'name': 'John'})
wal.flush() # query or transaction should create wal and use