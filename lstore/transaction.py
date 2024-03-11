from lstore.table import Table, Record
from lstore.index import Index
from threading import Lock
import threading



class MyLock:
    def __init__(self):
        self.shared_locks = 0  # Number of shared locks held
        self.exclusive_lock = False  # Whether an exclusive lock is held
        self.owner_thread_id = None

    def acquire_shared(self):
        current_thread_id = threading.current_thread().ident
        if self.exclusive_lock or (self.shared_locks > 0 and self.owner_thread_id != current_thread_id):
            return False  # Another transaction holds an exclusive lock or there's an exclusive lock already
        else:
            self.shared_locks += 1
            self.owner_thread_id = current_thread_id
            return True

    def acquire_exclusive(self):
        current_thread_id = threading.current_thread().ident
        if self.shared_locks > 0 or (self.exclusive_lock and self.owner_thread_id != current_thread_id):
            return False  # Another transaction holds a shared lock or there's an exclusive lock already
        else:
            self.exclusive_lock = True
            self.owner_thread_id = current_thread_id
            return True

    def release_shared(self):
        current_thread_id = threading.current_thread().ident
        if self.shared_locks > 0 and self.owner_thread_id == current_thread_id:
            self.shared_locks -= 1
            if self.shared_locks == 0:
                self.owner_thread_id = None
            return True
        return False

    def release_exclusive(self):
        current_thread_id = threading.current_thread().ident
        if self.exclusive_lock and self.owner_thread_id == current_thread_id:
            self.exclusive_lock = False
            self.owner_thread_id = None
            return True
        return False






class Transaction:
    def __init__(self):
        self.queries = []
        self.locks = {}

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self):
        try:
            # Growing Phase: Acquire locks
            print("Acquiring locks...")
            for query, table, args in self.queries:
                if query.__name__ == "insert":  # For write operations
                    if table not in self.locks:
                        self.locks[table] = MyLock()
                    success = self.locks[table].acquire_exclusive()
                else:  # For read operations
                    if table not in self.locks:
                        self.locks[table] = MyLock()
                    success = self.locks[table].acquire_shared()

                if not success:
                    print("Failed to acquire lock for query:", query.__name__, "Aborting transaction.")
                    return self.abort()

            # Execute queries
            print("Executing queries...")
            for query, table, args in self.queries:
                result = query(*args)
                if not result:
                    print("Query", query.__name__, "failed. Aborting transaction.")
                    return self.abort()

            print("All queries executed successfully.")
            return self.commit()

        except Exception as e:
            print("An error occurred during transaction execution:", str(e))
            return self.abort()

    def abort(self):
        print("Transaction aborted. Releasing locks.")
        # Release all locks
        for table in self.locks:
            if table in self.locks:
                self.locks[table].release_exclusive()  # For write operations
                self.locks[table].release_shared()  # For read operations
        return False

    def commit(self):
        print("Committing transaction. Releasing locks.")
        # Release all locks
        for table in self.locks:
            if table in self.locks:
                self.locks[table].release_exclusive()  # For write operations
                self.locks[table].release_shared()  # For read operations
        print("Transaction committed successfully.")
        return True


