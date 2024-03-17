from lstore.table import Table, Record
from lstore.index import Index
from threading import Lock
from lstore.query import Query
import threading
import sys
from readerwriterlock import rwlock
import traceback

# Global dictionary to store locks
global_locks = {}
# Lock to synchronize access to the global_locks dictionary
global_locks_lock = threading.Lock()

def get_or_create_lock(key):
    with global_locks_lock:  # Synchronize access to the dictionary
        if key not in global_locks:
            global_locks[key] = rwlock.RWLockFair()
        return global_locks[key]


def filter_unique_exclusive_over_shared(data):
    """
    Filter and sort a list of lists to ensure unique IDs, preferring "Exclusive" over "Shared",
    with the result sorted by the ID.
    
    :param data: List of lists, each inner list containing an ID and a string ("Exclusive" or "Shared")
    :return: Sorted list of filtered entries based on the criteria
    """
    result_dict = {}
    for item in data:
        #print (item)
        id_, access_type,q_type = item
        if id_ not in result_dict:
            # Add the ID to the dictionary if it's not already present
            result_dict[id_] = access_type
        elif result_dict[id_] == "Shared" and access_type == "Exclusive":
            # Update the dictionary if the current access type is "Exclusive" and the existing one is "Shared"
            result_dict[id_] = access_type
    
    # Convert the dictionary back to a list of lists and sort by the ID
    result_list = sorted([[id_, access_type] for id_, access_type in result_dict.items()], key=lambda x: x[0])
    result_list.append(["TABLE", "Exclusive"])
    return result_list

# Example usage
#data = [["ID3", "Shared"], ["ID1", "Shared"], ["ID2", "Exclusive"], ["ID2", "Shared"], ["ID1", "Exclusive"]]
#filtered_and_sorted_data = filter_unique_exclusive_over_shared(data)
#print(filtered_and_sorted_data)



def get_locks(object_list):
    lock_list = []
    for object in object_list:
        lock = get_or_create_lock(object[0])
        lock_list.append([lock, object[0], object[1]])
    return lock_list    



class Transaction:
    def __init__(self):
        self.queries = []
        self.locks = {}

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def get_sorted_objs(self, all_locks):
       sorted_locks_list = sorted(all_locks, key=lambda x: x[0])
       return sorted_locks_list

    def run(self):
        #try:
        my_thread_id = threading.current_thread().ident
        #print(f"{my_thread_id}: LIFE_CYCLE++++++++++++++++++++++")
        while True:
            # Growing Phase: Acquire locks
            #print(f"{my_thread_id}: Acquiring locks...")
            all_locks = []
            #print (f"NUMBER of queries:{len(self.queries)}")
            for query, table, args in self.queries:
                my_query_obj =  Query(table)
                if query.__name__ == "insert":
                    lock_objs = my_query_obj.get_insert_lock_objs(*args)
                    #print(f"INSERT lock objs:{lock_objs}")
                    all_locks.extend(lock_objs)
                elif query.__name__ == "update":
                    lock_objs = my_query_obj.get_update_lock_objs(*args)
                    #print(f"UPDATE lock objs:{lock_objs}")
                    all_locks.extend(lock_objs)
                elif query.__name__ == "delete":
                    lock_objs = my_query_obj.get_delete_lock_objs(*args)
                    #print(f"DELETE lock objs:{lock_objs}")
                    all_locks.extend(lock_objs)
                elif query.__name__ == "select":
                    lock_objs = my_query_obj.get_select_lock_objs(*args)
                    #print(f"SELECT lock objs:{lock_objs}")
                    all_locks.extend(lock_objs)
                else:
                    print (f"WHAT IS THIS QUERY??{query.__name__}")

            #for lock in all_locks:
                #print (f"len:{len(all_locks)},lock:{lock}")

            #print (f"Unsorted:{all_locks}")
            all_locks_sorted = filter_unique_exclusive_over_shared(all_locks)
            #print (f"Sorted:{all_locks_sorted}")

            # Acquire locks in the predefined global order
            to_be_acquired_locks = get_locks(all_locks_sorted)
            acquired_locks = []
            tables = []
            try:
                for lock_data in to_be_acquired_locks:
                    lock = lock_data[0]
                    mode = lock_data[2]
                    #print(f"{my_thread_id}:Acquiring lock:{lock_data[1]}, {lock_data[2]}")
                    if mode == "Exclusive":
                        wlock = lock.gen_wlock()
                        wlock.acquire()
                        acquired_locks.append([wlock, lock_data[1], lock_data[2]])
                    else:
                        wlock = lock.gen_rlock()
                        wlock.acquire()
                        acquired_locks.append([wlock, lock_data[1], lock_data[2]])



                # Perform your operation that requires all locks here
                #print("Performing an operation protected by multiple locks.")
                # Execute queries
                #print(f"{my_thread_id}:Executing queries...")
                for query, table, args in self.queries:
                    #print (f"{my_thread_id}: {args}")
                    if table not in tables: tables.append(table)
                    table.log.write(query.__name__, args)
                    result = query(*args)
                    ## Handle errors... will require UNDO for a True failure
                    ## However, valid failures shouldn't cause a transaction to fail
                    if False and not result:
                        print(f"{my_thread_id}:EXECUTING Query", query.__name__, "failed. Aborting transaction.")
                        #return self.abort()
                        pass
    
                #print(f"{my_thread_id}:LIFE_CYCLE All queries executed successfully.")
                sys.stdout.flush()

            except Exception as e:
                print(f"{my_thread_id}:An error occurred during transaction execution:", str(e))
                print(f"{my_thread_id}:An error occurred during transaction execution:", repr(e))
                print(f"{my_thread_id}:An error occurred during transaction execution:", e.args)

                traceback.print_exception(type(e), e, e.__traceback__)

       
            finally:
                # Release all acquired locks in reverse order
                for table in tables: table.log.flush()
                for lock_data in reversed(acquired_locks):
                    lock = lock_data[0]
                    #print (f"{my_thread_id}:Releasing lock:{lock_data[1]},{lock_data[2]}")
                    lock.release()
    
            return True
        """
        #except Exception as e:
        else:
            print(f"{my_thread_id}:An error occurred during transaction execution:", str(e))
            return self.abort()
        """

    """
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
    """