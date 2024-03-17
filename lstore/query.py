from lstore.table import Table, Record
from lstore.index import Index
from time import time 
import copy
import threading
from lstore.page import PageFactory


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.page_range = table.page_range
        self.keys = {}
        self.bufferpool = table.bufferpool
        self.merge_lock = threading.Lock()
        pass
    def get_delete_lock_objs(self, primary_key):
        Empty = []
        if primary_key not in self.table.index.indices[self.table.key]:
            return Empty
        rid = self.table.index.locate(self.table.key, primary_key)[0]

        return [[f"RID_{rid}", "Exclusive", "Delete"]]

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):

        if primary_key not in self.table.index.indices[self.table.key]: 
            return False # Invalid Key 
        
        rid = self.table.index.locate(self.table.key, primary_key)[0]

        address = self.table.get_page_directory_entry(rid)
        if address[2] == True:
            indirection_page = address[0][0][0]
            location_on_page = address[1]
            indirection_page.rewrite(-1, location_on_page)
            self.bufferpool.return_to_pool(address[0][1], True)
            del self.table.index.indices[self.table.key][primary_key]
            #del self.keys[primary_key]
            return True
        else: 
            print("Not a base page")
            return False
    
    def get_insert_lock_objs(self, *columns):
        Empty = []
        if len(columns) != self.table.num_columns:
            return Empty

        if columns[self.table.key] in self.table.index.indices[self.table.key]:
            return Empty

        return [[f"INDEX_{columns[self.table.key]}", "Exclusive", "Insert"]]
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):

        if len(columns) != self.table.num_columns:
            #print(f"insert: FAIL num_columns don't match")
            return False

        if columns[self.table.key] in self.table.index.indices[self.table.key]: 
            #print(f"insert: KEY already exists")
            return False

        schema_encoding = int('0' * self.table.num_columns, 2)
        insert_loc, pid = self.page_range.get_insert_loc(self.table.num_columns)
        pages, num_records = insert_loc[0], insert_loc[1]

        # Write metadata
        pages[0].write(0) # Inital indirection is 0 to indicate no updates
        pages[1].write(self.page_range.rid) # RID is in table 
        pages[2].write(int(time()))
        pages[3].write(schema_encoding)

        # Write data
        for i in range(4, self.table.num_columns+4):
            pages[i].write(columns[i-4])
            
        # if primary key is in indices[0], append rid to mapping at index
        for i in range(len(columns)):
            self.table.index.update_index(i, columns[i], self.page_range.rid)

        self.table.update_page_directory_entry(self.page_range.rid, [pid, num_records, True])
        self.bufferpool.return_to_pool(pid, True) 
        return True

    def get_select_lock_objs(self, search_key, search_key_index, projected_columns_index):
        Empty = []
        rid_list = []
        rids = self.table.index.locate(search_key_index, search_key)
        if len(rids) > 0:
            for rid in rids:
                rid_list.append([f"RID_{rid}", "Shared", "Select"])
            return rid_list
        return Empty
      
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        records, _ = self.select2(search_key, search_key_index, projected_columns_index)
        return records


    def select2(self, search_key, search_key_index, projected_columns_index):
        if self.table.index.indices[search_key_index] == None:
            self.table.index.create_index(search_key_index)

        rids = self.table.index.locate(search_key_index, search_key)

        records = []
        level = 0
        original_tail = -1

        for rid in rids:
            record_vals = [None]*self.table.num_columns
            address = self.table.get_page_directory_entry(rid)
            pages, record_num = address[0][0], address[1]

            indirection = pages[0].read(record_num)
            if indirection == -1: return False
            if original_tail == -1:
                original_tail = indirection

            if indirection == 0 or (len(address[0]) == 4 and address[0][3] == indirection): # No updates made

                for i, val in enumerate(projected_columns_index):
                    if val == 1:
                        record_vals[i] = pages[i+4].read(record_num)

                record_vals = [val for val in record_vals if val is not None]
                record_to_append= Record(rid, search_key, record_vals)
                self.bufferpool.return_to_pool(address[0][1], False)
                records.append(record_to_append)
            else: 

                level += 1
                
                MRTP = pages[0].read(record_num) #Most Recent Tail Page
                self.bufferpool.return_to_pool(address[0][1], False)
                while True:

                    address_tail = self.table.get_page_directory_entry(MRTP)
                    pages_tail, record_num_tail = address_tail[0][0], address_tail[1]
                    indirection = pages_tail[0].read(record_num_tail)

                    for i in range(len(pages_tail)-4):
                        if projected_columns_index[i] == 1 and record_vals[i] == None:
                            if pages_tail[i+4].read(record_num_tail) != -1:
                                record_vals[i] = pages_tail[i+4].read(record_num_tail)
                    MRTP = indirection
                    if MRTP == 0 or (len(address[0]) == 4 and MRTP == address[0][3]):
                        for i, val in enumerate(projected_columns_index):
                            if projected_columns_index[i] == 1:
                                if record_vals[i] == None:
                                    record_vals[i] = pages[i+4].read(record_num)
                        record_vals = [val for val in record_vals if val is not None]
                        record_to_append = Record(rid, search_key, record_vals)
                        records.append(record_to_append)
                        #print("returned")
                        self.bufferpool.return_to_pool(address_tail[0][1], False)
                        break
        
        return records, original_tail 
        pass
    
    def get_update_lock_objs(self, primary_key, *columns):
        Empty = []
        if primary_key not in self.table.index.indices[self.table.key]:
            return Empty
        column = self.table.key
        base_rid = self.table.index.locate(column, primary_key)[0]#self.keys[primary_key] # Get RID
        return [[f"RID_{base_rid}", "Exclusive", "Update"]]
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):

        if primary_key not in self.table.index.indices[self.table.key]:
            return False # Invalid Key 
        """ 
        if columns[self.table.key] != primary_key and columns[self.table.key] != None: 
            self.delete(primary_key)
            self.insert(*columns)
            return True
        """
    
        column = self.table.key
        base_rid = self.table.index.locate(column, primary_key)[0]#self.keys[primary_key] # Get RID
        #address = self.table.page_directory[base_rid]
        address = self.table.get_page_directory_entry(base_rid)
        base_page, record_num_base = address[0][0], address[1]
        base_indirection = base_page[0]
        base_indirection_val = base_indirection.read(record_num_base)
        base_schema_encoding = base_page[3]
        base_schema_encoding_val = base_schema_encoding.read(record_num_base)
        
        # If First, we copy base record to first tail record
        if base_indirection_val == 0:
            first_update_loc = self.page_range.get_update_loc(base_rid, self.table.num_columns) 
            if len(first_update_loc[0]) > 2:
                if first_update_loc[0][2] == True:
                    page_range = first_update_loc[0][3]
                    #merge = threading.Thread(target=self.__merge, args=(page_range,))
                    #merge.start()
                    #self.__merge(page_range)

            page1, record_num1 = first_update_loc[0][0], first_update_loc[0][1]
            pid = first_update_loc[1]
            # Write Metadata
            page1[0].write(0) # Initial Indirection = 0 
            page1[1].write(self.page_range.rid)
            page1[2].write(int(time()))
            page1[3].write(int('0'*self.table.num_columns))

            previous_value = [None] * self.table.num_columns
            for i in range(4, self.table.num_columns + 4):
                value = base_page[i].read(record_num_base)
                if columns[i-4] != None:
                    previous_value[i-4] = value
                page1[i].write(value)
            
            # We must update the base page indirection column
            base_indirection.rewrite(self.page_range.rid, record_num_base)
            #self.table.page_directory[self.page_range.rid] = [page1, record_num1, False, base_rid]
            self.table.update_page_directory_entry(self.page_range.rid, [pid, record_num1, False, base_rid])
            self.bufferpool.return_to_pool(pid, True)

        # Not first update
        # Update BaseRID AND Schema Encoding
        #base_indir = self.table.page_directory[base_rid][0][0]
        base_indir = self.table.get_page_directory_entry(base_rid)[0][0]
        update_loc = self.page_range.get_update_loc(base_rid, self.table.num_columns)

        if len(update_loc[0]) > 2:
            if update_loc[0][2] == True:
                page_range = update_loc[0][3]
                #merge = threading.Thread(target=self.__merge, args=(page_range,))
                #merge.start()
        
        page, record_num = update_loc[0][0], update_loc[0][1]
        pid1 = update_loc[1]
        # Write Metadata

        schema_encoding = int(''.join(['0' if x is None else '1' for x in columns]), 2)
        page[0].write(base_indirection.read(record_num_base)) # Tail record points where Base points
        page[1].write(self.page_range.rid)
        page[2].write(int(time()))
        page[3].write(schema_encoding)

        previous_value = [None] * self.table.num_columns
        for i in range(4, len(columns)+4):
            value = -1 if columns[i-4] is None else columns[i-4]
            previous_value[i-4] = value
            page[i].write(value)
        
        base_indirection.rewrite(self.page_range.rid, record_num_base) # Base points to new tail 

        new_schema_encoding = list(format(base_schema_encoding_val, '0{}b'.format(self.table.num_columns)))
        for i, val in enumerate(columns):
            if val is not None:
                new_schema_encoding[i] = '1'
        new_schema_encoding = int(''.join(new_schema_encoding), 2)

        """
        for i in range(len(columns)):
            if columns[i] != None:
                self.table.index.remove_index(i, previous_value[i], base_rid)
                self.table.index.update_index(i, columns[i], base_rid)
        """

        # Update base schema encoding
        base_schema_encoding.rewrite(new_schema_encoding, record_num_base)
        #self.table.page_directory[self.page_range.rid] = [page, record_num, False, base_rid]
        self.table.update_page_directory_entry(self.page_range.rid, [pid1, record_num, False, base_rid])
        self.bufferpool.return_to_pool(pid1, True)

         
        return True
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired co lumn to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        records_found = False
        # Iterate through the range of primary keys
        for primary_key in range(start_range, end_range + 1):
            # Check if the primary key exists in the page directory
            if primary_key in self.table.index.indices[self.table.key]:
                records_found = True
                # Access the value in the specified column index

                record = self.select(primary_key, self.table.key, [1]*self.table.num_columns)[0]
                value = record.columns[aggregate_column_index]
                
                # If the value exists (not None), add it to the total sum
                if value is not None:
                    total_sum += value
        
        # If total_sum is 0, no records were found in the given range
        if records_found == True: return total_sum
        else: return False
    
    def __merge(self, page_range):
        #try:

        with self.merge_lock:
            self.my_merge(page_range)
    

    def my_merge(self, page_range):
        print("merge starting")
        deallocate_queue = []
        current_thread = threading.current_thread()

        # Get the thread's identifier
        thread_id = current_thread.ident

        last_tail_pid = self.page_range.tail_pages[page_range][-2][1]
        last_tail_page = self.bufferpool.retrieve_page(last_tail_pid)[0]
        if True and last_tail_page[0].has_capacity() == False:
            tail_page = last_tail_page
            index = 4*page_range
            relevant_pages = []

            merged_rids = []
            for i in reversed(range(0, 512)):
                relevant_pages = {}
                rid = tail_page[1].read(i)
                rid_data = self.table.get_page_directory_entry(rid)
                brid = rid_data[3]
                base_rid_data = self.table.get_page_directory_entry(brid)
                pid = self.table.page_directory[brid][0]
                base_pages = base_rid_data[0][0]
                if base_pages[0].has_capacity():
                    continue
                
                if pid not in relevant_pages.keys():
                    relevant_pages[pid] = copy.deepcopy(base_pages)
                 
                if brid not in merged_rids:
                    merged_rids.append(brid)
                else:
                    continue
            
            for brid in merged_rids:
                pid, offset = self.table.page_directory[brid][0], self.table.get_page_directory_entry(brid)[1]

                # get location on deepcopied relevant base pages
                if pid not in relevant_pages.keys(): 
                    continue # update to a not fully commited BP record
                relevant_page = relevant_pages[pid]
                keyval = relevant_page[4+self.table.key].read(offset)
                columns = [1]*self.table.num_columns
                new_record_data, tps = self.merge_select(brid, relevant_page, offset,  tail_page)
                new_record = new_record_data[0]
                if relevant_page[self.table.key +4].read(offset) == keyval and relevant_page[0].read(offset) != -1:
                    for j in range(len(columns)):
                        relevant_page[j+4].rewrite(new_record[j], offset)
                        PageFactory().set_page(relevant_page[j+4].get_name(), relevant_page[j+4])
                else:
                    #print (f"Not match reading value:{page[self.table.key +4].read(offset)}, keyval:{keyval}, page[0].read(offset):{page[0].read(offset)}")
                    continue

                page = self.table.get_page_directory_entry(brid)[0][0]
                pid = self.table.get_page_directory_entry(brid)[0][1]
                new_pages = page[:4] + relevant_page[4:]

                self.table.update_page_directory_entry(brid, [pid, offset, True, tps])
                self.bufferpool.return_to_pool(pid, True)
                deallocate_queue.append(page)
    
    def merge_select(self, rid,  relevant_base_page, record_num, tail_page):
        records = []
        level = 0
        original_tail = -1
        projected_columns_index = [1]*self.table.num_columns

        record_vals = [None]*self.table.num_columns
        address = self.table.get_page_directory_entry(rid)
        pages = relevant_base_page

        indirection = pages[0].read(record_num)
        if indirection == -1: return False
        if original_tail == -1:
            original_tail = indirection

        if indirection == 0 or (len(address[0]) == 4 and address[0][3] == indirection): # No updates made

            for i, val in enumerate(projected_columns_index):
                if val == 1:
                    record_vals[i] = pages[i+4].read(record_num)

            record_vals = [val for val in record_vals if val is not None]
            record_to_append= record_vals
            records.append(record_to_append)
        else: 

            level += 1
                
            MRTP = pages[0].read(record_num) #Most Recent Tail Page
            while True:

                address_tail = self.table.get_page_directory_entry(MRTP)
                pages_tail, record_num_tail = address_tail[0][0], address_tail[1]
                indirection = pages_tail[0].read(record_num_tail)

                for i in range(len(pages_tail)-4):
                    if projected_columns_index[i] == 1 and record_vals[i] == None:
                         if pages_tail[i+4].read(record_num_tail) != -1:
                            record_vals[i] = pages_tail[i+4].read(record_num_tail)
                MRTP = indirection
                if MRTP == 0 or (len(address[0]) == 4 and MRTP == address[0][3]):
                    for i, val in enumerate(projected_columns_index):
                        if projected_columns_index[i] == 1:
                            if record_vals[i] == None:
                                record_vals[i] = pages[i+4].read(record_num)
                    record_vals = [val for val in record_vals if val is not None]
                    record_to_append = record_vals
                    records.append(record_to_append)
                    break
         
        return records, original_tail 
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

