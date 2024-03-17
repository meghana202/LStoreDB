from lstore.index import Index
from lstore.page_range import Page_Range
from time import time
from lstore.bufferpool import Bufferpool
import threading
from lstore.page import PageFactory
from lstore.wal import WriteAheadLogger

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, folder_path='./', pr=4):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = Bufferpool(500, num_columns, name, path=folder_path)
        self.index = Index(self)
        self.page_range = Page_Range(pr, self.bufferpool) #we try 4 for now
        self.index.indices[key] = {}
        self.page_directory = {}
        self.page_directory_locks = {}
        self.disk_page_mapping = {}
        self.log = WriteAheadLogger('log.txt')
        
    def get_page_directory_entry (self, rid):

        new_values = []
        if rid not in self.page_directory_locks:
            self.page_directory_locks[rid] = threading.Lock()

        with self.page_directory_locks[rid]:
            values = self.page_directory[rid]
            pages = self.bufferpool.retrieve_page(values[0])
            new_values.extend(values)
            new_values[0] = pages
        return new_values 

    def update_page_directory_entry (self, rid, entry):

        if rid not in self.page_directory_locks:
            self.page_directory_locks[rid] = threading.Lock()

        with self.page_directory_locks[rid]:
            pages = entry[0]
            #page_names = [page.get_name() for page in pages]
            entry[0] = pages
            self.page_directory[rid] = entry



 
