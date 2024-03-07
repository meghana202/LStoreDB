from lstore.page import Page
from lstore.durable_page_id import DurablePageIDGenerator
import os

'''
Once transaction has ended Query can modify queue and buffpool directly 
check if bufferpool [2] (transaction count) will be 0 and add to queue or decrement
'''

class Bufferpool():

    def __init__(self, size, columns, table_name, path):
        self.bufferpool = {}
        self.queue = []
        self.size = size
        self.num_columns = columns
        self.empty_pages = []
        self.table_name = table_name
        self.path = path

        for i in range(size):
            pages = []
            page_id = DurablePageIDGenerator().get_new_id()
            for j in range(self.num_columns + 4):
                column = j
                page = Page(column, page_id)
                pages.append(page)
            self.empty_pages.append(page_id)
            self.bufferpool[page_id] = [pages, 0, 0, True] # Base Page/Tail Page, Dirty Bit, Transaction Count

    '''
    Checks Bufferpool for Page, else ejects and allocates space
    '''
    def request_new_page(self):

        # If empty page in pool
        if len(self.empty_pages) > 0:
            page = self.bufferpool[self.empty_pages[0]][0]
            page_id = self.empty_pages.pop(0)
            return page, page_id
        
        # No empty pages in pool
        else:
            self.eject()
            new_page = []
            page_id = DurablePageIDGenerator().get_new_id()
            for i in range(self.num_columns + 4):
                column = i
                page = Page(column, page_id)
                new_page.append(page)
            self.bufferpool[page_id] = [new_page, 0, 0]
        return new_page, page_id
    
    '''
    Makes necessary updates to bufferpool and queue after transaction
    '''
    def return_to_pool(self, page_id, change):
        
        self.bufferpool[page_id][2] -= 1 # Reduce transaction count

        if change == True:
            self.bufferpool[page_id][1] = 1 # If changes have occurred, set dirty bit to 1
        if page_id not in self.queue: self.queue.append(page_id)
        else: 
            self.queue.remove(page_id)
            self.queue.append(page_id)
        
    '''
    Takes a page_id and will load page, or page is already in bufferpool and returns page (array of byte arrays)
    and page_id
    '''
    def retrieve_page(self, page_id):
        
        # Page already in bufferpool
        if page_id in self.bufferpool:
            self.bufferpool[page_id][2] += 1
            return self.bufferpool[page_id][0], page_id
    
        # If no space, eject
        if len(self.queue) >= self.size:
            page_id_to_replace = self.eject()
        
        pages = self._load_page(page_id)
        self.bufferpool[page_id] = [pages, 0, 1, False]
        return pages, page_id
    
    def _load_page(self, page_id):

        pages_dir_name = f"{self.path}/{self.table_name}/pages"
        page_path = []
        for i in range(self.num_columns + 4):
            file_name = f"{i}_{page_id}.dat"
            page_path.append(file_name)

        pages = []
        for page_name in page_path:
            page_name_prefix = page_name.split(".")[0]
            tokens = page_name_prefix.split("_")
            column, page_id = tokens[0], tokens[1]
            p = Page (column, page_id)
            with open(pages_dir_name + "/" + page_name, 'rb') as file:
                # Read the first 4096 bytes
                tmp_bytes = file.read(4096)
                p.data[:len(tmp_bytes)] = tmp_bytes
            pages.append(p)
        return pages

    '''
    Flushes if necessary
    Returns the page_id of ejected page
    '''
    def eject(self):
        
        page_id_to_eject = self.queue[0]
        # Page is dirty and must be flushed
        if self.bufferpool[page_id_to_eject][1] == 1 or self.bufferpool[page_id_to_eject][1] == 0:
            pages_dir_name = f"{self.path}/{self.table_name}/pages"
            in_memory_pages = self.bufferpool[page_id_to_eject][0]
            for i in range(self.num_columns + 4):
                data = in_memory_pages[i].data
                file_name = f"{i}_{page_id_to_eject}.dat"
                isExist = os.path.exists(pages_dir_name)
                if not isExist:
                    # Create a new directory because it does not exist
                    os.makedirs(pages_dir_name)
                with open(f"{pages_dir_name}/{i}_{page_id_to_eject}.dat", 'wb') as file:
                    file.write(data)
        # Page is clean 
        else:
            pass
        del self.bufferpool[page_id_to_eject]
        return self.queue.pop(0)

    def flush_all(self):
        
        while len(self.queue) > 0:
            self.eject()
        
        return True


            
