from lstore.page import Page
import threading
import copy
from  lstore.durable_page_id import DurablePageIDGenerator
from lstore.bufferpool import Bufferpool

class Page_Range():

    def __init__(self, page_range, bufferpool):
        self.base_pages = [] #[bp, pid]
        self.tail_pages = [] #[[range1, range2]], range = [[tp1, pid], tp2, pid]]
        self.page_range = page_range 
        self.bufferpool = bufferpool
        self.rid = 0

    # Insert is requested, return back base page to write to and num_records
    def get_insert_loc(self, num_cols):
        
        # First Insert, no pages have been allocated
        # Initialize Page Range

        if self.base_pages == [] or (self.bufferpool.retrieve_page(self.base_pages[-1][1])[0][0].has_capacity() == False):

            bp, page_id = self.bufferpool.request_new_page()
            self.base_pages.append([bp, page_id])
            if len(self.base_pages) % self.page_range == 1:
                new_tail_range = []
                self.tail_pages.append(new_tail_range)

            self.rid += 1
            return [bp, 0], page_id
        
        else: 
            self.rid += 1
            last_base_page_pid = self.base_pages[-1][1]
            last_base_page = self.bufferpool.retrieve_page(last_base_page_pid)[0]
            return [last_base_page, last_base_page[0].num_records], last_base_page_pid
    
    def get_update_loc(self, rid, num_cols):

        # RID parameter is Base RID being updated
        page_range = rid//(self.page_range*512)
        merge = False
        # tail pages = [[[tp1, tp2]], [pagerange0]]

        if self.tail_pages[page_range] == [] or (self.bufferpool.retrieve_page(self.tail_pages[page_range][-1][1])[0][0].has_capacity() == False):
            # if the previous tail page is full (not empty page range set), initiate merge
            if self.tail_pages[page_range] != []:
                merge = True

            tp, pid = self.bufferpool.request_new_page()
            if self.tail_pages == []:
                self.tail_pages.append([[tp, pid]])
            else: self.tail_pages[page_range].append([tp, pid])
            self.rid += 1
            return [tp, 0, merge, page_range], pid
        else: 
            self.rid += 1
            last_tail_page_pid = self.tail_pages[page_range][-1][1]
            last_tail_page = self.bufferpool.retrieve_page(last_tail_page_pid)[0]
            return [last_tail_page, last_tail_page[0].num_records], last_tail_page_pid
        



    
    

