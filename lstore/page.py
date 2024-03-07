class PageFactory:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PageFactory, cls).__new__(cls)
            cls._instance.page_id_page_map = {}  # Initialize it on the instance
        return cls._instance

    def get_page(self, page_name):
        # Access the instance attribute with self
        return self.page_id_page_map.get(page_name, None)  # Added .get for safer access

    def set_page(self, page_name, page):
        # Modify the dictionary on this instance
        self.page_id_page_map[page_name] = page

class Page:

    def __init__(self, column, page_id):
        self.num_records = 0
        self.data = bytearray(4096)
        self.column = column
        self.page_id = page_id
        PageFactory().set_page(self.get_name(), self)

    def get_name(self):
        page_name = f"{self.column}_{self.page_id}"
        return page_name

    def has_capacity(self):
        max_records = len(self.data) / 8
        return self.num_records < max_records  

    def read(self, record_num):
        return int.from_bytes(self.data[(record_num)*8: (record_num+1)*8], byteorder='big', signed=True)

    def write(self, value):
        if self.has_capacity():
    
            offset = self.num_records * 8

            # Convert the value to bytes and write it to the data bytearray
            self.data[offset:offset + 8] = value.to_bytes(8, byteorder='big', signed=True)

            self.num_records += 1

        else:
            raise Exception("Page is full")
    
    def rewrite(self, value, location): 
        # write some checks to make sure page is not read only
        offset = (location)*8
        self.data[offset:offset + 8] = value.to_bytes(8, byteorder='big', signed=True)
      
