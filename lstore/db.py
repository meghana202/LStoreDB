from lstore.table import Table
from lstore.page import Page
import os
import csv
import ast  

class Database():

    def __init__(self):
        self.tables = {}
        self.path = "./"
        pass

    def get_directories(self, path):
        try:
            # Path to the directory you want to enumerate
            folder_path = path
    
            # List all entries in the directory
            entries = os.listdir(folder_path)
    
            # Filter out files, keep only directories
            directories = [entry for entry in entries if os.path.isdir(os.path.join(folder_path, entry))]
    
            return directories
        except:
            return []

    def get_files(self, path):
        try:
            # Path to the directory you want to enumerate
            folder_path = path

            # List all entries in the directory
            entries = os.listdir(folder_path)
    
            # Filter out files, keep only directories
            files = [entry for entry in entries if os.path.isfile(os.path.join(folder_path, entry))]
    
            return files
        except:
            return []
    
    def load_pr(self, path):
        table_names = self.get_directories(self.path)
        for table_name in table_names:
            path += f"/{table_name}"
            path += "/page_range.csv"
            table = self.get_table(table_name)
            base_pages = []
            tail_pages = []
            rid = 0

            with open(path, mode='r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if row[0] == '':
                        # Assuming the line starts with ',' followed by integers
                        base_page = [[None, int(entry)] for entry in row if entry.isdigit()]
                        base_pages.extend(base_page)
                    elif row[0][0] == '[':
                        tail_page_range = [[None, int(entry)] for entry in row if entry.isdigit()]
                        tail_pages.append(tail_page_range)
                    else: 
                        rid = int(row[0])
                
            #print(base_pages)
            table.page_range.rid = rid
            table.page_range.base_pages = base_pages
            table.page_range.tail_pages = tail_pages

    def open(self, path):
        
        self.path = path
        if self.get_directories(self.path) == []:
            if not os.path.exists(path):
                os.makedirs(path)
            return

        table_names = self.get_directories(self.path)
        for table_name in table_names:
            table_metadata_file_name = f"{self.path}/{table_name}/metadata.csv"
            with open(table_metadata_file_name, 'r', newline='',) as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                  self.create_table(table_name, int(row[0]),int(row[1]))
                  break
        self.load_pr(path)
        
        # read page directory
        for table_name in table_names:
            my_table = self.get_table(table_name)
            page_dir_file_name = f"{self.path}/{table_name}/directory/page_directory.csv"
            with open(page_dir_file_name, newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                data_dict = {}
                for row in csvreader:
                    # The key is the first element
                    key = int(row[0])
                    pid = int(row[1])
                    offset = int(row[2])
                    bp = bool(row[3])
                    last = int(row[4])

                    data = [pid, offset, bp, last]
                    data.extend(data)

                    data_dict[key] = data

                    
                my_table.page_directory = data_dict

        # read indices 
        for table_name in table_names:

            my_table = self.get_table(table_name)
            dirname = f"{self.path}/{table_name}/indices"
            indices = self.get_files(dirname)
            for index in indices:
                column_index = int(index.split("_")[0])
                index_file_path = f"{dirname}/{index}"
                with open(index_file_path, newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    data_dict = {}
                    for row in csvreader:
                        # The key is the first element
                        key = int(row[0])
                        # The second element is a string representation of a list, safely evaluate it
                        value_list = ast.literal_eval(row[1])
                        # Store the key and value in the dictionary
                        data_dict[key] = value_list
                    my_table.index.indices[column_index] = data_dict
            

    def persist_page_directory(self,path, data_rows):

        # Specify the CSV file path
        csv_file_path = path + "/page_directory.csv"

        # Extract the directory path
        directory = os.path.dirname(csv_file_path)

        # Check if the directory exists, and if not, create it (including any intermediate directories)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Write the data to a CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data_rows)

    def persist_indices(self,path, column, data_rows):

        # Specify the CSV file path
        csv_file_path = path + f"/{column}_indices.csv"

        # Extract the directory path
        directory = os.path.dirname(csv_file_path)

        # Check if the directory exists, and if not, create it (including any intermediate directories)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Write the data to a CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data_rows)
    
    def persist_table_metadata(self,path, data_rows):

        # Specify the CSV file path
        csv_file_path = path + f"/metadata.csv"

        # Extract the directory path
        directory = os.path.dirname(csv_file_path)

        # Check if the directory exists, and if not, create it (including any intermediate directories)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Write the data to a CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data_rows)

    def persist_pr(self, path):
        for table_string in self.tables:
            path += f"/{table_string}"
            # Base Pages 
            table = self.get_table(table_string)
            base_pages = table.page_range.base_pages 
            bp_to_write = []
            for i in range(len(base_pages)):
                obj = base_pages[i]
                bp_to_write.append([None, obj[1]])

            # Tail Pages 
            tail_pages = table.page_range.tail_pages
            tp_to_write = []
            for j in range(len(tail_pages)):
                new_pr = []
                for i in range(len(tail_pages[j])):
                    obj = tail_pages[j][i]
                    new_pr.append([None, obj[1]])
                tp_to_write.append(new_pr) 

            csv_file_path = path + f"/page_range.csv"
            directory = os.path.dirname(csv_file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(csv_file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(bp_to_write)
                #print(tp_to_write)
                writer.writerow(tp_to_write)
                writer.writerow([table.page_range.rid])
        
    def close(self):
        page_table_entries = []
        self.persist_pr(self.path)
        for table_name in self.tables.keys():
            #print(f"closing {table_name}")
            table = self.tables[table_name]
            
            for rid in table.page_directory.keys():
                rid_data = table.page_directory[rid]

                page_table_entry = [rid]
                if len(rid_data) == 4:
                    page_table_entry.extend([rid_data[0],rid_data[1], rid_data[2], rid_data[3]])
                else:
                    page_table_entry.extend([rid_data[0],rid_data[1], rid_data[2], -1])
                page_table_entries.append(page_table_entry)
            
            folder_path = f"{self.path}/{table_name}"
            fname = f"{folder_path}/directory"
            self.persist_page_directory(fname, page_table_entries)                 
            index = table.index

            for i,column in enumerate(index.indices):
                if column:
                    column_indexes = []
                    for key in column:
                        column_indexes.append([key, column[key]])
                    fname = f"{folder_path}/indices"
                    self.persist_indices (fname,  i, column_indexes)
            table.bufferpool.flush_all()
        

    def create_file_with_directories(self, path, data):
        # Extract the directory path
        directory = os.path.dirname(path)
        
        # Check if the directory exists, and if not, create it (including any intermediate directories)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Now that the directory exists, create the file
        with open(path, 'wb') as file:
            file.write(data)  
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index, folder_path="./",pr=4):
        table = Table(name, num_columns, key_index, folder_path=self.path, pr=pr)
        self.tables[name] = table

        folder_path = f"{self.path}/{name}"
        data_rows = []
        data_rows.append([table.num_columns, table.key])
        self.persist_table_metadata(folder_path, data_rows)
        

        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] 

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
