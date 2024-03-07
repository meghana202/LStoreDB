"""
A data structure holding indices for various columns of a table. Key column should be index by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        self.bufferpool = table.bufferpool

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if value in self.indices[column]: 
            return self.indices[column][value]
        else: 
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rids = []
        for value in range(begin, end + 1):
            if self.indices.locate(column, value) != None:
                for index in self.indices.locate(column, value):
                    rids.append(index)
        return rids

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number < len(self.indices):
            if not self.indices[column_number]:  # Check if index already exists
                self.indices[column_number] = {}
                # Get the total number of records
                num_records = len(self.table.page_directory)
                # Create index for existing data
                for rid, (pid, _, _) in self.table.page_directory.items():
                    pages = self.bufferpool.retrieve_page(pid)[0]
                    value = pages[4 + column_number].read(rid-1)  # Read the value from the specified column
                    #print(f"RID: {rid}, Value: {value}")  # Print RID and value for test case purposes

                    if value in self.indices[column_number]:
                        # Append the rid to the list of values for the value
                        self.indices[column_number][value].append(rid)
                    else:
                        # Create a new entry with the value as the key and a list containing the rids as the value
                        self.indices[column_number][value] = [rid]
            else:
                pass
        else:
            print("Column number out of range")

        pass


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number < len(self.indices): self.indices[column_number] = None
        pass
