from lstore.db import Database
from lstore.query import Query

# Create and write to table
db = Database()
db.open('./ECS165A')

table2 = db.create_table('Random', 5, 0)
query2 = Query(table2)

for i in range(10000):
    query2.insert(i, i+1, i+2, i+3, i+4)
print("Insert Completed")

for i in range(10000):
    columns  = [None, None, None, None, 1]
    query2.update(i, *columns)
print("Update Completed")

db.close()

# Load from persistance and update
db.open('./ECS165A')

table3 = db.get_table('Random')
print(table3.page_range.base_pages)
query3 = Query(table3)
columns = [None, 100000, None, None, None]
query3.update(1, *columns)
record = query3.select(1, 0, [1,1,1,1,1])
print(record[0].columns)

