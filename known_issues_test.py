from lstore.db import Database
from lstore.query import Query

# Test Select after Index Creation
db = Database()
db.open('./ECS165A')

table = db.create_table('Grades', 5, 0)

query = Query(table)

for i in range(10):
    query.insert(i, i, 3, 4, 5)

record = query.select(0, 1, [1,1,1,1,1])

if (len(record) == 1):
    if record[0].columns == [0, 0, 3, 4, 5]:
        print("Pass initial query test")

query.insert(10, 0, 3, 4, 5)
record2 = query.select(0, 1, [1,1,1,1,1])
if (len(record2) == 2):
    if record2[1].columns == [10, 0, 3, 4, 5]:
        print("Pass index test")

if record2[1].columns != [10, 0, 3, 4, 5]:
    print(f"Your columns: {record2[1].columns}\n Correct Columns: [10, 0, 3, 4, 5]")

columns = [None, 2, None, None, None]
query.update(10, *columns)

# Check that select on 0 returns 1 record 
record2 = query.select(0, 1, [1,1,1,1,1])
if (len(record2) == 1):
    if record2[0].columns == [0, 0, 3, 4, 5]:
        print("Pass update test 1")

# Check that select on 2 returns 2 records
record2 = query.select(2, 1, [1,1,1,1,1])
if (len(record2) == 2):
    if record2[1].columns == [10, 2, 3, 4, 5]:
        print("Pass update test 2")

# Test Bufferpool Pin Counts

table2 = db.create_table('Random', 5, 0)
query2 = Query(table2)

for i in range(100):
    query2.insert(i, i+1, i+2, i+3, i+4)
print("Insert Completed")

for i in range(100):
    query2.select(i, 0, [1,1,1,1,1])
print("Select Completed")

records = query2.select(0, 0, [1,1,1,1,1])
print(len(records))

db.close()

