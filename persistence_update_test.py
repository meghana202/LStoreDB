from lstore.db import Database
from lstore.query import Query
import sys

# Test Select after Index Creation
db = Database()
db.open('./ECS165')
table = db.create_table('Grades', 5, 0)
query = Query(table)

# Does update work before persistence?
for i in range(10):
    query.insert(i, 2, 3, 4, 5)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, 2, 3, 4, 5]:
        print("Failed insert before persisting")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, 2, 3, 4, 5]")
        sys.exit(1)

for i in range(10):
    columns = [None, i, None, None, None]
    query.update(i, *columns)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, i, 3, 4, 5]:
        print("Failing update before persisting")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, {i}, 3, 4, 5]")
        sys.exit(1)

db.close()

# Update after 1st persistence
db.open('./ECS165')
table = db.get_table('Grades')
query = Query(table)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, i, 3, 4, 5]:
        print(i)
        print("Failing reading values updated in first persistance")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, {i}, 3, 4, 5]")
        sys.exit(1)

for i in range(10):
    columns = [None, None, i, None, None]
    query.update(i, *columns)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, i, i, 4, 5]:
        print("Failing reading values updated after loading 1st persistence")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, {i}, {i}, 4, 5]")
        sys.exit(1)

db.close()
# Update after 2nd persistence
db.open('./ECS165')
table = db.get_table('Grades')
query = Query(table)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, i, i, 4, 5]:
        print("Failing reading values updated in second persistance")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, {i}, {i}, 4, 5]")
        sys.exit(1)

for i in range(10):
    columns = [None, None, None, i, None]
    query.update(i, *columns)

for i in range(10):
    record = query.select(i, 0, [1,1,1,1,1])
    if record[0].columns != [i, i, i, i, 5]:
        print("Failing reading values updated after loading 2nd persistence")
        print(f"Your columns: {record[0].columns}\nCorrect: [{i}, {i}, {i}, {i}, 5]")
        sys.exit(1)
