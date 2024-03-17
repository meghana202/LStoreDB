from lstore.db import Database
from lstore.query import Query

db = Database()
db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)



query.insert(1,2,3,4,5)
result = query.select(1,0,[1,1,1,1,1])
for record in result:
    print("Column Values:", record.columns)

columns=[None,None,None,None, 200 ]
query.update(1,*columns)

columns=[None,None,None,None, 400 ]
query.update(1,*columns)

columns=[None,None,None,None, 600 ]
query.update(1,*columns)

record = query.select(1,0, [1,1,1,1,1])
print ("Open",record[0].columns)
db.close()


def read_after_persist(loops):
     print ("")
     db = Database()
     db.open('./ECS165')
     # Getting the existing Grades table
     grades_table = db.get_table('Grades')
    # create a query class for the grades table
     query = Query(grades_table)
     result = query.select(1,0,[1,1,1,1,1])
     for record in result:
        print("Column Values:", record.columns)
     columns=[None,None,None,None, 1000 + loops]
     query.update(1,*columns)
     record = query.select(1,0, [1,1,1,1,1])
     print ("ReOpen 1",record[0].columns)
     db.close()


for i in range (0,10):
    read_after_persist(i)