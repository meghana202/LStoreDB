from lstore.db import Database
from lstore.query import Query
db = Database()
grades_table = db.create_table('Grades',5,0)
query = Query(grades_table)

query.insert(1,70,85,89,92)
query.insert(2,70,80,95,90)
query.insert(3,95,88,92,87)
query.insert(4,100,88,92,87)
query.insert(5,105, 88, 92, 87)

print("Indices after creating index:", query.table.index.indices)

select_test_cases = [
{"student_id": 1, "search_key_index": 0,"projected_columns_index":[1,1,1,1,1]},
{"student_id": 2, "search_key_index": 0 ,"projected_columns_index":[1,1,1,1,1]},
{"student_id": 3, "search_key_index": 0,"projected_columns_index":[1,1,1,1,1]},
]

for case in select_test_cases:
    student_id = case["student_id"]
    search_key_index = case["search_key_index"]
    projected_columns_index = case["projected_columns_index"]
    print(f"test case: select columns for student ID {student_id}, using search key index {search_key_index}, and projected columns: {projected_columns_index}")

    result = query.select(student_id, search_key_index, projected_columns_index)
    print("Result:")
    for record in result:
        print("Column Values:", record.columns)
    print()