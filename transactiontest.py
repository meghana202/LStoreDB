from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

# Create a database instance
db = Database()

# Create a table
grades_table = db.create_table('Grades', 5, 0)

# Insert records using a transaction
transaction = Transaction()
query = Query(grades_table)  # Create a Query instance

transaction.add_query(query.insert, grades_table, 1, 70, 85, 89, 92)  # Using student ID 1
transaction.add_query(query.insert, grades_table, 2, 70, 80, 95, 90)  # Using student ID 2
transaction.add_query(query.insert, grades_table, 3, 95, 88, 92, 87)  # Using student ID 3

# Run the transaction
result = transaction.run()

if result:
    print("Transaction committed successfully.")
else:
    print("Transaction failed to commit.")


'''# Print the results of the transactions
select_test_cases = [
    {"student_id": 1, "search_key_index": 0, "projected_columns_index": [1, 1, 1, 1, 1]},
    {"student_id": 2, "search_key_index": 0, "projected_columns_index": [1, 1, 1, 1, 1]},
    {"student_id": 3, "search_key_index": 0, "projected_columns_index": [1, 1, 1, 1, 1]},
]

for case in select_test_cases:
    student_id = case["student_id"]
    search_key_index = case["search_key_index"]
    projected_columns_index = case["projected_columns_index"]
    print(f"Test case: Select columns for student ID {student_id}, using search key index {search_key_index}, and projected columns: {projected_columns_index}")

    result = query.select(student_id, search_key_index, projected_columns_index)
    print("Result:")
    for record in result:
        print("Column Values:", record.columns)
    print()
'''