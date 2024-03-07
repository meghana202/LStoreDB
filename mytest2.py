from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed


records = {}
number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1
keys = {}

def reorganize_result(result):
    val = list()
    for r in result:
        val.append(r.columns)
    val.sort()
    return val

# 30 points in total
def correctness_tester1():
    records = [
        [0, 1, 1, 2, 1],
        [1, 1, 1, 1, 2],
        [2, 0, 3, 5, 1],
        [3, 1, 5, 1, 3],
        [4, 2, 7, 1, 1],
        [5, 1, 1, 1, 1],
        [6, 0, 9, 1, 0],
        [7, 1, 1, 1, 1],
    ]
    db = Database()
    db.open("./CT")
    test_table = db.create_table('test', 5, 0)
    query = Query(test_table)
    for record in records:
        query.insert(*record)
        # select on columns with index
        test_table.index.create_index(2)
        result = reorganize_result(query.select(1, 2, [1,1,1,1,1]))
        print(result)
        if len(result) == 4:
            if records[0] in result and records[1] in result and records[5] in result and records[7] in result:
                print("PASS[0]")
            else:
                print("Error[0]")
        else:
            print("Error[0]")

m2tests = [1,0,0]
if m2tests[0] == 1:
    print("==========correctness tester===============")
    correctness_tester1() 
'''
    correctness_tester2() 
if m2tests[1] == 1:
    print("==========durability tester================")
    generte_keys()
    durability_tester1()
    durability_tester2() 
if m2tests[2] == 1:
    print("==========merging tester===================")
    start = timer()
    merging_tester()
    end = timer()
    print()
    print("Total time Taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
'''