from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed
import time

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

s = time.perf_counter()
for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])

e = time.perf_counter()
print(f"Insert finished in {round(e -s, 3)} s")

s = time.perf_counter()
# Check inserted records using select query
for key in records:
    # select function will return array of records 
    # here we are sure that there is only one record in t hat array
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)

e = time.perf_counter()
print(f"Read finished in {round(e -s, 3)} s")

s = time.perf_counter()
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # copy record to check
        original = records[key].copy()
        # update our test directory
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
            assert(False)
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None

e = time.perf_counter()
print(f"Update finished in {round(e -s, 3)} s")


keys = sorted(list(records.keys()))
s = time.perf_counter()
# aggregate on every column 
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)

e = time.perf_counter()
print(f"Sum Finished in {round(e -s, 3)} s")


# delet every third record and sum remaining
keys = sorted(list(records.keys()))
delete_keys = [keys[i] for i in range(len(keys)) if i % 3 == 0]

s = time.perf_counter()
for key in delete_keys:
    updated_columns = None
    for i in range(2, grades_table.num_columns):

        # copy record to check
        #original = records[key].copy()
        
        query.delete(key)
        record_objects = query.select(key, 0, [1, 1, 1, 1, 1])
        error = False

        if record_objects:
            error = True

        if error:
            print('delete error on', original, 'and', 'obtained', ': ', record.columns, ', correct: no record',)
            assert(False)
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)

e = time.perf_counter()
print(f"Delete finished in {round(e -s, 3)} s")



keys = sorted(list(records.keys()))
s = time.perf_counter()
# aggregate on every column 
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        new_keys = [key for key in keys[r[0]: r[1] + 1] if key not in delete_keys]
        column_sum = sum(map(lambda key: records[key][c], new_keys))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)

e = time.perf_counter()
print(f"Sum accurate after delete Finished in {round(e -s, 3)} s")



s = time.perf_counter()
for key in delete_keys:
    updated_columns = [None, None, None, None, None]
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # copy record to check
        original = records[key].copy()
        # update our test directory
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])
        error = False
        if record:
            error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
            assert(False)
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None

e = time.perf_counter()
print(f"Update finished in {round(e -s, 3)} s")