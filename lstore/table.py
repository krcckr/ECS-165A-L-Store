from lstore.index import Index
from lstore.lock_manager import LockManager
from lstore.buffer_pool import BufferPool as bp

from time import time, sleep

import concurrent.futures
from threading import Lock, Thread
import threading


# import repeat function from itertools module
from itertools import repeat
from collections import defaultdict
import pickle as pk
import os
import glob
import json
import numpy as np
from copy import deepcopy
from collections import deque
import os.path as osp

import datetime

from lstore.page import Page
from lstore.config import *


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class PageRanges:
    def __init__(self, select_directory, page_directory, tail_directory, total_columns):
        self.page_directory = page_directory
        self.tail_directory = tail_directory
        self.num_total_columns = total_columns
        self.select_directory = select_directory

    def load_base_page_4sel(self, RID):
        page = self.select_directory[RID]
        page_index = ((RID - 1) // 512)
        record_index = RID - (page_index * MAX_NUM_RECORD) - 1
        record_index *= INT_SIZE

        return record_index, page

    def load_base_page(self, RID):
        page = self.page_directory[RID]
        page_index =  ((RID - 1) // 512)
        record_index = RID - (page_index * MAX_NUM_RECORD) - 1
        record_index *= INT_SIZE

        return record_index, page

    def load_tail_page(self, IND):
        latest_page, latest_index = self.tail_directory[IND]

        return latest_index, latest_page

    def get_tail_page_index(self, RID): 
        tail_range_index = (RID - 1) // MAX_NUM_RECORD
        tail_range_index //= TAIL_HANDLE

        return tail_range_index

    def is_deleted(self, page, index):
        val = int.from_bytes(page[IS_DELETED].data[index: index + INT_SIZE], 'big')
        return val
    
    def load_record_tail(self, key, RID):
        index, page = self.load_tail_page(RID)
        print(page[0].tail)
        result = []
        record = Record(RID, key, result)

        if not self.is_deleted(page, index):

            for i in range(self.num_total_columns):
                int_val = int.from_bytes(page[i].data[index: index + INT_SIZE], "big")
                result.append(int_val)

            record.columns = result

        return record
        


    def load_record(self, key, RID, type=0):
        if type == 1:
            index, page = self.load_base_page_4sel(RID)
        else:
            index, page = self.load_base_page(RID)

        result = []
        result2 = []
        record = Record(RID, key, result)
        records = []

        if int.from_bytes(page[IS_DELETED].data[index: index + INT_SIZE], "big"):
            return records

        for i in range(self.num_total_columns):
                int_val = int.from_bytes(page[i].data[index: index + INT_SIZE], "big")
                result.append(int_val)

        record.columns = result
        records.append(record)

        IND = int.from_bytes(page[0].data[index: index + INT_SIZE], 'big')

        if IND:
            index , page = self.load_tail_page(IND)
            RID = IND

            if not self.is_deleted(page, index):

                for i in range(self.num_total_columns):
                    int_val = int.from_bytes(page[i].data[index: index + INT_SIZE], "big")
                    result2.append(int_val)
            
            records.append(Record(IND, key, result2))


        return records

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.select_directory = {}
        self.tail_directory = {}
        self.index = Index(self, num_columns)


        # lock management
        self.lm = LockManager()
        self.page_locks = defaultdict(lambda: defaultdict(threading.Thread))


        self.num_total_columns = num_columns + NUM_META
        self.num_records = 0

        #track all primary keys
        self.primary_keys = ()

        # next available rid id #
        self.rid = 0
        self.update_rid = 0

        # page ranges
        self.page_ranges = []

        # tail ranges
        self.tail_ranges = []

        # page range object
        self.page_range = PageRanges(self.select_directory, self.page_directory, self.tail_directory, self.num_total_columns)

        # Quit Merge in the background
        self.open_path = './ECS165'
        self.merge_queue = deque()
        # background thread
        background_thread = Thread(target=self.__merge, daemon=True, name='Merge')
        #background_thread.start()

        pass

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

    @classmethod
    def from_file(cls, objs):
        pass

    def __merge(self):
        while True:
            if len(self.merge_queue) != 0:
                print("merge is happening")

                # tail page
                tail_page = self.merge_queue.pop()

                # unique base rids
                base_rids = [*set(tail_page[BASE_RID_COLUMN].get_data())]

                # initialize base pages
                base_pages = []

                # filter
                unique_page_rid = {}
                for rid in base_rids:
                    page_index = ((rid - 1) // MAX_NUM_RECORD)
                    unique_page_rid[page_index] = rid

                for page_index, rid in unique_page_rid.items():
                    record_index, base_page = self.page_range.load_base_page(rid)
                    base_pages.append(base_page)

                for base_page in base_pages: # len(1)
                    base_rids = base_page[RID_COLUMN].get_data()
                    consolidate_page = deepcopy(base_page)

                    updated_RID = []
                    TPS = 0
                    set_TPS = False
                    for i in range(len(tail_page[RID_COLUMN].data) - 8, -1, -8):

                        # Get BaseRID of that Tail RID through page_directory
                        BaseRID = int.from_bytes(tail_page[BASE_RID_COLUMN].data[i: i + 8], 'big')

                        if BaseRID in base_rids and BaseRID not in updated_RID:  # to be modified

                            IND = int.from_bytes(tail_page[RID_COLUMN].data[i: i + 8], 'big')

                            if set_TPS == False:
                                TPS = IND
                                set_TPS = True

                            updated_RID.append(BaseRID)

                            slot = base_rids.index(BaseRID) * 8  # ?

                            consolidate_page[SCHEMA_ENCODING_COLUMN].update_record(slot, int(0).to_bytes(8, 'big'))

                            for j in range(NUM_META, self.num_total_columns):
                                consolidate_page[j].update_record(slot, tail_page[j].data[i:i + 8])

                        # update page_directory !!!
                        # print(consolidate_page)
                        self.page_directory[BaseRID] = consolidate_page
                        self.page_range.page_directory[BaseRID] = consolidate_page

                    # write the consolidated page into the disk
                    consolidate_page[0].set_tps(TPS)
                    # path = os.path.expanduser(f'./ECS165/{self.name}/pages')
                    # self.__save_single_page(self.name, consolidate_page, path, "page")
                    self.page_ranges.append(consolidate_page)

                    # Save and prepare to de-allocate the outdated base pages
                    # de_allocation_space.append(base_page_index)

            sleep(0.1)  # force the thread to sleep for a fraction of a second each iteration
 

    def get_nxt_rid(self):
            """
            Gives the next rid # for a new Record object

            Returns:
                new_rid: int
            """
            
            self.rid += 1
            return self.rid

    def get_nxt_update_rid(self):
        """
        Gives the next rid # for a new Record object

        Returns:
            new_rid: int
        """
        
        self.update_rid += 1

        return self.update_rid

    def datetime_int(self):
        date = datetime.datetime.now()
        int_date = int(date.strftime("%Y%m%d%H%M%S"))
        
        return int_date


    def add_record(self, columns):
        schema_encoding = '0' * self.num_columns

        pk = columns[0]
        

        # check the number of columns or  if id exists
        if len(columns) != self.num_columns or pk in self.primary_keys:
            return False
        
        # add primary key for tracking
        self.primary_keys = (*self.primary_keys, pk)
        

        # create a new record
        rid = self.get_nxt_rid()


        # db is empty or base_page is full
        if not self.page_ranges or not self.page_ranges[-1][0].has_capacity():
            # create a new page and append it to page_ranges
            self.page_ranges.append([Page() for _ in range(self.num_total_columns)])


        # get the last base page 
        base_page = self.page_ranges[-1]


        #update index
        self.index.insert_item(columns, rid)


        # update page_directory
        self.page_directory[rid] = base_page
        self.select_directory[rid] = base_page


        # insert meta data
        base_page[INDIRECTION_COLUMN].insert(0) # col 0
        base_page[INDIRECTION_COLUMN].dirty = True

        base_page[RID_COLUMN].insert(rid) # col 1
        base_page[TIMESTAMP_COLUMN].insert(self.datetime_int()) # col 2
        base_page[SCHEMA_ENCODING_COLUMN].insert(int(schema_encoding, 2)) # col 3
        base_page[IS_DELETED].insert(0) # col 4
        base_page[BASE_RID_COLUMN].insert(rid) # col 5


        # insert data from params 
        for i in range(NUM_META, self.num_total_columns):
            base_page[i].insert(columns[i-NUM_META])


        # balance the ratio of base pages to tail bases with the tail handle
        if len(self.page_ranges) // TAIL_HANDLE >= len(self.tail_ranges):
            self.tail_ranges.append([[Page(True) for _ in range(self.num_total_columns)]])


        return rid
    

    def __record_by_rid(self, search_key, rids, projected_columns_index, relative_version, type=0):
        all_records = []

        for rid in rids:
                records = self.page_range.load_record(search_key, rid, type)

            
                for i in range(relative_version + 1, 1):
                    # if no second record, then record is deleted
                    if len(records[-1].columns) == 0:
                        all_records.append(records[-1])
                        return all_records
                    
                    rid = records[-1].columns[INDIRECTION_COLUMN]
                    if rid == 0:
                        break
                    record = self.page_range.load_record_tail(search_key, rid)
                    records.append(record)
                    
                    
                if ((-1 * relative_version) + 1) >= len(records):
                    record = records[0]

                else:
                    record = records[(-1 * relative_version) + 1]

                if record.columns and search_key in record.columns:
                    record.columns = record.columns[NUM_META:]
                    for i, col in enumerate(projected_columns_index):
                        if col == None:
                            record.columns.pop(NUM_META+i)

                    all_records.append(record)

        return all_records


    def retrieve_record(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids = self.index.locate(search_key_index, search_key)
        records = []
        all_records = []

        # Manual search for no index-col
        if not rids:
            rids = []
            for i in range(1, self.rid + 1):
                rids.append(i)
            
            all_records = self.__record_by_rid(search_key, rids, projected_columns_index, relative_version)

        
        else:
            for rid in rids:
                records = self.page_range.load_record(search_key, rid, 1)

            
                for i in range(relative_version + 1, 1):
                    # if no second record, then record is deleted
                    if len(records[-1].columns) == 0:
                        all_records.append(records[-1])
                        return all_records
                    
                    rid = records[-1].columns[INDIRECTION_COLUMN]
                    if rid == 0:
                        break
                    record = self.page_range.load_record_tail(search_key, rid)
                    records.append(record)
                    
                    
                if ((-1 * relative_version) + 1) >= len(records):
                    record = records[0]

                else:
                    record = records[(-1 * relative_version) + 1]

                if record.columns:
                    record.columns = record.columns[NUM_META:]
                    for i, col in enumerate(projected_columns_index):
                        if col == None:
                            record.columns.pop(NUM_META+i)
                all_records.append(record)

        return all_records


    def update(self, search_key, cols, is_delete=0):
 
        # if self.tail_ranges:
        #     if not self.tail_ranges[0][-1][0].num_records > 0:
        #         pass
        #         #self.merge_queue.append(self.tail_ranges[0][-1])

        if is_delete == 0 and cols[0] != None and cols[0] != search_key:
            return

        # get record from base
        rids = self.index.locate(0,search_key)
        rid = None

        if rids:
            rid = self.index.locate(0,search_key)[0]

        else:
            return

        index, page = self.page_range.load_base_page(rid)

        latest_index, latest_page = index, page

        # get latest record from tail
        IND = page[0].data[index: index + 8]
        IND = int.from_bytes(IND, 'big')

        tail_page = None
        
        if IND:
            latest_index, latest_page = self.page_range.load_tail_page(IND)
            tail_page = latest_page

        # check record is not marked "deleted"
        if self.page_range.is_deleted(latest_page, latest_index):
            return
        
        # get tail page for record from tail ranges
        tail_range_index = self.page_range.get_tail_page_index(rid)
        tail_page = self.tail_ranges[tail_range_index][-1]


        # base page is dirty now
        page[0].dirty = True


        if not tail_page or not tail_page[0].has_capacity(): # Error fixed
            if tail_page:
                self.merge_queue.append(tail_page)
            # create a new tail page and append it to page_ranges
            self.tail_ranges[tail_range_index].append([Page(True) for _ in range(self.num_total_columns)])
            tail_page = self.tail_ranges[tail_range_index][-1]



        # set meta data
        latest_rid = latest_page[1].data[latest_index: latest_index + 8]
        latest_rid = int.from_bytes(latest_rid, 'big')
        tail_page[INDIRECTION_COLUMN].insert(0) # col 0 (IND)
        tail_page[INDIRECTION_COLUMN].dirty = True
        

        tail_rid = self.get_nxt_update_rid()
        tail_page[RID_COLUMN].insert(tail_rid) # col 1 (RID)

        tail_page[TIMESTAMP_COLUMN].insert(self.datetime_int()) # col 2 (datetime)

        schema_encoding = int.from_bytes(latest_page[3].data[latest_index: latest_index + INT_SIZE], 'big')
        tail_page[SCHEMA_ENCODING_COLUMN].insert(schema_encoding)

        base_rid = int.from_bytes(latest_page[BASE_RID_COLUMN].data[latest_index: latest_index + INT_SIZE], 'big')
        tail_page[BASE_RID_COLUMN].insert(base_rid)

        # update base linkage
        page[0].update_record(index,tail_rid.to_bytes(8, 'big') )

        # mark page as dirty

        if is_delete:
            tail_page[IS_DELETED].insert(1) # col 4 (IS_DELETED))

            for i in range(self.num_columns):
                value = latest_page[i+NUM_META].data[latest_index: latest_index + INT_SIZE]
                int_value = int.from_bytes(value, 'big')
                tail_page[i+NUM_META].insert(int_value) 

        else:
            tail_page[IS_DELETED].insert(0) # col 4 (IS_DELETED)
            # insert data
            for i, val in enumerate(cols):
                # commulative updates
                if val != None:
                    tail_page[i + NUM_META].insert(val)
                else:
                    value = latest_page[i+NUM_META].data[latest_index: latest_index + INT_SIZE]
                    int_value = int.from_bytes(value, 'big')
                    tail_page[i+NUM_META].insert(int_value)


        # update tail directory
        self.tail_directory[tail_rid] = (tail_page, (tail_page[0].num_records * INT_SIZE)-INT_SIZE)
        self.page_range.tail_directory[tail_rid] = (tail_page, (tail_page[0].num_records * INT_SIZE)-INT_SIZE) 



    def sum(self, start_range, end_range, aggregate_column_index, relative_version = 0):
        cols = [ i == aggregate_column_index for i in range(self.num_columns)]
        projected_cols = [1,1,1,1,1]
        res = 0
        
        for i in range(start_range, end_range + 1):
            if i in self.index.indices[0]:
                record = self.retrieve_record(i, 0, projected_cols, relative_version) #self.retrieve_record(i, 0, projected_cols, 0)
                if record[-1].columns:
                    value = record[-1].columns[aggregate_column_index]
                    res += value
        
        return res

        with concurrent.futures.ThreadPoolExecutor() as executor:
            ranges = [i for i in range(start_range, end_range + 1) if i in self.index.indices]
            results = executor.map(self.retrieve_record, ranges, repeat(0), repeat(projected_cols), repeat(0))

            for result in results:
                res += result.columns[aggregate_column_index]

        return res


    def __save_page(self, table_name, pages, path, page_type):
        # save pages
        for i, page in enumerate(pages):

            # check if page is dirty
            if not page[0].dirty:
                return
            
            # check if base sub-directory exists
            page_path = os.path.expanduser(f'{path}/{page_type}{i}')
            if not os.path.isdir(page_path):
                os.mkdir(f'{page_path}')

            curr_path = f'{page_path}/TPS.txt'
            with open(curr_path, 'w') as f:
                f.seek(0)
                f.write(str(page[0].TPS))

            for j, col in enumerate(page):
                curr_path = os.path.expanduser(f'{page_path}/col{j}')
                if not os.path.isdir(curr_path):
                    os.mkdir(curr_path)

            
                curr_path_data = f'{curr_path}/data.txt'
                with open(curr_path_data, 'wb+') as f:
                    f.seek(0)
                    f.write(col.data)

                curr_path_data = f'{curr_path}/meta.txt'
                str_attr = f'{col.num_records}-{col.nxt_page}-{col.tail}-{col.curr_index}-{col.TPS}-{col.pinned}-{col.dirty}'
                with open(curr_path_data, 'w+') as f:
                        f.seek(0)
                        f.write(str_attr)

    def __save_single_page(self, table_name, page, path, page_type):
        # check if base sub-directory exists
        i = len(self.page_ranges)
        page_path = os.path.expanduser(f'{path}/{page_type}{i}')
        if not os.path.isdir(page_path):
            os.mkdir(f'{page_path}')

        curr_path = f'{page_path}/TPS.txt'
        with open(curr_path, 'w') as f:
            f.seek(0)
            f.write(str(page[0].TPS))

        for j, col in enumerate(page):
            curr_path = os.path.expanduser(f'{page_path}/col{j}')
            if not os.path.isdir(curr_path):
                os.mkdir(curr_path)

            curr_path_data = f'{curr_path}/data.txt'
            with open(curr_path_data, 'wb+') as f:
                f.seek(0)
                f.write(col.data)

            curr_path_data = f'{curr_path}/meta.txt'
            str_attr = f'{col.num_records}-{col.nxt_page}-{col.tail}-{col.curr_index}'
            with open(curr_path_data, 'w+') as f:
                f.seek(0)
                f.write(str_attr)


    def creat_folder(self, table_name, folder_lst):
        # check if base sub-directory exists
        path = os.path.expanduser(f'./ECS165/{table_name}/')
        if not os.path.isdir(path):
            os.mkdir(path)

        for folder in folder_lst:
            # check if base sub-directory exists
            path = os.path.expanduser(f'./ECS165/{table_name}/{folder}')
            if not os.path.isdir(path):
                os.mkdir(path)

        

    def write(self, table_name):
        # create needed folders
        folder_lst = ["pages", "tails", "attr", "index", "range", "keys"]
        self.creat_folder(table_name, folder_lst)

        # save pages
        path = os.path.expanduser(f'./ECS165/{table_name}/pages') 
        self.__save_page(table_name, self.page_ranges, path, "page")

        # save tails
        for i, tail_range in enumerate(self.tail_ranges):
            # check if base sub-directory exists
            path = os.path.expanduser(f'./ECS165/{table_name}/tails/tail_range{i}')
            if not os.path.isdir(path):
                os.mkdir(path)
            
            self.__save_page(table_name, tail_range, path, "tail")


        with open(f'./ECS165/{table_name}/index/index', 'wb+') as f:
                pk.dump(self.index, f)


        with open(f'./ECS165/{table_name}/range/range', 'wb+') as f:
                pk.dump(self.page_range, f)


        with open(f'./ECS165/{table_name}/keys/keys.txt', 'wb+') as f:
                pk.dump(self.primary_keys, f)

        

        
        str_attr = f'{self.key}-{self.name}-{self.num_columns}-{self.num_columns}-{self.num_records}-{self.num_total_columns}-{self.rid}-{self.update_rid}'
        with open(f'./ECS165/{table_name}/attr/attr.txt', 'w+') as f:
                f.write(str_attr)             


       


