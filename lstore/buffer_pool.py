from .config import *
from .page import *



import concurrent.futures
import threading
import os
import numpy as np


from collections import OrderedDict


class BufferPool:
    size = BUFFER_SIZE
    num_base_pages = 0
    num_tail_pages = 0

    cwd = path = os.path.expanduser("./ECS165") + '/'

    base_path = cwd + "/base"
    tail_path = cwd + "/tail"
    lock = threading.Lock()

    active_page_directory = {} # key: RID, value: page and record index
    active_tail_directory = {} # key: RID, value: page and record index
    record_loc = {} # key: RID, value (tail, page number, record index) ex (true, 12, 0) 
    __pages = []
    __tails = {} # Key: range number, return: tail range 
    free_space_pages = [] # tracks which pages have free space for new records
    LRU = OrderedDict()

    def __init__(self) -> None:
        for _, dirnames, _ in os.walk(self.base_path):
            self.num_base_pages += len(dirnames)

        for _, dirnames, _ in os.walk(self.tail_path):
                self.num_tail_pages += len(dirnames)


    @classmethod
    def is_full(cls):
        return len(cls.active_page_directory + cls.active_tail_directory) >= cls.size

    

    @classmethod
    def __add_rids_to_directory(cls, page, is_base):
        directory = cls.active_page_directory
        if is_base:
            directory = cls.active_tail_directory

        is_space_free = False

        for i in range(0, 512) and not is_space_free:
            rid = int.from_bytes(page[0].data[(i* INT_SIZE): ((i+1) * INT_SIZE)], 'big')
            if rid > 0:
                directory[rid] = page

            else:
                is_space_free = True


        if is_space_free:
            cls.free_space_pages.append(page) 
    

    @classmethod
    def load_page(cls, path):
        page = []
        for filename in os.listdir(path):
            with open(filename, 'r') as f:
                col_attr = np.load(f)
                col = Page.from_file(col_attr)
                page.append(col)

        return page
    
    @classmethod
    def load_tail(cls, path, tail_range):
        pass


    @classmethod
    def load_frame_from_disk(cls, RID, page_name):
        _, page_num, record_index = cls.record_loc(RID)
        page, tail_range = None, None

        path = cls.cwd + page_name + f'/{page_num}'

        IND = 0

        # only load IND to check for latest record
        with open(f'{path}/0', 'r') as f:
            ind_col_attr = np.load(f)
            IND = int.from_bytes(ind_col_attr[1][record_index: record_index + INT_SIZE], 'big')

        # load latest record from the tail page
        if IND:
            path = cls.cwd + f'tail/range{page_num // TAIL_HANDLE}'

            # each page
            for filename in os.listdir(path):
                new_page = []

                # each file
                for i in range(10):

                    with open(f'{path}/{filename}/col{i}_att', 'r') as f:
                        str_attr = f.read()
                        str_attr = str_attr.split('-')


                    with open(f'{path}/{filename}/col{i}', 'rb') as f:
                        data = f.read()

                    new_page.append(Page.from_file(str_attr, data))

                    # check if tail contains RID
                    if new_page[RID_COLUMN].contains(RID):
                        page = new_page
                        break

                    

        else:
            page = cls.load_page(cls, path)

        # add RIDS to the directory
        cls.__add_rids_to_directory(cls,page, IND)



    @classmethod
    def __evict(cls):
        evicted = False
        while not evicted:
            
            for key, page in cls.LRU.items():
                if not page.pinned:
                    cls.LRU.pop(key)
                    evicted = True
                    break


        # TODO: remove RIDS from directory

        #TODO: write dirty pages to disk



    @classmethod
    def load_frame_to_buffer(cls, RID, page_name):
        if cls.is_full(cls):
            cls.__evict(cls)

        cls.load_frame_from_disk(RID, page_name)

    @classmethod
    def get_record(cls, RID):
        page, index = None, None

        if RID in cls.active_page_directory:
            page, index = cls.active_page_directory[RID]

        else:
            page, index = cls.active_tail_directory[RID]

        return page, index

        
    @classmethod
    def read(cls, RID, page_name, record_version):
        if RID not in cls.active_page_directory or cls.active_tail_directory:
            cls.load_frame_to_buffer(RID, page_name)

        page, index = cls.get_record(RID)

        cols = []

        for col in page[NUM_META: ]:
            val = int.from_bytes(col.data[index: index + 8], 'big')
            cols.append(val)

        return cols
    
    @classmethod
    def get_page(cls, RID, page_name):
        if RID not in cls.active_page_directory or cls.active_tail_directory:
            cls.load_frame_to_buffer(RID, page_name)

        page, index = cls.get_record(RID)

        return index, page
    
    @classmethod
    def write(cls, RID, cols, num_total_columns, schema_encoding, int_datetime, table_name):
        page = None

        # check if a page has space:
        if cls.free_space_pages:
            page = cls.free_space_pages.pop()

         # obtain last page from disk
        else:
            if not cls.num_base_pages:
                page = [Page() for _ in range(num_total_columns)]
                cls.__pages.append(page)
                cls.free_space_pages.append(page)

            else:
                path = f'{cls.base_path}/{cls.num_base_pages - 1}'
                page = cls.load_page(path)

                # check if last page is full
                if not page[0].has_capacity():
                    n = len(page)
                    page = [Page() for _ in range(num_total_columns + NUM_META)]
                    cls.free_space_pages.append(page)

                else:

                    # check buffer is not full

                    # add RIDs to directory
                    cls.__add_rids_to_directory(cls, page, False)


        # insert meta data
        page[INDIRECTION_COLUMN].insert(0) # col 0
        page[RID_COLUMN].insert(RID) # col 1
        page[TIMESTAMP_COLUMN].insert(int_datetime) # col 2
        page[SCHEMA_ENCODING_COLUMN].insert(int(schema_encoding, 2)) # col 3
        page[IS_DELETED].insert(0) # col 4


        # insert data from params 
        for i in range(NUM_META, num_total_columns):
            page[i].insert(cols[i-NUM_META])

        cls.active_page_directory[RID] = (page, (page[0].num_records - 1) * 8)


        if page[0].has_capacity() and page not in cls.free_space_pages:
            cls.free_space_pages.append(page)

        for col in page:
            col.dirty = True

        for col in page:
            col.pinned = False

    
    @classmethod
    def update(cls):
        pass

    @classmethod
    def write_to_disk(cls, page, path):

        if not os.path.isdir(path):
            os.mkdir(path)

        # only write dirty pages
        if page[0].dirty:
            # check page is not pinned
            while page[0].pinned:
                continue

            # write page attrib
            for i, col in enumerate(page):

                #write page attr
                str_attr = f'{col.num_records}-{col.nxt_page}-{col.tail}-{col.curr_index}-{col.dirty}-{col.pinned}-{col.col_num}-{col.path}'
                

                with open(f'{path}/col{i}_attr.txt', "w+") as f:
                    f.seek(0)
                    f.write(str_attr)
                    
                with open(f'{path}/col{i}.txt', "wb+") as f:
                    f.seek(0)
                    f.write(col.data)


    @classmethod
    def stablize_table(cls, table_name, table):
        path = os.path.expanduser(table_name)

        # base
        for page in cls.__pages:
            page_num = int.from_bytes(page[RID_COLUMN].data[0: 8], 'big')
            page_num //= MAX_RECORDS

            path = os.path.expanduser(f'{table_name}{table}/base/page{page_num}')
            cls.write_to_disk(page, path)


        # tail
        for key, tail_range in cls.__tails.items():
            range_num = int.from_bytes(tail_range[0][RID_COLUMN].data[0:8], 'big')
            range_num //= MAX_RECORDS
            range_num //= TAIL_HANDLE
            
            for i, tail in enumerate(tail_range):
                path = os.path.expanduser(f'{table_name}{table}/tail/range{range_num}/tail{i}')
                if not os.path.isdir(path):
                    os.mkdir(path)

                cls.write_to_disk(tail, path)
                



BufferPool()