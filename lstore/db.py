from lstore.table import Table
from lstore.page import Page
import os
import glob
import numpy as np
import pickle as pk

class Database():

    def __init__(self):
        self.tables = {}
        self.path = None
        self.table_name = None
        pass

    # Not required for milestone1
    def open(self, path):
        # check if directory exists
        path = os.path.expanduser(path) + '/'
        if not os.path.isdir(path):
            os.mkdir(path)
        self.path = path
        
        print("database is now open")

    def close(self):
        for name, table in self.tables.items():
            print(table.name)
            table.write(table.name)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.table_name = name

        self.tables[name] = table

         # check if table directory exists
        path = os.path.expanduser(f'./ECS165/{name}/')
        if not os.path.isdir(path):
            os.mkdir(path)

        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)

    
    """
    # Returns table with the passed name
    """
    def __load_page(self, path):
        page_lst = []
        str_attr = None
        data = None

        n = len(next(os.walk(path))[1])


        for j in range(n):
            curr_path = os.path.expanduser(f'{path}/col{j}')
            if not os.path.isdir(curr_path):
                assert(False, f'The following path DNE: {curr_path}')

            # construct the page
            curr_page = Page()

            # obtain bytearray data
            curr_path_data = f'{curr_path}/data.txt'
            with open(curr_path_data, 'rb+') as f:
                f.seek(0)
                data = bytearray(f.read())

            # obtain page attr
            curr_path_data = f'{curr_path}/meta.txt'
            with open(curr_path_data, 'r') as f:
                    f.seek(0)
                    str_attr = f.read()

            str_attr = str_attr.split('-')
            num_records, nxt_page, tail, curr_index, TPS, pinned, dirty = str_attr
            
            curr_page.num_records = int(num_records)
            curr_page.data = data
            curr_page.nxt_page = nxt_page
            curr_page.tail = tail
            curr_page.curr_index = curr_index
            curr_page.TPS = TPS
            curr_page.pinned = pinned
            curr_page.dirty = dirty

            page_lst.append(curr_page)

        return page_lst



    
    def get_table(self, name):

        # for each table in the database folder
        path = glob.glob(self.path + "/*", recursive=True)
        for _, names in enumerate(path):
            table_att = None
            table_index = None
            primary_keys = None
            pages = []
            tails = []
            page_range = None


            # load attr
            with open(f'{path[0]}/attr/attr.txt', 'r') as f:
                table_att = f.read()

            table_att = table_att.split('-')

            key, table_name, num_columns, num_columns, num_records, num_total_columns, rid, update_rid = table_att

            table = Table(table_name, int(num_columns), int(key))
            table.num_records = int(num_records)
            table.num_total_columns = int(num_total_columns)
            table.rid = int(rid)
            table.update_rid = int(update_rid)

            # load index
            with open(f'{path[0]}/index/index', 'rb') as f:
                table_index = pk.load(f)
            table.index = table_index

            # load keys
            with open(f'{path[0]}/keys/keys.txt', 'rb') as f:
                primary_keys = pk.load(f)
            table.primary_keys = primary_keys

            # load range
            with open(f'{path[0]}/range/range', 'rb') as f:
                page_range = pk.load(f)
            table.page_range = page_range
            

            # load pages
            n = len(next(os.walk(f'{path[0]}/pages'))[1])

            for i in range(n):
                page_path =f'{path[0]}/pages/page{i}'
                pages.append(self.__load_page(page_path))
            table.page_ranges = pages

            # load tails
            n = len(next(os.walk(f'{path[0]}/tails'))[1])

            for i in range(n):
                tail_range = []
                k = len(next(os.walk(f'{path[0]}/tails/tail_range{i}'))[1])
                for j in range(k):
                    tail_path = f'{path[0]}/tails/tail_range{i}/tail{j}'
                    tail_range.append(self.__load_page(tail_path))
                tails.append(tail_range)

            table.tail_ranges = tails


            self.tables[table.name] = table

        for key, table in self.tables.items():

            if table.name == name:
                return table

