"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from BTrees.OOBTree import OOBTree

class Index:

    def __init__(self, table, num_col):
        # One index for each table. All our empty initially.
        self.indices = [OOBTree() for _ in range(num_col)]

        self.table = table
        # create B-Tree for sum
        print(table.key)
        self.tree = OOBTree()


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, index, value):
        if value in self.indices[index]:
            return self.indices[index][value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass


    def insert_item(self, cols, RID):
        for i, key in enumerate(cols):
            if key in self.indices[i]:
                rids = self.indices[i][key]
                rids.append(RID)
                self.indices[i][key] = rids

            else:
                self.indices[i][key] = [RID]

    def update_item(self, old_cols, new_cols, RID):
        for i, key in enumerate(new_cols):
            if key != None:
                rids = self.indices[i][old_cols[i]]
                rids.remove(RID)

            if key in self.indices[i]:
                rids.append(RID)
                self.indices[i][key] = rids

            else:
                self.indices[i][key] = [RID]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # check no index for the col
        num_col = self.table.num_columns

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number < len(self.indices):
            self.indices[column_number] = OOBTree()

    
    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        if "table" in state:
            del state['table']
        return state

