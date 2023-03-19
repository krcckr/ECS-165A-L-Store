from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.locks = {} # key: RID, value: if lock is a reader or writer
        self.table = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        self.table = table
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            page = query.__self__.table.index.locate(query.__self__.table.key,args[0])
            query_cols = list(args)

            rid = self.table.index.locate(0, query_cols[0])
            if rid:
                rid = rid[0]

            lock = self.locks.get(rid, None)

            if query == query.__self__.select or query == query.__self__.sum:
                # check if table has a look and if it is a reader

                if not lock:
                    # no lock for this record
                    if query.__self__.table.lm.acquire_read(rid) == False:
                        return self.abort()
                    else:
                        self.locks[rid] = 'reader'

            else:
                # insert or update
                if not lock:
                    # insert op
                    if query.__self__.table.lm.acquire_write(rid) == False:
                        return self.abort()
                    else:
                        self.locks[rid] = 'writer'

                elif lock == "reader":
                    query.__self__.table.lm.release_read(rid)
                    if query.__self__.table.lm.acquire_write(rid) == False:
                        return self.abort()
                    else:
                        self.locks[rid] = 'writer'


        # Safe to execute all ops : no need to do rollback 
        for query,args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            

        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for i, (rid, l) in enumerate(self.locks.items()):
            if l == 'reader':
                self.queries[0][0].__self__.table.lm.release_read(rid)
            elif l == 'writer':
                self.queries[0][0].__self__.table.lm.release_writ(rid)
        print("Transaction aborted")
        return False

    
    def commit(self):
        # TODO: commit to database
        for i, (rid, l) in enumerate(self.locks.items()):
            if l == 'reader':
                self.queries[0][0].__self__.table.lm.release_read(rid)
            elif l == 'writer':
                self.queries[0][0].__self__.table.lm.release_write(rid)
        print("Transaction Committed")
        return True

