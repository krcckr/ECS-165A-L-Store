import json


class Page:

    def __init__(self, tail=False):
        self.num_records = 0
        self.data = bytearray(4096)
        self.nxt_page = None
        self.tail = tail
        self.curr_index = 0
        self.TPS = 0
        self.pinned = False
        self.dirty = False

    @classmethod
    def toJson(cls):
        return json.dumps(cls, default=lambda o: o.__dict__)

    def insert(self, val):
        while self.pinned == True:
            continue

        self.pinned = True

        long_byte = val.to_bytes(8, "big")
        for i in range(8):
            self.data[(self.num_records * 8) + i] = long_byte[i]

        """for i in range(8):
            self.data[self.curr_index] = long_byte[i]
            self.curr_index += 1
        """

        self.num_records += 1
        self.pinned = False
        self.dirty = True

    def update_record(self, index, new_val):
        while self.pinned == True:
            continue
        self.pinned = True

        for i in range(8):
            self.data[index + i] = new_val[i]

        self.pinned = False
        self.dirty = True

    def has_capacity(self):
        return self.num_records * 8 < 4096

    def write(self, value):
        self.num_records += 1
        pass

    def get_data(self):
        while self.pinned == True:
            continue
        self.pinned = True

        data_list = []
        for i in range(self.num_records):
            data_list.append(int.from_bytes(self.data[i * 8: i * 8 + 8], 'big'))

        self.pinned = False

        return data_list

    def set_tps(self, tps):
        self.TPS = tps

    @classmethod
    def toLst(cls):
        datalst = []
        for i in range(0, len(cls.data), 8):
            bytes = cls.data[i: i + 8]
            datalst.append(int.from_bytes(bytes, 'big'))
        return datalst