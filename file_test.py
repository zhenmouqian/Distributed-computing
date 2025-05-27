import os
import xlutils.copy
import xlwt
import xlrd
import xlutils
import re
import struct
import json
import random

# with open("test123.txt", "r") as file:
# file_content = file.read()
# print(file_content)
# file_content = file.readline()
# print(file_content.strip())
# file_content = file.readlines()
# print(file_content)
# file_path = os.path.dirname(os.path.abspath(__file__))
# print(file_path)
# print(__file__)
# print(file_path)
# print(os.path.getsize(os.path.join(file_path, "test123.txt")))
# with open("test123.txt", "rb") as f:
#     for i in range(6):
#         file_name = f"test123_{i}.part"
#         with open(file_name, "wb") as part:
#             part.write(f.read(3))
# xls_test = xlwt.Workbook(encoding="ascii")
# worksheet = xls_test.add_sheet("Sheet1")
# xls_test.save("new_table.xls")


# xlrd_test: xlrd.book.Book = xlrd.open_workbook("new_table.xls")
# table: xlrd.sheet.Sheet = xlrd_test.sheet_by_index(0)
# print(table.ncols)
# print(table.col(1))
# print(list(table.col(1)))
# print(table.col_slice(1))
# print(table.col_types(1))
# print(table.col_values(1))
# print(table.cell(0, 0).value)
# print(table.cell_type(0, 0))
# new_wb: xlwt.Workbook = xlutils.copy.copy(xlrd_test)
# ws: xlwt.Worksheet = new_wb.get_sheet(0)
# ws.write(0, 0, 0)
# new_wb.save("new_table.xls")
# l = list(map(int, input().split(" ")))
# l = list(map(lambda x: x**2, l))
# l = list(filter(lambda x: x > 10, l))
# print(l)


# s = """catdogcat1dog1ct
# cat2dog2
# """
# # res = re.match("^cat1", s)
# # print(res.group())
# res = re.finditer("^cat", s, re.M)
# s_res = []
# for match in res:
#     ss = f"匹配到:{match.group()},start index:{match.start()},end index:{match.end()}"
#     s_res.append(ss)
# packed_data = struct.pack("if", 123, 456.789)
# with open("data.txt", "wb") as file:
#     file.write(packed_data)
# json_string = [
#     {
#         "latitude": 31.3102117,
#         "longitude": 120.635677,
#         "altitude": 14.006728518931597,
#         "type": 3,
#         "mission": -1,
#         "velocity": 1.0,
#     },
#     {
#         "latitude": 31.3102774,
#         "longitude": 120.6357092,
#         "altitude": 11.446819982628748,
#         "type": 2,
#         "mission": 3,
#         "velocity": 1.0,
#     },
# ]
# data = json.dumps(json_string)
# data1 = json.loads(data)
# for d in data1:
#     print(type(d))
# with open("setup1.txt", "r") as f:
#     allline = json.load(f)
#     print(len(allline))
#     # allline = f.readlines()
#     for d in allline:
#         d: dict
#         print(d.get("rank"), d.get("ip"))
# import psutil

# import netifaces

# print(psutil.net_if_addrs().get("WLAN")[1])
# print(psutil.net_if_stats())


# import datetime
# import random

# all_nums = []
# start = datetime.datetime.now()
# with open("test_num.txt", "w") as f:
#     for _ in range(9660):
#         data = random.randint(0, 100000)
#         f.write(str(data) + " ")
#         if _ % 100 == 0 and _ != 0:
#             f.write("\n")


# with open("setup1.txt", "w") as f:
#     config = [
#         {"rank": 0, "ip": "192.168.57.128", "port": 65432},
#         {"rank": 1, "ip": "192.168.57.123"},
#     ]
#     json.dump(config, f)

with open("setup1.txt", "w") as f:
    config = [
        {"rank": 0, "ip": "192.168.57.128", "port": 65432},
    ]
    json.dump(config, f)
