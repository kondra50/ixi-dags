from utility import utility
import base64
import pymssql


UTL= utility()
str= UTL.db_connection_string()
# str2=str.replace('\\','')
# import pymssql, os
#
# str= """host='192.168.3.40', user='sysdba', password='e$1s_s', database='ESI_JUN_2017'"""""
# print(base64.b64encode(bytes(str,'utf-8')))
#
# try:
#     print('test')
#     print(base64.b64decode(
#         'aG9zdD0nMTkyLjE2OC4zLjQwJywgdXNlcj0nc3lzZGJhJywgcGFzc3dvcmQ9J2UkMXNfcycsIGRhdGFiYXNlPSdFU0lfSlVOXzIwMTcn'))
#     str1=base64.b64decode(
#         'aG9zdD0nMTkyLjE2OC4zLjQwJywgdXNlcj0nc3lzZGJhJywgcGFzc3dvcmQ9J2UkMXNfcycsIGRhdGFiYXNlPSdFU0lfSlVOXzIwMTcn').decode(
#         "utf-8")
#     str2=base64.b64decode(
#         'aG9zdD0nMTkyLjE2OC4zLjQwJywgdXNlcj0nc3lzZGJhJywgcGFzc3dvcmQ9J2UkMXNfcycsIGRhdGFiYXNlPSdFU0lfSlVOXzIwMTcn')
#     str3=bytes(str2,"utf-8")
#     conn = pymssql.connect(str3)
# except Exception as e:
#     print(e)
# print(str)

conn = pymssql.connect(host='192.168.3.40', user=UTL.user, password=UTL.password, database='ESIDB')
s = """
USE ESIDB

select INVOICE_NUMBER from ARFIM where INVOICE_DATE=  DATEADD(day, -10, CONVERT (date, SYSDATETIME()))

"""
resaults_as_dict = []
try:
    cur = conn.cursor()
    cur.execute(s)
    for row in cur:
        val = row
        res_as_dict = {
    'INVOICE_NUMBER': val[0]
}
        resaults_as_dict.append(res_as_dict)
except Exception as e:
    res_as_dict = {'ERROR': str(e)}
    resaults_as_dict.append(res_as_dict)
finally:
    cur.close()
    conn.close()
#return
for item in  resaults_as_dict:
    print(item['INVOICE_NUMBER'])
print(resaults_as_dict)
print(resaults_as_dict[len(resaults_as_dict)-1][0])
print(len(resaults_as_dict))


