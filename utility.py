import base64
import hashlib
import os
from Crypto import Random
from Crypto.Cipher import AES
import base64

class utility(object):
     def __init__(self):
        """Return a Customer object whose name is *name*."""
        self.user = base64.b64decode('c3lzZGJh').decode("utf-8")
        self.password = base64.b64decode('ZSQxc19z').decode("utf-8")
     def db_connection_string(self):
         #str= """ host='192.168.3.40', user='', password='', database='ESI_JUN_2017' """""
         return base64.b64decode('aG9zdD0nMTkyLjE2OC4zLjQwJywgdXNlcj0nc3lzZGJhJywgcGFzc3dvcmQ9J2UkMXNfcycsIGRhdGFiYXNlPSdFU0lfSlVOXzIwMTcn').decode("utf-8")

     def recipients(self,DAG):
         file = open(os.path.join("/root/airflow/dags/recipients.txt"), 'r')
         lines = file.readlines()
         file.close()
         for line in lines:
             parts = line.split(':')
             if (parts[0]==DAG): return parts[1]
