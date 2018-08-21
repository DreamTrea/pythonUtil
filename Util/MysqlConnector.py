import os

from Util.JsonUtil import JsonUtil as ju
from Util.FileSystem import FileSystem

class MysqlConnector:
    def __init__(self):
        import mysql.connector

        system_path = os.getcwd()+'\\'
        config_data = ju.jsonToDictionary(FileSystem.getDataFromFile(system_path+"config.json"))
        mysql_config = config_data.get('mysql')
        config = {'host':mysql_config.get('server'),
            'user':mysql_config.get('userId'),
            'passwd':mysql_config.get('passwd'),
            'db':mysql_config.get('database'),
            'port':mysql_config.get('port'),
            'use_pure':False
            }
        self.connect = mysql.connector.connect(**config);

    def __del__(self):
        print("mysql connection 종류")
        self.connect.close()

    """
    query : 실행할 쿼리
    *data :
        None 쿼리에 사용될 파라미터가 없을때
        *({}) 단일항목 사용할때
        *('','',...) 다중항목 사용할때
    """
    def select(self,query,data):
        cursor = self.connect.cursor()
        if data is None :
            cursor.execute(query)
        elif isinstance(data,str) :
            cursor.execute(query,data)
        else :
            cursor.execute(query,*data)
        keys = cursor.column_names
        rows = cursor.fetchall()
        buff = dict()
        for i in range(0,len(rows)):
            buff[i]=dict(zip(keys, rows[i]))

        cursor.close()
        del cursor
        return buff

    def insertMany(self,table,key,insert_data):
        keys = ','.join(key)
        value_question=[]
        for i in range(0,len(key)):
            value_question.append('%s')
        value_string = ','.join(value_question)
        del value_question
        cursor = self.connect.cursor()
        cursor.executemany('insert ignore into '+table+'('+keys+') values ('+value_string+')',insert_data)
        cursor.close()
        del cursor
        self.connect.commit()

    def insert(self,table,key,insert_data):
        keys = ','.join(key)
        value_question=[]
        for i in range(0,len(key)):
            value_question.append('%s')
        value_string = ','.join(value_question)
        del value_question
        cursor = self.connect.cursor()
        cursor.execute('insert ignore into '+table+'('+keys+') values ('+value_string+')',insert_data)
        cursor.close()
        del cursor
        self.connect.commit()
