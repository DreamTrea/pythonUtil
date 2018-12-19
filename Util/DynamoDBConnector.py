# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 18:27:27 2018

@author: SEC
"""
import os
import boto3

from src.Util.JsonUtil import JsonUtil as ju
from src.Util.FileSystem import FileSystem

class DynamoDBConnector:
    # ==========================================
    # __init__
    #     생성자로서 DynamoDB에 접속하여 초기 태이블 유무에 따라 초기 테이블 생성
    # ==========================================
    def __init__(self):
        system_path = os.getcwd()+'/'
        config_data = ju.jsonToDictionary(FileSystem.getDataFromFile(system_path+"config.json"))
        self.dynamodb_config = config_data.get('DynamoDB')
        self.dynamodb_resource = boto3.resource("dynamodb",
                                       region_name=self.dynamodb_config.get("region_name"),
                                       endpoint_url=self.dynamodb_config.get("endpoint_url"),
                                       aws_access_key_id=self.dynamodb_config.get("accesskey"),
                                       aws_secret_access_key=self.dynamodb_config.get("secretkey"),
                                        )
        self.dynamodb_client = boto3.client("dynamodb",
                                       region_name=self.dynamodb_config.get("region_name"),
                                       endpoint_url=self.dynamodb_config.get("endpoint_url"),
                                       aws_access_key_id=self.dynamodb_config.get("accesskey"),
                                       aws_secret_access_key=self.dynamodb_config.get("secretkey"),
                                        )
        tables = self.dynamodb_client.list_tables()

        if not('Table1' in tables.get('TableNames')):
            self.init_RecommendationPost()

        if not('Table2' in tables.get('TableNames')):
            self.init_RecommendationTag()

    # ==========================================
    # insert
    # Dynamodb에 데이터 저장
    #     table_name : 테이블 명
    #     data : insert하고자 하는 데이터 목록 json 데이터
    # ==========================================
    def insert(self,table_name,data):
        table = self.dynamodb_resource.Table(table_name)
        with table.batch_writer() as batch:
            for buff in data[table_name]:
                batch.put_item(Item=buff)

    def initTable1(self):
        self.dynamodb_resource.create_table(
            TableName='Table1',
            KeySchema=[
                {'AttributeName':'user','KeyType':'HASH'},
                {'AttributeName':'createdTime','KeyType':'RANGE'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'influenceIndex',
                    'KeySchema': [
                        {'AttributeName': 'createdTime','KeyType':'HASH'},
                        {'AttributeName': 'order2','KeyType':'RANGE'}
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 50,
                        'WriteCapacityUnits': 50
                    }
                }
            ],
            AttributeDefinitions=[
                {'AttributeName': 'user','AttributeType': 'N'},
                {'AttributeName': 'order2','AttributeType': 'S'},
                {'AttributeName': 'createdTime','AttributeType': 'N'}
            ],

            ProvisionedThroughput={
                'ReadCapacityUnits': 50,
                'WriteCapacityUnits': 50
            }
        )

    def initTable2(self):
        self.dynamodb_resource.create_table(
            TableName='Table2',
            KeySchema=[
                {'AttributeName':'tag','KeyType':'HASH'},
                {'AttributeName':'createdTime','KeyType':'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'tag','AttributeType': 'S'},
                {'AttributeName': 'createdTime','AttributeType': 'N'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 50,
                'WriteCapacityUnits': 50
            }
        )
