import os
#https://github.com/RaRe-Technologies/smart_open
from smart_open import smart_open
from src.Util.JsonUtil import JsonUtil as ju
import boto3

class FileSystem(object):

    """docstring for FileLoader."""
    def __init__(self, arg):
        super(FileSystem, self).__init__()
        self.arg = arg

    @staticmethod
    def getFileListfromDirectory(base_dir,sign):
        file_path_list = []
        for file_path, dir, files in os.walk(base_dir):
            for filename in files:
                full_filename = '/'.join([file_path, filename])
                ext = os.path.splitext(filename)[-1]
                if ext == sign:
                    file_path_list.append(full_filename)
        return file_path_list

    @staticmethod
    def getDataFromFile(file_name):
        buff_line = []
        for line in smart_open(file_name,encoding='utf8'):
            buff_line.append(line)
        return ''.join(buff_line)

    """
    server_path : s3 service path
    file_path : file name on server
    return type : string data
    """
    @staticmethod
    def getDataFromS3(server_path,file_path):
        fileLine = []

        for line in smart_open("//".join(server_path,file_path),'rb'):
            fileLine.append(line)
        return "".join(fileLine)

    """
    type : saving type
        - support type
            - dictionary
            - file
    server_path : s3 service path
    file_path : file name on server
    data : directory data or file data path
    """
    @staticmethod
    def uploadToS3(type,file_name,data,localPath=''):
        system_path = os.getcwd()+'/'
        config_data = ju.jsonToDictionary(FileSystem.getDataFromFile(system_path+"config.json"))

        config_data = config_data.get('s3_bucket')
        if type=='file' :
            client = boto3.client('s3',
                    region_name=config_data.get('region_name'),
                    aws_access_key_id=config_data.get('accesskey'),
                    aws_secret_access_key=config_data.get('secretkey')
                )
            client.upload_file(file_name,config_data.get('serverPath'),data)
        else:
            session = boto3.resource('s3',
                region_name=config_data.get('region_name'),
                aws_access_key_id=config_data.get('accesskey'),
                aws_secret_access_key=config_data.get('secretkey')
            )
            session.Bucket(config_data.get('serverPath')).put_object(Key=file_name,Body=data)
#
#    @staticmethod
#    def uploadToS3(type,server_path,file_name,data):
#        with smart_open("//".join(server_path,file_path),'wb',encoding='utf8') as fout:
#
#            if type.equal("dictionary"):
#                fout.write(ju.dictionaryToJson(data))
#
#            elif type.equal("file"):
#                for line in smart_open(data,encoding='utf8'):
#                    fout.write(line)
#        pass
