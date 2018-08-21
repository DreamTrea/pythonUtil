import os
#https://github.com/RaRe-Technologies/smart_open
from smart_open import smart_open
from Util.JsonUtil import JsonUtil as ju

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
                full_filename = '\\'.join([file_path, filename])
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
    def uploadToS3(type,server_path,file_path,data):
        with smart_open("//".join(server_path,file_path),'wb',encoding='utf8') as fout:

            if type.equal("dictionary"):
                fout.write(ju.dictionaryToJson(data))

            elif type.equal("file"):
                for line in smart_open(data,encoding='utf8'):
                    fout.write(line)
        pass
