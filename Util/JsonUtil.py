import json

class JsonUtil(object):
    """docstring for Json."""
    def __init__(self, arg):
        super(json, self).__init__()
        self.arg = arg

    @staticmethod
    def jsonToDictionary(string):
        return json.loads(string)

    @staticmethod
    def dictionaryToJson(data):
        return json.dumps(data, ensure_ascii=False)
