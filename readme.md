# MysqlConnector class
 - 설치필요한 패키지
  - [link](https://dev.mysql.com/downloads/connector/python/8.0.html)
 - config.json 파일의 내용을 수정하셔야 합니다.

### functions
- insert(self,table,key,insert_data)
  - table
    - 저장할 태이블 명
  - key
    - 저장하는 insert_data의 컬럼명(순서대로)
      - dictionary list에서 키를 요청하여 사용합니다.
        **해당 row에는 모든 밸류값이 들어가있어야 오류가 안납니다.**
        ```
        keys = list(buffData[0].keys())
        ```
  - insert_data
    - 저장 데이터 목록
    - dictionary list 를 루프돌려서 data 차트로 만들어주는 소스입니다.
    ```
    buffDataList = []
        for data in buffData :
            buffDataList.append(list(data.values()))
    ```
- select(self,query,data)
  - query
    - 실행할 쿼리
  - data (where data)
    - data 항목

# FileStream class
  - 설치필요한 패키지
    [link](https://github.com/RaRe-Technologies/smart_open)

### functions
  - **staticmethod** getFileListfromDirectory(base_dir,sign):
    - base_dir
      - 디랙토리 경로
    - sign
      - 파일타입(ex : .json)
  - **staticmethod** getDataFromFile(file_name):
    - file_name
      - 파일의 절대경로(추천)
  - **staticmethod** getDataFromS3(server_path,file_path):
    - server_path
      - 서버 url 정보
    - file_path
      - 서버내에서의 파일경로
  - **staticmethod** uploadToS3(type,server_path,file_path,data):
    - type
      - support type
        - dictionary
        - file
    - server_path
      - 서버경로
    - file_path
      - 파일경로
    - data
      - 업로드할 데이터

# JsonUtil class

### functions
  - **staticmethod** jsonToDictionary(string)
    - String to Json
  - **staticmethod** dictionaryToJson(data)
    - Json to String
