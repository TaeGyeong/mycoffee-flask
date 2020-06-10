import os
from pyspark.mllib.recommendation import ALS
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 위경도 거리계산용.
from haversine import haversine
from difflib import SequenceMatcher

class dataEngine:

    def search(self, name, localName):
        return self.cafe_RDD \
            .filter(lambda tokens: SequenceMatcher(None, str(tokens[7]), str(name)).ratio() * 100 > 40) \
            .filter(lambda tokens: str(tokens[5]).split()[0] == localName) \
            .map(lambda tokens: (tokens, SequenceMatcher(None, str(tokens[7]), str(name)).ratio() * 100)) \
            .take(100000)
        

    def get_all_data(self, curLat, curLng):
        p1 = (curLat, curLng)
        logger.info("current user's location:", p1[0], p1[1])
        print(p1)
        return self.cafe_RDD\
            .filter(lambda tokens: tokens[10] != 'None' or tokens[11] != 'None') \
            .filter(lambda tokens: haversine(p1, (float(tokens[10]), float(tokens[11]))) < 2.00).take(100000)
            #.map(lambda tokens: (tokens[10], tokens[11])).take(100000)\
    
    def get_price_data(self, manageNum):
        return self.price_RDD.filter(lambda tokens: str(tokens[0]) == str(manageNum)).take(100)

    def __init__(self, sc):
        self.sc = sc
        data_path = os.path.join("data", "data.csv")
        raw_data = self.sc.textFile(data_path)
        raw_data_header = raw_data.take(1)[0]        

        headerList = str(raw_data_header).split(",") # 관리번호 = PK
        need_column = ['관리번호', '영업상태구분코드', '영업상태명', '소재지전화',\
                '소재지면적','소재지전체주소', '도로명전체주소', '사업장명', '최종수정시점',\
                '업태구분명', '좌표정보(X)', '좌표정보(Y)', '시설총규모', '홈페이지']
        need_column_index = []
        column_list = []
        for i, value in enumerate(headerList):
            if str(value) in need_column:
                need_column_index.append(i)
                column_list.append(str(value))

        data = raw_data.filter(lambda line: line!=raw_data_header) \
            .map(lambda line: line.split(",")) \
            .filter(lambda tokens: tokens[24] == '커피숍' and tokens[7] =='영업/정상') \
            .map(lambda tokens: [tokens[i] for i in need_column_index]) \
        
        # data save
        self.cafe_RDD = data

        price_data_path = os.path.join("data", "price.csv")
        price_data = self.sc.textFile(price_data_path)
        raw_data_header = price_data.take(1)[0]

        data = price_data.filter(lambda line: line != raw_data_header) \
            .map(lambda line: str(line).replace('"', "").replace("원", "").split(",")) \
            .map(lambda tokens: (tokens[0], tokens[1], tokens[2:]))

        self.price_RDD = data
        