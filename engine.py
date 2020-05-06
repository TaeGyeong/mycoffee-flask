import os
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class dataEngine:
    
    def get_all_data(self):
        return self.cafe_RDD
    
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
            .map(lambda tokens: [tokens[i] for i in need_column_index])        
        
        # data save
        self.cafe_RDD = data

        