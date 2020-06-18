import os, csv
from time import time
from pyspark.mllib.recommendation import ALS
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 위경도 거리계산용.
from haversine import haversine
from difflib import SequenceMatcher

def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class dataEngine:

    ########################################################################################################################

    def get_select_data(self, data):
        return self.cafe_RDD \
            .filter(lambda tokens: str(tokens[0]) in data) \
            .take(100000)

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

    ########################################################################################################################

    def __predict_ratings(self, user_and_cafe_RDD):
        predicted_RDD = self.model.predictAll(user_and_cafe_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.ratings_RDD).join(self.cafe_ratings_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def make_new_ratings_info(self, user_id):
        # temp test
        data_path = os.path.join("data", "user_rating.csv")
        user_rating_raw_data = self.sc.textFile(data_path)
        user_rating_raw_data_header = user_rating_raw_data.take(1)[0]

        # 경우의수: 첫번째로 들어온 id 일경우 / 이미 이전에 등록된id / 처음이지만 다른 id들이 등록된 경우.
        user_rating_RDD = user_rating_raw_data.filter(lambda line: line!=user_rating_raw_data_header)\
            .map(lambda line: line.split(","))\
            .map(lambda tokens: (tokens[0], tokens[1]))
        index = 1000
        user_mapping = user_rating_RDD.filter(lambda line: line[0] == user_id).take(1)
        if len(user_rating_RDD.take(1)) == 0: # 유저좋아요정보가 없는 경우.
            file = open(data_path, "a", newline="", encoding="utf-8")
            writer = csv.writer(file)
            row = [user_id, index]
            writer.writerow(row)
            file.close()
        elif len(user_mapping) == 1: # 이미 mapping 이 되어있다면?
            index = user_mapping[0][1]
        else: # 유저정보는 있지만 mapping 되지 않은 유저인 경우
            temp_RDD = user_rating_RDD.take(10000)
            index = int(temp_RDD[-1][1]) + 1
            file = open(data_path, "a", newline="", encoding="utf-8")
            writer = csv.writer(file)
            row = [user_id, index]
            writer.writerow(row)
            file.close()
        
        return index

    def get_top_ratings(self, id, ratings):
        index = self.make_new_ratings_info(id)
        new_user_ratings = []
        for i in range(len(ratings)):
            new_user_ratings.append((index, str(ratings[i]).replace("-", ""), 10))

        new_user_ratings_RDD = self.sc.parallelize(new_user_ratings).map(lambda r: (r[1], (r[0], r[2]))).join(self.item_int).map(lambda r: (r[1][0][0], r[1][1], r[1][0][1]))

        self.ratings_RDD = self.ratings_RDD.union(new_user_ratings_RDD)
        self.__train_model()

        new_user_ratings_ids = map(lambda x: x[1], new_user_ratings) 
        new_user_unrated_cafe_RDD = (self.cafe_id_with_ratings_RDD.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (index, x[0])))

        # Use the input RDD, new_user_unrated_movies_RDD, with new_ratings_model.predictAll() to predict new ratings for the movies
        new_user_recommendations_rating_title_and_count_RDD = \
            self.model.predictAll(new_user_unrated_cafe_RDD).map(lambda x: (x.product, x.rating)).join(self.ratings_RDD).join(self.cafe_ratings_counts_RDD).map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        top_cafe = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

        ratings = self.sc.parallelize(ratings).join(self.int_item) \
            .map(lambda tokens: tokens[1][1]).take(25)
        
        result = self.sc.parallelize(top_cafe).join(self.int_item).map(lambda tokens: (tokens[1][1])).take(25)
        return self.cafe_RDD.filter(lambda line: str(line[0]).replace("-", "") in result).take(25)
        

    def add_ratings(self, new_ratings_RDD):
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        self._count_and_average_ratings()
        self.__train_model()

        return True
            
    def _count_and_average_ratings(self):
        logger.info("counting cafe rating data...")
        self.cafe_id_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        cafe_id_with_avg_ratings_RDD = self.cafe_id_with_ratings_RDD.map(get_counts_and_averages)
        self.cafe_ratings_counts_RDD = cafe_id_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        logger.info("Training the ALS model ... ")
        t0 = time()
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed, iterations=self.iterations, lambda_=self.regularization_parameter)
        tt = time() - t0
        logger.info("ALS model built complete")
        logger.info("New model trained in %s seconds" % round(tt, 3))

    ########################################################################################################################

    def __init__(self, sc):
        # logger start
        logger.info("Starting up the Recommendation Engine:" )

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
            .filter(lambda tokens: tokens[5].split()[0] == '경기도' or tokens[5].split()[0] == '충청북도')

        # data save
        self.cafe_RDD = data

        price_data_path = os.path.join("data", "price.csv")
        price_data = self.sc.textFile(price_data_path)
        raw_data_header = price_data.take(1)[0]

        data = price_data.filter(lambda line: line != raw_data_header) \
            .map(lambda line: str(line).replace('"', "").replace("원", "").split(",")) \
            .map(lambda tokens: (tokens[0], tokens[1], tokens[2:]))

        self.price_RDD = data
        
        ########################################################################################################################
                # Start Recommendation model creation #
        ########################################################################################################################

        # start recommendation engine create
        logger.info("Loading Ratings & cafe data...")

        self.sc.setCheckpointDir('checkpoint/')
        ALS.checkpointInterval = 2        
        
        cafe_data = self.cafe_RDD.map(lambda tokens: (tokens[0].replace("-", ""), tokens[7]))
        
        ratings_file_path = os.path.join("data", "rating.csv")
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header) \
            .map(lambda line: line.split(",")) \
            .map(lambda tokens: (tokens[1], tokens[0].replace("-", ""), tokens[2])).cache() \

        users = ratings_RDD.map(lambda r: r[0]).distinct()
        items = cafe_data.map(lambda r: r[0]).distinct()

        # Zips the RDDs and creates 'mirrored' RDDs to facilitate reverse mapping 
        self.user_int = users.zipWithIndex()
        self.int_user = self.user_int.map(lambda u: (u[1], u[0]))
        self.item_int = items.zipWithIndex()
        self.int_item = self.item_int.map(lambda i: (i[1], i[0]))

        print(self.user_int.take(3))
        print(self.item_int.take(3))

        ratings = ratings_RDD.map(lambda r: (r[1], (r[0], r[2])))\
            .join(self.item_int).map(lambda r: (r[1][0][0], r[1][1], r[1][0][1]))
        self.ratings_RDD = ratings.map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache() 

        # ratings count and average calculation.
        self._count_and_average_ratings()
       
        self.rank = 12
        self.seed = 10
        self.iterations = 8
        self.regularization_parameter = 0.1
        
        
        # training model.
        self.__train_model()
                