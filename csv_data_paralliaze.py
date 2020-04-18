from pyspark import SparkConf, SparkContext
import os
def get_sc():
    conf = SparkConf().setMaster("local[3]").setAppName("temp-test-dataset")
    sc = SparkContext.getOrCreate(conf=conf)
    return sc


if __name__ == "__main__":
    sc = get_sc()
    data = os.path.join('.', 'data', 'data.csv')
    raw_data = sc.textFile(data)
    raw_data_header = raw_data.take(1)[0]
    temp = str(raw_data_header).split(",")
    need_column = ['개방서비스명', '관리번호', '영업상태구분코드', '영업상태명', '소재지전화',\
        '소재지면적','소재지전체주소', '도로명전체주소', '사업장명', '최종수정시점', '업태구분명',\
        '좌표정보(X)', '좌표정보(Y)', '시설총규모', '홈페이지']
    need_column_index = []
    for i, value in enumerate(temp):
        if str(value) in need_column:
            need_column_index.append(i)
            print(value)
        print(i, value)
        
    if len(need_column) != len(need_column_index):
        print(len(need_column))
        print(len(need_column_index))
        print("exit!")
        exit()


    filt_data = raw_data.filter(lambda line: line!= raw_data_header)\
        .map(lambda line: line.split(","))\
        .map(lambda tokens: [tokens[i] for i in need_column_index])\
        .filter(lambda tokens: tokens[10] == '커피숍')
    
    print(need_column) 
    print(filt_data.take(3))