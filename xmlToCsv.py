import xml.etree.cElementTree as ET
from openpyxl import Workbook
import csv
import json
import os

class xmlToCsv:
    filename = ""
    result = ""

    def __init__(self):
        super().__init__()
        self.filename = "data/fulldata_07_24_05_P_휴게음식점.xml"
        self.result = self.readFile(self.filename)
        

    def readFile(self, filename):
        if not os.path.exists(filename): return
        tree = ET.parse(filename)
        root = tree.getroot()
        dict_text, dict_keys = [], []
        columns = root.find('header').find('columns')
        for item in columns:
            dict_keys.append(str(item.tag))
            dict_text.append(str(item.text))
        mdlist = []
        mdlist.append(dict_text)
        for child in root.find('body').find('rows').findall('row'):
            temp = []
            for key in dict_keys:
                temp.append(str(child.find(key).text))
            mdlist.append(temp)
        return mdlist

    def to_CSV(self, mdlist):
        newfilename = os.path.abspath("data/data.csv")
        fh = open(newfilename, "w", newline="", encoding='utf-8')
        writer = csv.writer(fh)
        for row in mdlist:
            writer.writerow(row)
        fh.close()
        return "complete"

    def getCsv(self):
        if self.result:
            return self.to_CSV(self.result)

if __name__ == "__main__":
    a = xmlToCsv()
    a.to_CSV(a.result)
