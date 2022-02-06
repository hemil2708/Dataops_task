# import pymongo
# import pymysql
# import pandas as pd
# import datetime
#
# conn = pymongo.MongoClient('localhost', 27017)
# # # # conn = pymongo.MongoClient('mongodb://harsh.r:Harsh#123@51.161.13.140:27017/?authSource=admin')
# db = conn['epv_gov_task']
#
# date_time = datetime.datetime.now().strftime('%d_%m_%Y %H:%M:%S')
# date_time = date_time.split(' ')[0]
#
# table1 = f"epvgov"
# collection = db[table1]
#
# cursor = collection.find({})
# # #
# df = pd.DataFrame(cursor)
# df.pop('_id')
#
#
# df.to_csv('Dataops.csv', encoding='utf-8', index=False)
# print("CSV Genrated")
#
#
#
#
