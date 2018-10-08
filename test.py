from pymongo import MongoClient
import pymongo
from prettytable import PrettyTable
from time import sleep
from bson.son import SON
import os
from datetime import datetime

client = MongoClient('localhost', 27017)
db = client["A1"]
serverStatusResult=db.command("serverStatus")

query_1_diff = []
query_2_diff = []
query_3_diff = []

output = db.rated_under_3.aggregate([{"$lookup":{"from": "reviews", "localField": "_id", "foreignField": "itemID", "as": "rated"}}, {"$group" : {"_id" : "$_id", "avg" : {"$avg": "$rated.rating"}}},{"$sort" : {"avg" : -1}}, {"$limit" : 10}])
print(list(output))
	
# script_start_time = datetime.now()

# for x in range(1, 100):
    # start_query1 = datetime.now()
    # db.reviews.aggregate([{"$group" : {"_id" : "$userID", "Total_Reviews" : {"$sum": 1}}}, {"$sort" : { "_id" : 1}}])
    # query_1_diff.append((datetime.now() - start_query1).total_seconds())

    # start_query2 = datetime.now()
    # avg_review = db.reviews.aggregate([{"$group" : {"_id" : "$itemID", "avg" : {"$avg": "$rating"}}},{"$sort" : {"avg" : -1}}, {"$limit" : 10}])
    # query_2_diff.append((datetime.now() - start_query2).total_seconds())

    # start_query3 = datetime.now()
    # db.reviews.aggregate([{"$match": { "rating" : {"$lt":3}}}, {"$group" : {"_id" : "$itemID"}}, { "$out": "rated_under_3" }])
	# db.rated_under_3.aggregate([{"$lookup":{"from": "reviews", "localField": "_id", "foreignField": "itemID", "as": "rated"}}, {"$group" : {"_id" : "$itemID", "avg" : {"$avg": "$rated.rating"}}},{"$sort" : {"avg" : -1}}, {"$limit" : 10}])
    # query_3_diff.append((datetime.now() - start_query3).total_seconds())

# total_script_time = (datetime.now() - script_start_time).total_seconds()

# avg_query1_time = sum(query_1_diff) / len(query_1_diff)
# avg_query2_time = sum(query_2_diff) / len(query_2_diff)
# avg_query3_time = sum(query_3_diff) / len(query_3_diff)

# print(avg_query1_time)
# print(avg_query2_time)
# print(avg_query3_time)
# print(total_script_time)
