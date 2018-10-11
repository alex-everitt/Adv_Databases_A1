from pymongo import MongoClient
import pymongo
from prettytable import PrettyTable
from time import sleep
from bson.son import SON
import os
from datetime import datetime

def PrintTable(rows, query_num):
	if(query_num == 1):
		t = PrettyTable(['User ID','# of Reviews'])
		for r in rows:
			t.add_row([r['_id'], r['Total_Reviews']])
	elif(query_num == 2):
		t = PrettyTable(['Movie ID', 'Avg Rating'])
		for r in rows:
			t.add_row([r['_id'], r['avg']])
	elif(query_num == 3):
		t = PrettyTable(['Movie ID', 'Title', 'Avg Rating'])
		for r in rows:
			t.add_row([r['_id'], r['movie'][0]['title'], r['avg']])
	#print(rows.__dict__.keys())
	#count=0
	#for r in rows:
		#count = count + r['Total_Reviews']
		#t.add_row([r['_id'], r['movie'][0]['title'],r['rated'][0]['avg']])
		#t.add_row([r['_id'], r['avg']])
	print(t)

def insert_reviews():
########################## INSERT ###########################
	cwd = os.getcwd()
	file = open(cwd + "/dataset1/netIDs.data")
	count=0
	for line in file:
		lineArr = line.split('\t')
		time = datetime.utcfromtimestamp(int(lineArr[3])).strftime('%Y-%m-%d %H:%M:%S')
		review = {
			'userID' : int(lineArr[0]),
			'itemID' : int(lineArr[1]),
			'rating' : float(lineArr[2]),
			'timeStamp' : time
		}
		#Step 3: Insert business object directly into MongoDB via isnert_one
		result=db.reviews_short.insert_one(review)
		count = count + 1
		if (count%500 == 0): print(str(count) + "/60 000")
	print('Done')

def insert_movies():
########################## INSERT ###########################
	cwd = os.getcwd()
	file = open(cwd + "/dataset1/movies.dat")
	count=0
	for line in file:
		lineArr = line.split('|')
		genres=[]
		
		if(lineArr[5]=='1'):genres.append("unknown") 
		if(lineArr[6]=='1'):genres.append("Action") 
		if(lineArr[7]=='1'):genres.append("Adventure") 
		if(lineArr[8]=='1'):genres.append("Animation") 
		if(lineArr[9]=='1'):genres.append("Children's") 
		if(lineArr[10]=='1'):genres.append("Comedy") 
		if(lineArr[11]=='1'):genres.append("Crime") 
		if(lineArr[12]=='1'):genres.append("Documentary") 
		if(lineArr[13]=='1'):genres.append("Drama") 
		if(lineArr[14]=='1'):genres.append("Fantasy") 
		if(lineArr[15]=='1'):genres.append("Film-Noir") 
		if(lineArr[16]=='1'):genres.append("Horror") 
		if(lineArr[17]=='1'):genres.append("Musical") 
		if(lineArr[18]=='1'):genres.append("Mystery") 
		if(lineArr[19]=='1'):genres.append("Romance ") 
		if(lineArr[20]=='1'):genres.append("Sci-Fi") 
		if(lineArr[21]=='1'):genres.append("Thriller") 
		if(lineArr[22]=='1'):genres.append("War") 
		if(lineArr[23]=='1'):genres.append("Western")
		
		movie = {
			'_id' : int(lineArr[0]),
			'title' : lineArr[1],
			'release_date' : lineArr[2],
			'url' : lineArr[4],
			'genre' : genres
		}
		#Step 3: Insert business object directly into MongoDB via isnert_one
		result=db.movies.insert_one(movie)
		count = count + 1
		if (count % 500 ==0) : print(str(count) + "/60 000")
	print('Done')
	
#uri = "<connection_string>"
#client = MongoClient(uri)
client = MongoClient('localhost', 27017)
db = client["A1"]

col = db["reviews"]

serverStatusResult=db.command("serverStatus")
#insert_reviews()
#insert_movies()

#uncomment below as needed

########################### Find total reviews by each user ID ###########################
#query_1 = db.reviews.aggregate([{"$group" : {"_id" : "$userID", "Total_Reviews" : {"$sum": 1}}}, {"$sort" : { "_id" : 1}}])#iterate over reviews collection and determine number of reviews by each distinct userID
#PrintTable(query_1,1)

###########################Find top 10 movies with highest average rating#################
#query_2 = db.reviews.aggregate([{"$group" : {"_id" : "$itemID", "avg" : {"$avg": "$rating"}}},{"$sort" : {"avg" : -1}}, {"$limit" : 10}])#iterate over reviews collection and determine average rating for each distinct movieID. Sort results and output only top 10 rated movies
#PrintTable(query_2,2)

###########################Find top 10 avg rated movies with at least 1 rating < 3 #################
query_3 = db.reviews_short.aggregate([{"$match": { "rating" : {"$lt":3}}}, {"$group" : {"_id" : "$itemID"}}, {"$project" : {"_id" : 1} }, {"$lookup": {"from": "reviews_short", "localField": "_id", "foreignField": "itemID", "as": "matching_reviews"}}, {"$unwind" : "$matching_reviews"}, {"$group" : {"_id" : "$_id" , "avg" : {"$avg" : "$matching_reviews.rating"}}}, {"$lookup": {"from": "movies", "localField": "_id", "foreignField": "_id", "as":"movie" }}, {"$project": {"_id":1, "avg":1, "movie.title":1}},{"$sort": {"avg":-1}} , {"$limit" : 10}])#find all distint movieID with at least one rating < 3. Left outer join this collection with the reviews set in order to have all relevent reviews that next need to be averaged for each movieID. Lastly, join the set of averaged movies with the movie set in order to determine the title. Sort results and only output top 10 highest average ratings

PrintTable(query_3,3)






