from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import * 
import os
from time import sleep
from datetime import datetime

def createKeySpace():
	session.execute('CREATE KEYSPACE IF NOT EXISTS a1 WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\':1};');#simplestrategy and replication_factor=1 because not distributing this setup
	print ("\nCreated Keyspace")
	
def createTable_1():
	#create table for query 1
	#session.execute('CREATE TABLE IF NOT EXISTS a1.review_1 (user_id int, count counter, PRIMARY KEY( user_id))')# use this table to demonstrate issue with sorting results across partitions
	session.execute('CREATE TABLE IF NOT EXISTS a1.review_1 (partition int, user_id int, count counter, PRIMARY KEY(partition, user_id)) WITH CLUSTERING ORDER BY (user_id DESC)')#(user_id int PRIMARY KEY, count counter) WITH CLUSTERING ORDER BY (count DESC)
	print ("\nCreated Table")

def createTable_2():
	#create table for query 2
	session.execute('CREATE TABLE IF NOT EXISTS a1.reviews (partition int, user_id int, movie_id int, rating float, time_stamp timestamp, PRIMARY KEY(partition, movie_id, user_id))');
	print ("\nCreated Table")

def insert_1():
########################## INSERT for query 1###########################
	cwd = os.getcwd()
	file = open(cwd + "/dataset1/netIDs.data")#read dataset
	count = 0#track # of records inserted
	#iterate over lines
	for line in file:
		lineArr = line.split('\t')		#split lines by delimeter
		part=1
		id=int(lineArr[0])
		session.execute("UPDATE a1.review_count SET count = count + 1 WHERE partition = %s AND user_id=%s", (part, id))
		count = count + 1
		
		if (count % 500 == 0):
			print(str(count) + "/60 000")#track insert progress
	print('Done')
	
def insert_2():
########################## INSERT for query 2###########################
        cwd = os.getcwd()
        file = open(cwd + "/dataset1/netIDs.data")#read dataset
        count = 0#track # of records inserted
        for line in file:
                lineArr = line.split('\t')#split lines by delimeter
                time_stamp = datetime.utcfromtimestamp(int(lineArr[3]))#convert timestamp to readable format
                userID = int(lineArr[0])
		movieID = int(lineArr[1])
		rating = float(lineArr[2])
		partition = 1
                session.execute("INSERT INTO a1.reviews (partition, user_id, movie_id, rating, time_stamp) VALUES (%s, %s,%s, %s, %s)", (partition, userID, movieID, rating, time_stamp))
                count = count + 1
                if (count % 500 == 0):
                        print(str(count) + "/60 000")#track insert progress

        print('Done')
	
###################MAIN METHOD###########################
cluster = Cluster()#create cluster mapped to local host
session = cluster.connect()#establish session with service

createKeySpace()#Create the keyspace
###################Q1###################
createTable_1()
insert_1()
query_1 = session.execute('SELECT * FROM a1.review_count GROUP BY partition, user_id;')
for r in query_1:
	print(r)
###################Q2############################
createTable_2()
insert_2()
query_2 = session.execute('SELECT movie_id, avg(rating) FROM a1.reviews GROUP BY partition, movie_id;')
for r in query_2:
	print(r)

cluster.shutdown()#end session