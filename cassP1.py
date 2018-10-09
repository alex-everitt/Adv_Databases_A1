from cassandra.auth import PlainTextAuthProvider
#import config as cfg
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

def PrintTable(rows):
    t = PrettyTable(['user_id', 'count'])
    for r in rows:
        t.add_row([r.user_id, r.count])
    print(t)

def createKeySpace():
	session.execute('CREATE KEYSPACE IF NOT EXISTS a1 WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\':1};');
	print ("\nCreated Keyspace")
	
def createTable():
	session.execute('CREATE TABLE IF NOT EXISTS a1.reviews (partition int, user_id int, movie_id int, rating float, time_stamp timestamp, PRIMARY KEY(partition, movie_id, user_id))');#'CREATE TABLE IF NOT EXISTS a1.reviews (userID int PRIMARY KEY, user_name text, user_bcity text)'
	#session.execute('CREATE TABLE IF NOT EXISTS a1.review_test (user_id int, count counter, PRIMARY KEY( user_id))')

	#session.execute('CREATE TABLE IF NOT EXISTS a1.review_count (partition int, user_id int, count counter, PRIMARY KEY(partition, user_id)) WITH CLUSTERING ORDER BY (user_id DESC)')#(user_id int PRIMARY KEY, count counter) WITH CLUSTERING ORDER BY (count DESC)
	print ("\nCreated Table")

def insert():
########################## INSERT ###########################
        cwd = os.getcwd()
        file = open(cwd + "/dataset1/netIDs.data")
        count = 0
        for line in file:
                lineArr = line.split('\t')
                time_stamp = datetime.utcfromtimestamp(int(lineArr[3]))
                userID = int(lineArr[0])
		movieID = int(lineArr[1])
		rating = float(lineArr[2])
		partition = 1
                session.execute("INSERT INTO a1.reviews (partition, user_id, movie_id, rating, time_stamp) VALUES (%s, %s,%s, %s, %s)", (partition, userID, movieID, rating, time_stamp))
                #session.execute(insert_data, part, id )
                count = count + 1
                if (count % 500 == 0):
                        # session.execute(batch)
                        # batch = BatchStatement(cassandra.query.BatchType.UNLOGGED)
                        print(str(count) + "/60 000")
                # #Step 3: Insert business object directly into MongoDB via isnert_one
        print('Done')
	
def insert_1():
########################## INSERT ###########################
	cwd = os.getcwd()
	file = open(cwd + "/dataset1/netIDs.data")
	count = 0
	for line in file:
	
		lineArr = line.split('\t')
		#time_stamp = datetime.utcfromtimestamp(int(lineArr[3]))
		#print(int(lineArr[3]))
		#return()
		#batch.add(insert_data, (int(lineArr[0]), int(lineArr[1]), float(lineArr[2]), int(lineArr[3])))
		
		id = list()
		part = list()
		part.append(int(1))
		id.append(int(lineArr[0]))
		session.execute("UPDATE a1.review_count SET count = count + 1 WHERE partition = %s AND user_id=%s", (part[0], id[0]))
		#session.execute(insert_data, part, id )
		count = count + 1
		if (count % 500 == 0):
			print(str(count) + "/60 000")
		# #Step 3: Insert business object directly into MongoDB via isnert_one
	print('Done')
	
###################MAIN METHOD###########################
cluster = Cluster()
session = cluster.connect()
#createKeySpace()
#createTable()
#insert_1()

#insert()
#print ("\nSelecting All")
#rows = session.execute('SELECT user_id, count(*) FROM a1.reviews group by user_id;')#LIMIT 50
rows = session.execute('SELECT movie_id, avg(rating) FROM a1.reviews GROUP BY partition, movie_id;')
#rows = session.execute('SELECT * FROM a1.review_count GROUP BY partition, user_id;')# LIMIT 50

for r in rows:
	print(r)
#	count = count+1
# print(count)			
#PrintTable(rows)

cluster.shutdown()

