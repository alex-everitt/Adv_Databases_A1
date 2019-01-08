# NoSQL Query Demos   
The objective of this project is to practice executing various queries on various NoSQL databases

# Databases Under Test

**MongoDB:** A highly flexible document based NoSQL database that can be distributed in replicated and sharded configurations   

**Cassandra:** A wide column storage based NoSQL database designed to handle big data with high availablity   

**Neo4j:**  A native graph database management system offering high performance   

# Data Set
The full ratings data set is 100000 ratings by 943 users on 1682 items (movies)    
The downSample.py script will just create a smaller subset of the file ratings.dat with a randomly selected 60K ratings instead of 100K   

ratings.dat   
------------------------------   
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of 
	         user id | item id | rating | timestamp. 
              The time stamps are unix seconds since 1/1/1970 UTC   


movies.dat   
------------------------------   
Information about the items (movies); this is a tab separated
              list of
              movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
              The last 19 fields are the genres, a 1 indicates the movie
              is of that genre, a 0 indicates it is not; movies can be in
              several genres at once.
              The movie ids are the ones used in the ratings.dat data set as "item id".

# Queries
1.	Find the total number of reviews by each user_id. (netIDs.dat is only needed)

2.  Find top-10 movies (only item_ids) with highest rating average. (netIDs.dat is only needed)

3.  Find the top-10 movie titles of all movies with at least one review rating less than 3. (netIDs.dat and movies.dat are both needed).

# Files

**query_results** folder contains results of the queries ran on each database using the corresponding Python script
**dataset** folder contains the data set used during development
**mongo_queries.py** is a python script handling data insertion and the execution of each query on MongoDB
**cassandra _queries.py** is a python script handling data insertion and the execution of each query on Cassandra
**neo4j_queries.py** is a python script handling data insertion and the execution of each query on Neo4j

# Instructions

Mongo:
1. install mongo service using yum
2. install pip using yum
3. install pymongo using pip
4. run "python mongo_queries.py"

------------------------------

Cassandra:
1. install cassandra service using yum
2. install pip using yum
3. install cassandra-driver using pip
4. run "python cassandra_queries.py"

------------------------------

Neo4j:
#1 Download Neo4j 
#2 Move to neo4j directory, run '/bin/neo4j console' command to start local instance of Neo4j
#3 Go to bolt://localhost:7687 to connect to local Neo4j instance
#4 Follow instructions to change username/password
#5 Connect to local instance using: driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4jpass"))
#6 Import data to database using import_movies(), then import_ratings() functions in that order
