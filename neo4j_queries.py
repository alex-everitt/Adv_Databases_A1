from neo4j.v1 import GraphDatabase
import os

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4jpass"))

## movies.dat must be in neo4j/import folder!
## Did this manually
def import_movies(tx):
    tx.run("LOAD CSV FROM 'file:///movies.dat' AS movie " # Load data as variable
           "FIELDTERMINATOR  \'|\'" # Delimit the data using "|"
           # If node does not exist, create node with label "Movie" with movie ID and title properties
           "MERGE (:Movie { movieID: toInteger(movie[0]), movieTitle: movie[1]})") 

## netIDs.dat must be in neo4j/import folder!
## Did this manually
def import_ratings(tx):
    tx.run("LOAD CSV FROM 'file:///netIDs.dat' AS ratings " # Load data as variable
           "FIELDTERMINATOR \'\\t\' " # Delimit data using tabs
           # Find node of label type Movie that contains item ID from ratings
           "MATCH (m:Movie) " 
           "WHERE m.movieID = toInteger(ratings[1]) " 
           # If node does not exist, create node with label "User" with userID properties
           "MERGE (user:User {userID: toInteger(ratings[0])}) "
           # If relationship does not exist, create relationship of type RATED that contains timestamp and rating to Movie
           "MERGE (user)-[r:RATED {rating: toFloat(ratings[2]), timestamp: toInteger(ratings[3])}]->(m)")

def query1(tx):
    tx.run("MATCH (u:User)-[r:RATED]->(m:Movie) " # Query all RATED relationships
            # Take Users queried and count the total number of movies they've rated
           "WITH u, count(*) as moviesRated "
           # Return User ID and the number of movies they've rated in ascending order
           "RETURN u.userID as UserID, moviesRated "
           "ORDER BY UserID")

def query2(tx):
    tx.run("MATCH () -[r:RATED]->(m:Movie) " # Query all RATED relationships
            # Using each movie, find the average rating by aggregating each RATED relationship
           "WITH m, avg(r.rating) AS avgRating " 
           "RETURN m.movieID, avgRating "
           "ORDER BY avgRating DESC "
           "LIMIT 10")

def query3(tx):
    for record in tx.run("MATCH () -[r:RATED]->(m:Movie) " 
                        # Query all RATED relationships where the rating is less than 3
                         "WHERE r.rating < 3 "
                         # Grab the titles of the movies that are part of those relationships
                         "WITH DISTINCT m.movieTitle AS title "
                         # Query all RATED relationships that match the titles above
                         "MATCH ()-[r:RATED]->(m:Movie) "
                         "WHERE m.movieTitle = title "
                         # Retrun the title and average rating of these relationships
                         "RETURN m.movieTitle as Title, avg(r.rating) as AverageRating "
                         "ORDER BY AverageRating DESC LIMIT 10"):
        print(record["m.movieTitle"])


with driver.session() as session:
    session.write_transaction(import_movies)
    session.write_transaction(import_ratings)
    #session.write_transaction(query1)
    #session.write_transaction(query2)
    #session.write_transaction(query3)