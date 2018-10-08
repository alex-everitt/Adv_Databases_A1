The full ratings data set is 100000 ratings by 943 users on 1682 items (movies). 
You must down sample the ratings.dat file first using the downSample.py. The downSample.py script will just create 
a smaller subset of the file ratings.dat with a randomly selected 60K ratings instead of 100K. 
This means that each student group will ends up working on different data from this dataset.

ratings.dat  
------------------------------
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of 
	         user id | item id | rating | timestamp. 
              The time stamps are unix seconds since 1/1/1970 UTC   


movies.dat   
------------------------------
-- Information about the items (movies); this is a tab separated
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