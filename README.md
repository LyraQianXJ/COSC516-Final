# COSC516-Final
Final exam of COSC516-database:implementation of Cassandra 
gamestate were uploaded twice as two seperate tables for query1 and query2.
I only left the load of the first table(test passed for the other two since they were identical) so that data won't be uploaded multiple times. If needed to test, perform "Trunctate tablename" before uploading to that table again.
The update was only to gamestate1, since Cassandra uploads same data multiple itmes and for gamestate2, one of the primary key(power) was required to be updated, it was not allowed in Cassandra. It has to be updated on the client side.
Since Cassandra does not support ORDER BY COUNT, I had to order the data maunually for query3.
