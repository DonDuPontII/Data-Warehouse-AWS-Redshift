## Process Documentation

1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
    - The relational database is constructed where Sparkify data can be easily queried from and is curated to what song users are listening to.


2. State and justify your database schema design and ETL pipeline.
    - Utilizing a star schema, there is a single fact table *songplays* focusing on data associated with song plays aligning to the primary analysis of interest. 
    - There are four dimension tables containing Sparktify characteristics that can easily be joined to the *songplays* fact table to uncover insights. 
    - The dimensions tables included: 
        - *users* (users in the app) 
        - *songs* (songs reflected in the database) 
        - *artists* (artists reflected in the database) 
        - *time* (date and time units of when songs were played) 
    - Due to the rudimentary dimensions/characteristics, there wasn’t a need to build out a snowflake schema, which would involve a multidimensional layout. 
        - For example, if data around a user’s most listened to artist began being collected, then building a secondary dimension off of the user dimension table would reflect the need for a snowflake schema. 
    - Utilizing fact and dimension tables work together to form organized data models that are intuitive to query off of.


### Files contianed in the repository
1. sql_queries.py
    - Create and insert into table statements (not executed) for the database layout
    - Copy statements (not executed) that loads staging tables from S3 into Redshift database
2. create_tables.py
    - Creates database *sparktifydb*
    - Executes drop and create table statments
3. etl.ipynb
    - Reads in Sparktify files
    - Extracts column headers and a subset of data for each table
    - Inserts content into tables within the database
4. etl.py
    - Same as etl.ipynb, but handles all data not just a subset
5. test.ipynb
    - A space to verify the content in the table displays as intended

### Order to run python scrips
- After creating a Redshift cluster that aligns with the provided dwh.cfg parameters, one can execute:
    1. create_tables.py
    2. etl.py


### Question for the reviewer
1. Since Redshift doesn't support the conflict constraints taught previously, is there any equivalent functionality available as opposed to massagin the data sets to fit the expeced parameters?
2. May I have some clarification around the purpose of the LOG_JSONPATH parameter in dwh.cfg? As a recommendation, I would include context in the instructions to avoid further confusion going forward. 
3. The main function in both create_tables.py and etl.py are very similar except for calling functions to their respective scripts. Is it best practice to keep the design this way or would it be better to consolidate once all pieces are functioning as expected?
