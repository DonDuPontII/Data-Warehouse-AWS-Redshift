## Project Documentation

1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
    - The relational database *sparktifydb* is constructed where Sparkify data can be easily queried and is curated to what song users are listening to.


2. State and justify your database schema design and ETL pipeline.
    - Utilizing a star schema, a single fact table *songplays* focuses on data associated with song plays aligning to the primary analysis of interest. 
    - Four dimension tables contain Sparktify characteristics that can join to the *songplays* fact table to uncover insights. 
    - The dimensions tables included: 
        - *Users* (users in the app) 
        - *Songs* (songs reflected in the database) 
        - *Artists* (artists reflected in the database) 
        - *Time* (date and time units reflecting when songs were played) 
    - Due to the rudimentary dimensions/characteristics, there's no need to build a snowflake schema, which would involve a multidimensional layout. 
        - A use case for a snowflake schema: adding a user's most listened-to artist dimension table that joins to the user dimension table.
    - Utilizing fact and dimension tables work together to form organized data models that are intuitive to query.



### Files contianed in the repository
1. Sql_queries.py
    - Create and insert into table statements (not executed) for the database layout
    - Copy statements (not executed) that loads staging tables from S3 into Redshift database
2. Create_tables.py
    - Creates database *sparktifydb*
    - Executes drop and create table statments
3. ETL.ipynb
    - Reads in Sparktify files
    - Extracts column headers and a subset of data for each table
    - Inserts content into tables within the database
4. ETL.py
    - Same as etl.ipynb, but handles all data not just a subset
5. Test.ipynb
    - A space to verify the content in the table displays as intended

### Order to run python scrips
- After creating a Redshift cluster that aligns with the provided dwh.cfg parameters, one can execute:
    1. Create_tables.py
    2. ETL.py
