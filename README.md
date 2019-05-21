Project Overview:
>Create a star schema that optimizes searching for song play transactions.

Project Repository Files:
>1. sql_queries.py contains all your sql queries into the notebooks 
>2. test.ipynb displays the first few rows of each table to let you check your database
>3. create_tables.py drops and creates your tables
>4. etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables
>5. etl.py reads and processes files from song_data and log_data and loads them into your tables

Database Design:
>The Database was broken up into 4 dimensional tables and 1 fact table
>4 Dimensional tables: Users, Time, Songs, and Artist
>1 Fact table consisted data from all dimensional tables

ETL Process:
>1. Extract log and song data from json to dataframes
>2. Transform into its respective dataframe as per database design
>3. Load into Postgres Database

How to run the program:
>1. Use the jupyter notebooks to learn how the etl.py script works
>2. Open a terminal and run the etl.py script


