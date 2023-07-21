# mysql to snowflake

This program migrates a table from MySQL to Snowflake.
Snowflake is a cloud data warehouse. 

## **Prerequisites to run the program**
1. Database <dest_db> to be available on Snowflake
    - SQL command to create: ```CREATE DATABASE <dest_db>;```
    - To check if database is created: ```SHOW DATABASES;```
      
2. Schema <schema_name> to be available on Snowflake
    - SQL command to create: ```CREATE SCHEMA <schema_name>;```
    - To check if schema is created: ```SHOW SCHEMAS;```
      
3. Stage <stage_name> to be available on Snowflake
    - SQL command to create: ```CREATE STAGE <stage_name>;```
    - To check is stage is created: ```SHOW STAGES;```
  
4. Libraries required are given in requirements.txt
    - To install libraries using pip: 
      ```$ pip install -r requirements.txt```


## **The command to run the program:** 
```$ python -W ignore mysql2snowflake.py```
> The "-W ignore" is to ignore the warnings that pop up when executing the program.





