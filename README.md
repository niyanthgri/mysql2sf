# mysql to snowflake

This program migrates a table from MySQL to Snowflake.
Snowflake is a cloud data warehouse. 

### **Prerequisites to run the program**
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


### **The command to run the program:** 
```$ python -W ignore mysql2snowflake.py```
> The "-W ignore" is to ignore the warnings that pop up when executing the program.

### What you see when you run the code:
The program will ask you to enter the MySQL database, followed by the Snowflake database, followed by the MySQL table.

1. If you enter a database that does not exist: this is what you should see when you run the code.

![Screenshot 2023-07-24 at 12 30 35 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/1cd6e6ed-5808-4f71-9130-5353acc9e37b)

2. If you enter a table that does not exist: this is what you should see when you run the code.

![Screenshot 2023-07-24 at 12 52 03 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/38a3e7eb-c64c-4343-9210-9978f74c01ab)

3. If you enter a MySQL table and databases that exist, but the snowflake table does not exist: this is what will occur.

![Screenshot 2023-07-24 at 1 08 38 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/0c250071-c5ad-48b4-95d6-5299ec67b9b8)

   Here, since the snowfalke table did not exist, a table was dynamicaly created. It then pushes the converted CSV file of the MySQL table \
   to the stage. It then pushes the file from stage to table. Thus the migration is successful.

4.  If you enter a MySQL table and databases that exist, and the snowflake table does exist: this is what will occur.

![Screenshot 2023-07-24 at 2 00 38 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/5838b227-8cd0-4bef-9d85-91400f6c7b8b)

   Here, since the snowflake table already exists, the user now has the option to append to the table or truncate and \
   replace all the rows in the table with the CSV file.

4.1.  If you append: It appends the file to the table and then validates if the table has been appended correctly or not.

![Screenshot 2023-07-24 at 2 07 45 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/2111a833-68db-48b6-b1ff-b99dce8cc3dd)

4.2.  If you replace: It truncates the file and then appends the file to the table and then validates if the table has been appended correctly or not.

![Screenshot 2023-07-24 at 2 09 27 PM](https://github.com/niyanthgri/mysql2sf/assets/140157007/1bdbeaf2-9bd8-4101-b55c-a8e8c79e471e)


These are the many options available when running this program.

I hope it's been useful to you, thanks for reading.

                

    
  
      





