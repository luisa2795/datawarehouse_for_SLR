# datawarehouse_for_SLR
ETL to load _CauseMiner_ output files, transform them to Data Warehouse Tables and load them into the DB.

## How to run:
#### If you are initializing a new DB, you first have to create the Snowflake schema: 
To do so, copy the file _schema_creation.sql_ to your database tool (e.g. DBeaver) and execute it. It will create all tables in the schema _public_. (If _public_ was not existing before, please create it with ```create schema public```.)
#### Now you can run the ETL code:
1. Clone the repository to a folder of your choice on the university server _zeno_. it must be on _zeno_, as it accesses the source data from there and will also communicate with the Database, which is running on _zeno_.
2. Place a file called _credentials.py_ in the repository. It should contain the variable DB_CONNECTION_PARAMS to successfully connect to the target database of the Data Warehouse.
   ```
   DB_CONNECTION_PARAMS = {
    'username': '<your_username>',
    'password': '<your_password>',
    'host': '<IP_of_the_server>',
    'port': <your_db_port>,
    'database': '<your_db_name>'
   }
   ```
3. Install the packages defined in _requirements.txt_ in a fresh Python 3.9 environment. 
   ```
   python3.9 -m venv etlvenv
   source etlvenv/bin/activate
   pip install -r requirements.txt
   ```
4. All functions are called from _main.py_. To run a specific ETL pipeline for a certain dimension, run the _main.py_ file. A prompt will ask which step should be executed. You can type ```Keyword ETL```to execute the ETL pipeline for the keyword dimension, for example. 
  
   **Mind the dependencies:** As some keys are referenced from related tables as foreign keys, there are some dependencies regarding the execution order of the ETL steps. The picture below shows all rules you should consider to avoid errors:

     ![alt text](/pictures/etl_dependency_order.png)
     
    This means, that before executing ```Paper ETL```, you should have executed the ETL steps for keywords, authors, and jounals, as their private keys will be needed to completely transform the paper dimension.
5. Change Data Capture is realized via full diff compares. This means that when you have new source data, you can execute the ETL pipelines again and it will append the deltas to the Data Warehouse dimensions and fact tables.

## Where is the data:
- The source data is in the folder specified as ```sourcepath``` in _variables.py_ (Currently it is ```/home/muellerrol/causeminer2/reports/2021_12_06_153039_results``` on _zeno_.)
- The target database, in which the Data Warehouse has been initialized is a PostgreSQL database on _zeno_ with the name _luisa_. The credentials have to be added to a file called _credentials.py_ as described above.

## How does the logic work:
- A pipeline for one dimension starts with extracting the relevant CSV-files from the sourcepath into pandas DataFrames. Some dimensions, like the paper dimension, are extracted from multiple source files, in this case the files papers_final.csv and unique_references.csv. In these cases, the source data is transformed to a common format (common column names and datatypes) before it is merged.
- The data is cleaned by removing duplicates, merging similar rows that are likely regarding the same real-life entity and filling missing values with a default value. The default values are ‘MISSING’ for string attributes, ‘0’ for numeric attributes and ‘1678’ for missing year values. Each dimension gets a dummy row with the primary key ‘0’, so that any missing references from linked dimensions can be filled with the foreign key ‘0’ to point to this dummy entry. 
- After data preparation, any linked dimension is loaded to insert foreign keys. This means that for example the dim_paper transformation includes a repeated transformation of the keywords, authors, and journals as well, in order to join these tables in the end to get their foreign keys. The journal attributes in the paper table are then replaced by one foreign key to the respective row in the journal table. In the case of multivalued relationships, a group key is generated and stored in a separate bridge table and a group dimension. 
- Change data capture is done via full diff compares. This means that in the loading phase a delta between the rows in the transformed source data and the already existing rows in the DB tables is calculated. For most tables this is done by including all attributes in the comparison, except for dim_paragraph and dim_sentence. These two tables have their original source_id as an attribute, so for these two tables it is sufficient to compare only the source_id column.
- When the delta rows are known, they are equipped with a primary key, starting from the highest primary key already in the database + 1. Then the rows are appended to the DB table. In case of multivalued related dimensions, the new rows for the group and bridge tables must be written to the DB before loading the referencing dimension. This is achieved by executing the ETL functions only in the logical blocks defined in the __main__.py script.
