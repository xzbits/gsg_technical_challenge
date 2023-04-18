# Project summary
## Introduction
The outlined project is a data engineering pipeline that involves collecting and processing data from the Bike Index API V3. The pipeline comprises several components that work together to extract, transform, and load data into either Hive or Postgresql.

The project starts with a configuration file, `prj-config.cfg`, which contains settings for JDBC, Hadoop, Postgresql DB, AWS, and the Bike Index API V3. The `collect_data.py` script is responsible for collecting data from the API and temporarily storing it in JSON files.

The next component, `ingest_spark.py`, processes the JSON files using Spark and stores them in the Hadoop Distributed File System (HDFS) in the parquet file format. The data is then transformed using either the `transform_spark_hive.py` or `transform_spark_postgresql.py` script, depending on the target destination.

The final component, `create_postgresql_db.py`, creates the GSG database and tables in Postgresql, which serves as the target for the transformed data.

Overall, this project is an efficient and automated data engineering pipeline that enables the collection, processing, and storage of large volumes of data from an external API.


## Data architecture
![img.png](data_achitecture.png)

# Explanation for storing new daily bikes data
![img.png](table_transform_1.png)
![img.png](table_transform_2.png)
![img.png](table_transform_3.png)
![img.png](table_transform_4.png)

# Sample run
## Step 1: Start Hadoop

## Step 2: Clarify HDFS path, GSG Database, JAR file path for JDBC driver configuration
```buildoutcfg
[bike_url]
bikes = https://bikeindex.org:443/api/v3/search
op_bike = https://bikeindex.org:443/api/v3/bikes/{}

[hadoop_config]
host = 
port = 

[postgresql_default]
host = 
dbname = 
user = 
password = 

[postgresql_gsg]
host = 
dbname = gsg
user = 
password = 

[jdbc_jar]
jdbc_jar = 
```

## Step 3: Collect bike data from `https://bikeindex.org/`
```commandline
python collect_data.py
```

## Step 4: Ingest data
Please download the database which collects from Step 3 in 
`https://drive.google.com/file/d/1ooGfZVzReCDDIlLMcyeosXoO3vkSuCo3/view?usp=sharing`

For sample, I prepared 3 folders to simulate daily data ingestion `~/database/2022-2-27`, `~/database/2022-2-28`, and
`~/database/2022-3-1`

```commandline
python ingest_spark.py -d 2023-2-27 -p <DIR_PATH>\database\2022-02-27
python ingest_spark.py -d 2023-2-28 -p <DIR_PATH>\database\2022-02-28
python ingest_spark.py -d 2023-3-1 -p <DIR_PATH>\database\2022-03-01
```
As result of above command lines, it will reflect in Hadoop HDFS 
![img.png](hadoop_hdfs_1.png)
![img.png](hadoop_hdfs_2.png)
![img.png](hadoop_hdfs_3.png)

## Step 5: Transform data
Create GSG database and bikes table
```commandline
python create_postgresql_db.py
```

Transform and load latest bike data table
```commandline
python transform_spark_postgresql.py
```
As result of above command lines, it will reflect in gsg postgres DB
![img.png](postgres_db.png)
