from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, count
import os
from config_parser import config_parser
import argparse


def create_spark_session():
    """
    Create Spark session

    :return: Spark session object
    """
    spark = SparkSession.builder\
        .appName("GSG case study")\
        .getOrCreate()

    return spark


def hdfs_path_exists(sc, hdfs_path):
    """
    Check HDFS path exists
    :param sc: Spark session
    :param hdfs_path: HDFS path
    :return:
    """
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    hadoop_cfg = config_parser('prj-config.cfg', 'hadoop_config')

    url = hadoop_cfg['host'] + ':' + hadoop_cfg['port'] + hdfs_path
    uri = jvm.java.net.URI(url)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

    return fs.exists(jvm.org.apache.hadoop.fs.Path(url))


def ingest_bikes_data(spark_obj, input_path, execution_date):
    """
    Ingesting bike data from bike_data directory

    :param spark_obj: Spark session object
    :param input_path: Path to bikes data
    :param execution_date: Execution date [day, month, year]
    :return: None
    """
    # Get ingestion date
    day = execution_date[2]
    month = execution_date[1]
    year = execution_date[0]

    # Get Hadoop config
    hadoop_cfg = config_parser('prj-config.cfg', 'hadoop_config')

    # Read bikes data json files
    bikes_data_df = spark_obj.read.json(os.path.join(input_path, 'bikes_op_page_*.json'))
    bikes_data_df = bikes_data_df.withColumn("revision_number", lit(None))\
        .withColumn("ingest_year", lit(year))\
        .withColumn("ingest_month", lit(month))\
        .withColumn("ingest_day", lit(day))

    # Append latest record id and update revised data in data lake
    bikes_table_location = hadoop_cfg['host'] + ':' + hadoop_cfg['port'] + "/datalake/bikes"

    if hdfs_path_exists(spark_obj, r'/datalake/bikes'):
        current_df = spark_obj.read.parquet(bikes_table_location)

        # Combine current and new data, then drop duplicate rows
        combine_data = current_df.union(bikes_data_df)\
            .dropDuplicates(['id', 'registration_updated_at'])\
            .drop('revision_number').drop('ingest_year').drop('ingest_month').drop('ingest_day')

        # Grouping id and add revision number column based on registration_updated_at column
        window = Window.partitionBy("id").orderBy(col("registration_updated_at").asc())
        revision_data = combine_data.withColumn('revision_number', row_number().over(window))

        # Combine again
        revision_data = revision_data.withColumn("ingest_year", lit(year))\
            .withColumn("ingest_month", lit(month))\
            .withColumn("ingest_day", lit(day))
        combine_data_2 = current_df.union(revision_data)

        # Eliminate all records which are duplicated
        windowDel = Window.partitionBy(["id", "registration_updated_at"])
        daily_bikes_df = combine_data_2.withColumn('cnt', count("id").over(windowDel))\
            .where('cnt == 1')\
            .drop('cnt')
    else:
        # Grouping id and add revision number column based on registration_updated_at column
        window = Window.partitionBy("id").orderBy(col("registration_updated_at").asc())
        daily_bikes_df = bikes_data_df.withColumn('revision_number', row_number().over(window))\
            .dropDuplicates(['id', 'registration_updated_at'])

    # Save to data lake
    daily_bikes_df.write\
        .mode('append')\
        .partitionBy("ingest_year", "ingest_month", "ingest_day")\
        .parquet(bikes_table_location)


if __name__ == "__main__":
    # Create interface to handle the command line arguments
    arg_parser = argparse.ArgumentParser(description="Ingest data")
    arg_parser.add_argument('-d', '--execution_day',
                            required=True,
                            type=str,
                            help='Executed date. Format yyyy-mm-dd')
    arg_parser.add_argument('-p', '--file_path',
                            required=True,
                            type=str,
                            help='Path point to JSON files')
    args = arg_parser.parse_args()
    execution_day = list(map(int, args.execution_day.split("-")))
    bikes_data_path = args.file_path

    spark_session = create_spark_session()
    ingest_bikes_data(spark_session, bikes_data_path, execution_day)
