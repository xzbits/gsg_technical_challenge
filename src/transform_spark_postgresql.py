from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from config_parser import config_parser


def create_spark_session(jdbc_cfg):
    """
    Create Spark session

    * Note: Setup postgresql jar for Spark session for using Spark to interact with Postgres DB
    :param jdbc_cfg: JDBC jar path
    :return: SparkSession object
    """
    spark = SparkSession.builder\
        .appName("Create bikes tables with latest records")\
        .config('spark.jars', jdbc_cfg['jdbc_jar'])\
        .getOrCreate()
    return spark


def load_df_to_db(spark_df, table_name, db_config):
    """
    Load data from spark df to postgres db

    :param spark_df: Table dataframe
    :param table_name: Table name in DB
    :param db_config: Database configuration
    :return: None
    """
    # Insert job offer data into postgres DB
    url = 'jdbc:postgresql://{}:5432/{}?user={}&password={}'.format(db_config['host'],
                                                                    db_config['dbname'],
                                                                    db_config['user'],
                                                                    db_config['password'])
    properties = {"driver": "org.postgresql.Driver"}

    spark_df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)


def transformation(spark_obj):
    # Load data to spark DF
    hadoop_cfg = config_parser('prj-config.cfg', 'hadoop_config')
    bikes_data_url = hadoop_cfg['host'] + ':' + hadoop_cfg['port'] + "/datalake/bikes"
    bikes_df = spark_obj.read.parquet(bikes_data_url)

    # Get the latest records
    window = Window.partitionBy("id").orderBy(col("registration_updated_at").desc())
    latest_record = bikes_df.select('id',
                                    'registration_updated_at',
                                    'api_url',
                                    'stolen_location',
                                    'manufacturer_name',
                                    'frame_model',
                                    'status',
                                    'stolen',
                                    'title',
                                    'type_of_cycle')\
        .withColumn('latest_rank', row_number().over(window))\
        .filter('latest_rank == 1')\
        .drop('latest_rank')

    # Write to Hive table
    gsg_db_cfg = config_parser('prj-config.cfg', 'postgresql_gsg')
    load_df_to_db(latest_record, 'bikes', gsg_db_cfg)


if __name__ == "__main__":
    jdbc_config = config_parser('prj-config.cfg', 'jdbc_jar')
    sc = create_spark_session(jdbc_config)
    transformation(sc)
