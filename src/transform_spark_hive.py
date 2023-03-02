from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from config_parser import config_parser


def create_hive_spark_session():
    spark = SparkSession.builder\
        .appName("Latest bikes records")\
        .config("hive.metastore.uris", "thrift://localhost:9083")\
        .config("hive.exec.dynamic.partition", "true")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def transformation(spark):
    # Load data to spark DF
    hadoop_cfg = config_parser('prj-config.cfg', 'hadoop_config')
    bikes_data_url = hadoop_cfg['host'] + ':' + hadoop_cfg['port'] + "/datalake/bikes"
    bikes_df = spark.read.parquet(bikes_data_url)

    # Get the latest records
    window = Window.partitionBy("id").orderBy(col("registration_updated_at").desc())
    latest_record = bikes_df.withColumn('latest_rank', row_number().over(window))\
        .filter('latest_rank == 1')\
        .drop('latest_rank')

    # Write to Hive table
    latest_record.write\
        .format('hive')\
        .mode('overwrite')\
        .saveAsTable('newest.bikes')


if __name__ == "__main__":
    hive_spark = create_hive_spark_session()
    transformation(hive_spark)
