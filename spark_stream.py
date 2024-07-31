from pyspark.sql import SparkSession


def create_keyspace(session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS spark_streams \
                    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':'1'}")
    print("KeySpace Created Scuuessfully")

def creat_table(session):
    session.execute("CREATE TABLE IF NOT EXISTS ")

def connect_to_kafka(spark_conn):
    print("Function Started")
    spark_df = None
    try:    
        spark_df = spark_conn.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', 'broker:29092')\
            .option('subscribe', 'user_created')\
            .load()
        print(spark_df)
        print("Kafka dataframe created successfully")
    except Exception as e:
        print(f"Kafka dataframe could not be created due to {e}")
    return spark_df

def create_spark_connection():
    print("Connection Started")
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")
    
    return s_conn

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
