import pyspark
from pyspark.sql import SparkSession
import s3fs
from pyspark.sql.functions import  input_file_name
from pyspark.sql import functions as F
from decouple import config

aws_access_key = config('AWS_ACCESS_KEY')
aws_secret_key = config('AWS_SECRET_KEY')

spark=SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2').config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").config('spark.hadoop.fs.s3a.access.key', aws_access_key).config('spark.hadoop.fs.s3a.secret.key', aws_secret_key).appName('test_aws_2').getOrCreate()


    
symbols_file_path = 's3a://stock-etfs-ai-project/all_data/raw_data/symbols_valid_meta.csv'
symbols_df = spark.read.csv(symbols_file_path, header=True, inferSchema=True)

main_file_path = 's3a://stock-etfs-ai-project/all_data/raw_data/**/*.csv'
main_df = spark.read.csv(main_file_path, header=True, inferSchema=True)
main_df = main_df.withColumn("filename",  F.element_at(F.split(F.input_file_name(), "/"),-1)   )
main_df = main_df.withColumn('name', F.split(main_df['filename'], '\.').getItem(0))

main_df = main_df.drop('filename')

new_df = main_df.join(symbols_df, main_df.name == symbols_df.Symbol).select(
    symbols_df['Symbol'], symbols_df['Security Name'], 
    main_df['*'])
    
new_df = new_df.drop('name')

new_df.write.mode('overwrite').parquet("s3a://stock-etfs-ai-project/all_data/transformed_stocks_etf_data.parquet")
    