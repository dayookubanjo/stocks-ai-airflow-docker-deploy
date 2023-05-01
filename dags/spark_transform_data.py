import pyspark
from pyspark.sql import SparkSession
import s3fs
from pyspark.sql.functions import  input_file_name
from pyspark.sql import functions as F
from decouple import config
from pyspark.sql.window import Window 
import numpy as np 
from pyspark.sql.types import FloatType

aws_access_key = config('AWS_ACCESS_KEY')
aws_secret_key = config('AWS_SECRET_KEY')

spark=SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2').config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").config('spark.hadoop.fs.s3a.access.key', aws_access_key).config('spark.hadoop.fs.s3a.secret.key', aws_secret_key).appName('transform').getOrCreate()

 
data_file_path = 's3a://stock-etfs-ai-project/all_data/transformed_stocks_etf_data.parquet/*.parquet'
pyspark_df = spark.read.parquet(data_file_path, header=True, inferSchema=True)

pyspark_df=pyspark_df.withColumn('Date', F.to_date('Date'))#format date if string
new = (pyspark_df.groupby('Symbol').agg(F.expr('max(Date)').alias('max_date'),F.expr('min(Date)').alias('min_date'))#Compute max and min date for use in generating date range
.withColumn('Date',F.expr("explode(sequence(min_date,max_date,interval 1 day))"))#Use sequence to compute range
       .drop('max_date','min_date')#drop unwanted columns
      )
#Join new df back to df
pyspark_df = pyspark_df.join(new, how='right', on=['Symbol', 'Date']) 


pyspark_df = pyspark_df.withColumn("Open_New", F.last('Open', True).over(Window.partitionBy('Symbol').orderBy('Date')))
pyspark_df = pyspark_df.withColumn("High_New", F.last('High', True).over(Window.partitionBy('Symbol').orderBy('Date')))
pyspark_df = pyspark_df.withColumn("Low_New", F.last('Low', True).over(Window.partitionBy('Symbol').orderBy('Date')))
pyspark_df = pyspark_df.withColumn("Close_New", F.last('Close', True).over(Window.partitionBy('Symbol').orderBy('Date')))
pyspark_df = pyspark_df.withColumn("Adj Close New", F.last('Adj Close', True).over(Window.partitionBy('Symbol').orderBy('Date')))
pyspark_df = pyspark_df.withColumn("Volume_New", F.last('Volume', True).over(Window.partitionBy('Symbol').orderBy('Date')))
 
days = lambda i: i * 86400
pyspark_df = pyspark_df.withColumn('vol_moving_avg', 
                                   F.avg("Volume_New").over(Window.partitionBy('Symbol').orderBy(F.col("Date").cast("timestamp").cast('long')).rangeBetween(-days(30), -1))
                                  ) 

pyspark_df = pyspark_df.withColumnRenamed('Adj Close New', 'Adj_Close_New')

pyspark_df = pyspark_df.withColumn(
    "adj_close_rolling_med",
    F.percentile_approx('Adj_Close_New',0.5).over(
        Window.partitionBy("Symbol")
        .orderBy(F.col("Date").cast("timestamp").cast('long'))
        .rangeBetween(-days(30), -1) 
    ),
)





w = Window.partitionBy("Symbol").orderBy(F.col("Date").cast("timestamp").cast('long')).rangeBetween(-days(30), -1)
median_udf = F.udf(lambda x: float(np.median(x)), FloatType())

pyspark_df= pyspark_df.withColumn("list", F.collect_list('Adj_Close_New').over(w)).withColumn("adj_close_rolling_med", median_udf("list"))


pyspark_df=pyspark_df.drop('list')

pyspark_df = pyspark_df.select('Symbol','Volume_New', 'vol_moving_avg', 'adj_close_rolling_med')

pyspark_df.write.mode('overwrite').parquet("s3a://stock-etfs-ai-project/all_data/ml_input_data.parquet")



