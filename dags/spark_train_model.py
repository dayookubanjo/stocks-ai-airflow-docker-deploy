import pyspark
from pyspark.sql import SparkSession
import s3fs
from pyspark.sql.functions import  input_file_name
from pyspark.sql import functions as F
from decouple import config
from pyspark.sql.window import Window 
import numpy as np 
from pyspark.sql.types import FloatType
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import pickle
import numpy as np
from sklearn.linear_model import LinearRegression 

aws_access_key = config('AWS_ACCESS_KEY')
aws_secret_key = config('AWS_SECRET_KEY')

spark=SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2').config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").config('spark.hadoop.fs.s3a.access.key', aws_access_key).config('spark.hadoop.fs.s3a.secret.key', aws_secret_key).appName('model_training').getOrCreate()

file_path = 's3a://stock-etfs-ai-project/all_data/ml_input_data.parquet/*.parquet'
pyspark_df = spark.read.parquet(file_path, header=True, inferSchema=True)
 
pandas_df = pyspark_df.toPandas()

#Missing Values
pandas_df.dropna(axis = 0, inplace = True)

pandas_df.reset_index(drop = True, inplace= True)

x_df = pandas_df[['Symbol', 'vol_moving_avg', 'adj_close_rolling_med']]
y_df = pandas_df['Volume_New']


x_train, x_test,y_train, y_test = train_test_split(x_df,y_df, test_size = 0.2, stratify=x_df[["Symbol"]])


ct = ColumnTransformer(transformers= [ ("encoder", OneHotEncoder(),[0])], remainder="passthrough")

symbol_encoder =  ct.fit(x_df) 


filename = './sklearn_models/symbol_encoder.sav'
pickle.dump(symbol_encoder, open(filename, 'wb'))


x_train =  symbol_encoder.transform(x_train) 
x_test =  symbol_encoder.transform(x_test) 


LR = LinearRegression()
LR.fit(x_train  ,y_train)

filename = './sklearn_models/linear_regression.sav'
pickle.dump(LR, open(filename, 'wb'))

