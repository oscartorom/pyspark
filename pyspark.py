# Apache Spark practice
import os
import pandas as pd

# pyspark launch
import findspark
findspark.init()
import pyspark

from pyspark.sql import SparkSession

# Create dataset
def create_local_data():
    if 'data' not in os.listdir():
        print('Creating local directory to store data')
        os.mkdir('data')
    
    if 'initial_data.csv' not in os.listdir('data'):
        data = pd.DataFrame([
            {'Name': 'Oscar', 'Age': 25, 'City':'Sydney'},
            {'Name': 'Sofia', 'Age': 3, 'City': 'Sydney'},
            {'Name': 'Random', 'Age': 78, 'City': 'Dubai'}]
        )

        data.to_csv('data/initial_data.csv', index=False)

create_local_data()

spark = SparkSession.builder.appName('Practice').getOrCreate()
df_pyspark = spark.read.option('header','true').csv('data/initial_data.csv')

# Show all data
df_pyspark.show()

# Show initial rowss
df_pyspark.head(2)

#Show schema
df_pyspark.printSchema()