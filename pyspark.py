# Apache Spark practice
import pyspark
import os
import pandas as pd
# Create dataset


def create_local_data():
    if 'data' not in os.listdir():
        print('Creating local directory to store data')
        os.mkdir('data')
    
    if 'initial_data.csv' not in os.listdir('data'):
        data = pd.DataFrame(
            {'Name': 'Oscsar', 'Age': 25, 'City':'Sydney'},
            {'Name': 'Sofia', 'Age': 3, 'City': 'Sydney'},
            {'Name': 'Random', 'Age': 78, 'City': 'Dubai'}
        )

        data.to_csv('data/initial_data.csv')

create_local_data()