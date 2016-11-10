import pandas as pd
import csv

Location = r'./paymo_input/stream_payment.txt'

def parseBatchData():
    df = pd.read_csv(Location,
                     index_col=False,
                     names=['id1', 'id2'],
                     usecols=[1, 2],
                     encoding='utf-8',
                     engine='python',
                     sep=',',
                     quoting=csv.QUOTE_NONE,
                     skiprows=1,
                     skipinitialspace=True)
    print(df)

parseBatchData()
