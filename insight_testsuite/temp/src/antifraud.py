import csv
import sys
import os
import pandas
import numpy as np
import networkx as nx


class antifraud(object):

    def __init__(self):
        
        ## Get args
        self.batch_data = sys.argv[1]
        self.stream_data = sys.argv[2]
        self.output1 = sys.argv[3]
        self.output2 = sys.argv[4]
        self.output3 = sys.argv[5]

        if os.path.exists(self.output1):
            os.remove(self.output1)
        self.o1 = open(self.output1, 'w')

        if os.path.exists(self.output2):
            os.remove(self.output2)
        self.o2 = open(self.output2, 'w')

        if os.path.exists(self.output3):
            os.remove(self.output3)
        self.o3 = open(self.output3, 'w')

        self.relation_graph = pandas.DataFrame() #this will store relationships between users
        self.batch_graph = pandas.DataFrame()
        self.stream_graph = pandas.DataFrame() #this will be used to determine order in which data will be processed

    def run(self):
        print('Processing batch data...')
        self.parseBatchData()
        print('Processing stream data...')
        self.parseStreamData()
        print('Checking relationships')
        self.checkTrust()
        self.closeFiles()

    def closeFiles(self):
        self.o1.close()
        self.o2.close()
        self.o3.close()

    ## open batch data
    def parseBatchData(self):
        df = pandas.read_csv(self.batch_data,
                             names=['id1', 'id2'],
                             usecols=[1, 2],
                             encoding='utf-8',
                             engine='python',
                             sep=',',
                             quoting=csv.QUOTE_NONE,
                             skiprows=1,
                             skipinitialspace=True)
        #s = df.groupby(['id1', 'id2']).size()
        #m = s.unstack()
        #m = m.fillna(0)
        self.batch_graph = df

    ## open stream data
    def parseStreamData(self):
        df = pandas.read_csv(self.stream_data,
                             names=['id1', 'id2'],
                             usecols=[1, 2],
                             encoding='utf-8',
                             engine='python',
                             sep=',',
                             quoting=csv.QUOTE_NONE,
                             skiprows=1,
                             skipinitialspace=True)
        #s = df.groupby(['id1', 'id2']).size()
        #m = s.unstack()
        #m = m.fillna(0)
        #pandas.concat([self.relation_graph, m], axis=1)
        self.stream_graph = df

    ## output trustworthiness to file
    def checkTrust(self):
        for index, row in self.stream_graph.iterrows():
            #print(str(row['id1']) + ' ' + str(row['id2']))
            #check direct relation
            if self.checkRelation(self.batch_graph, 1, row['id1'], row['id2']) <= 1:
                self.o1.write('trusted\n')
                print('trusted')
            else:
                self.o1.write('unverified\n')
                print('unverified')

            #check friend of friend
            if self.checkRelation(self.batch_graph, 2, row['id1'], row['id2']) <= 2:
                self.o2.write('trusted\n')
                print('trusted')
            else:
                self.o2.write('unverified\n')
                print('unverified')

            #check 4th degree friend
            if self.checkRelation(self.batch_graph, 5, row['id1'], row['id2']) <= 4:
                self.o3.write('trusted\n')
                print('trusted')
            else:
                self.o3.write('unverified\n')
                print('unverified')

            self.batch_graph.loc[self.batch_graph.shape[0]] = row

    ## check direct relation with simple comparison of user's friend groups
    def checkRelation(self, graph, factor, user1, user2):
        try:
            n = nx.from_pandas_dataframe(graph, 'id1', 'id2')
            a = nx.shortest_path_length(n, user1, user2)
            return a
        except:
            return 100      

af = antifraud()
af.run()
