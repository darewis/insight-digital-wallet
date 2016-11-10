import csv
import sys
import os


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

        self.relation_graph = {} #this will store relationships between users

    def run(self):
        self.parseBatchData()
        self.parseStreamData()
        self.closeFiles()

    def closeFiles(self):
        self.o1.close()

    ## adds a relationship to relation_graph
    def add_relation(self, row):
        self.relation_graph.setdefault(row['id1'],set()).add(row['id2'])
        self.relation_graph.setdefault(row['id2'],set()).add(row['id1'])

    ## open batch data
    def parseBatchData(self):
        linenumber = 1
        with open(self.batch_data, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', skipinitialspace = True, quoting=csv.QUOTE_NONE)
            for row in reader:
                linenumber += 1
                print('batch: ' + str(linenumber) + ' ' + str(row['id1']) + ' ' + str(row['id2']))
                self.add_relation(row)

    ## open stream data
    def parseStreamData(self):
        with open(self.stream_data, encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',', skipinitialspace = True, quoting=csv.QUOTE_NONE)
            for row in reader:
                print('stream: ' + str(row['id1']) + ' ' + str(row['id2']))
                self.checkTrust(row['id1'], row['id2'])
                self.add_relation(row)

    ## output trustworthiness to file
    def checkTrust(self, user1, user2):
        if user1 in self.relation_graph and user2 in self.relation_graph:
            if self.checkDirectRelation(user1, user2):
                self.o1.write('trusted\n')
            else:
                self.o1.write('unverified\n')

            if self.checkFriendOfFriend(user1, user2):
                self.o2.write('trusted\n')
            else:
                self.o2.write('unverified\n')

            if self.checkFourthDegree(user1, user2):
                self.o3.write('trusted\n')
            else:
                self.o3.write('unverified\n')
        else:
            self.o1.write('unverified\n')
            self.o2.write('unverified\n')
            self.o3.write('unverified\n')

    ## check direct relation with simple comparison of user's friend groups
    def checkDirectRelation(self, user1, user2):
        if user2 in self.relation_graph[user1]:
            return True
        else:
            return False

    ## check for one degree of seperation by comparing both user's set of friends
    def checkFriendOfFriend(self, user1, user2):
        if self.checkDirectRelation(user1, user2): #check first that a serperation exists
            return True
        else:
            mutual_friends = self.relation_graph[user1].intersection(self.relation_graph[user2])
            if len(mutual_friends) > 0:
                return True
            else:
                return False

    def checkFourthDegree(self, user1, user2):
        if self.checkFriendOfFriend(user1, user2):
            return True
        else:
            ## find shortest path using BFS
            if len(next(self.BFS(user1, user2))) < 6:
                return True
            else:
                return False
            
    def BFS(self, start, end):
        queue = [(start, [start])]
        while queue:
            (vertex, path) = queue.pop(0)
            for next in self.relation_graph[vertex] - set(path):
                if next == end:
                    yield path + [next]
                else:
                    queue.append((next, path + [next]))
            

af = antifraud()
af.run()
