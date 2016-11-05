import csv
import sys
from collections import defaultdict

## Global vars
relation_graph = {} #this will store relationships between users

## open batch data
with open(sys.argv[1]) as csvfile:
    batchreader = csv.DictReader(csvfile, delimiter=',', skipinitialspace = True)
    for row in batchreader:
        print(row['id1'] + ' ' + row['id2'])
        relation_graph.setdefault(row['id1'],set()).add(row['id2'])
        relation_graph.setdefault(row['id2'],set()).add(row['id1'])

print(relation_graph)
