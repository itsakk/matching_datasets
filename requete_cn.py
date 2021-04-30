import pandas as pd

conceptnet = pd.read_csv('ConceptNet_For_ElasticSearch.csv', sep='\t')

print(conceptnet.loc[lambda conceptnet: conceptnet['node1'] == "test" ])