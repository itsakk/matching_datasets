from elasticsearch import Elasticsearch
import pandas as pd
from ElasticSearch_functions import import_on_es, BC_generator, CN_generator

def Link_BC_CN (csv_output):
    
    es = Elasticsearch()
    
    
    bc_nodes = es.search(index="base_carbone", 
                    body={"query": { "match_all": {}}
                     }, size=10000)
    
    Data = pd.DataFrame(columns=['Node_BC', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2'])
    
    new_line = pd.DataFrame(columns=['Node_BC', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2'])
    
    bc_nodes["hits"]["hits"][0]["_source"]['node']
    
    for k in range(len(bc_nodes["hits"]["hits"])):
            node=bc_nodes["hits"]["hits"][k]["_source"]['node']
            new_line.loc[0, 'Node_BC'] = node
            cnet = es.search(index="conceptnet",
                        body={ "size": 3, "query": {
                                "multi_match" : {
                                    "query" : node,
                                      "fields" : [ "_node1"]}}
                          , "_source":["_node1", "_score"]}, explain=True)
            for i in range(len(cnet["hits"]["hits"])):
                new_line['node_cn_'+str(i)][0]= cnet["hits"]["hits"][i]["_source"]["_node1"]
                new_line['score_cn_'+str(i)][0] = cnet["hits"]["hits"][i]["_score"]
            Data = Data.append(new_line)
    
    Data = Data.reset_index()
    Data = Data.drop(['index'], axis=1)
    
    return Data.to_csv(csv_output, index=False)

if __name__ =='__main__':
    import_on_es('Base_carbone.csv', BC_generator, ',')
    import_on_es('ConceptNet_For_ElasticSearch.csv', CN_generator, '\t')
    Link_BC_CN('ConceptNet&BC_Links_Dataset.csv')
