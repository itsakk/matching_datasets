import pandas as pd
import numpy as np
from ElasticSearch_functions import import_on_es, CPF_generator, CN_generator
from analyzer import function_analyzer
from sklearn.preprocessing import Normalizer

""" 
The aim of this function is to query our Elasticsearch Database. We imported Conceptnet and CPF database to match and create
new links between these two databases. To do so, we created a function Link_CPF_CN which returns a csv_file with all the links
created between the two datasets. First, we query cpf_dataset to return all the data which are the last nodes of the connected 
graph. Then, we match each data with conceptnet nodes with a multi_match query and we create a DataFrame where we add each link 
created with the score level of the query and we limit the numbers of links for one node of cpf to 5. We normalize the score data 
and we delete the links which are lower than 105% of the mean of the 5 scores.
Then we reset indexes and import again the datasets on ES using analyzer.py and ElasticSearch_functions.py but with the n_gram
tokenizer. We build again a DataFrame and normalize it and keep only links which are higher than 105% of the mean of the 5 scores.
We concatenate the two Datasets in a Final_Dataset and we compare the normalized score and keep only the links which have a score
higher than the mean of all the existing scores of the Final_Dataset.
"""

def Link_CPF_CN (csv_output):
    es = function_analyzer("standard")
    
    Data = pd.DataFrame(columns=['Node_CPF', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2', 'node_cn_3',
                                 'score_cn_3', 'node_cn_4', 'score_cn_4'])
    
    cpf_nodes = es.search(index="cpf", 
                    body={"query": {
                              "bool": {
                                  "must": [
                                      { "match": {"p":"prefLabel"}},
                                      { "match": {"s": "sousCategorie"}}
                                      ]}}
                          , "_source":["o"]
                          },
                          size=10000)
    
    new_line = pd.DataFrame(columns=['Node_CPF', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2', 'node_cn_3',
                                 'score_cn_3', 'node_cn_4', 'score_cn_4'])
    
    for k in range(len(cpf_nodes["hits"]["hits"])):
        # on prend un mot sur deux pour garder les mots francais
        # y a t'il une indication dans le fichier pour garder seulement le francais ?
        # voir ligne 2748 de ConceptNetCPF_Links_Dataset
        # verifier s'il y a un equivalent francais a remplacer
        if k%2==0 and k<len(cpf_nodes["hits"]["hits"]):
            node=cpf_nodes["hits"]["hits"][k]["_source"]["o"]
            new_line.loc[0, 'Node_CPF'] = node
            cnet = es.search(index="conceptnet",
                        body={ "size": 5, "query": {
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
    
    Data.loc[:,['score_cn_0', 'score_cn_1','score_cn_2', 'score_cn_3',
                     'score_cn_4']] = Normalizer(norm='l1').fit_transform(Data[['score_cn_0',
                     'score_cn_1','score_cn_2', 'score_cn_3','score_cn_4']])
    
    for i in range(len(Data)):
        mean = Data[['score_cn_0', 'score_cn_1', 'score_cn_2', 'score_cn_3',
                     'score_cn_4']].loc[i].mean()
        for k in reversed (range(5)):
            while Data['score_cn_'+str(k)][i]<1.05*mean:
                Data['score_cn_'+str(k)][i] = np.nan
    
    """ Now that we have a first Dataset with the standard tokenizr, we are going to reset 
    our indexes and build it again with a ngram tokenizer"""
    
    es = function_analyzer("my_tokenizer")
    import_on_es('ConceptNet_For_ElasticSearch.csv', CN_generator, '\t')
    import_on_es('CPF.csv', CPF_generator, ',')
    
    cpf_nodes_ngram = es.search(index="cpf", 
                    body={"query": {
                              "bool": {
                                  "must": [
                                      { "match": {"p":"prefLabel"}},
                                      { "match": {"s": "sousCategorie"}}
                                      ]}}
                          , "_source":["o"]
                          },
                          size=10000)
    
    Data_ngram = pd.DataFrame(columns=['Node_CPF', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2', 'node_cn_3',
                                 'score_cn_3', 'node_cn_4', 'score_cn_4'])
    
    new_line_ngram = pd.DataFrame(columns=['Node_CPF', 'node_cn_0', 'score_cn_0', 'node_cn_1',
                                 'score_cn_1', 'node_cn_2', 'score_cn_2', 'node_cn_3',
                                 'score_cn_3', 'node_cn_4', 'score_cn_4'])
    
    for k in range(len(cpf_nodes_ngram["hits"]["hits"])):
        if k%2==0 and k<len(cpf_nodes_ngram["hits"]["hits"]):
            node_ngram=cpf_nodes_ngram["hits"]["hits"][k]["_source"]["o"]
            print(node_ngram)
            new_line_ngram.loc[0, 'Node_CPF'] = node_ngram
            cnet_ngram = es.search(index="conceptnet",
                        body={ "size": 5, "query": {
                                "multi_match" : {
                                    "query" : node_ngram,
                                      "fields" : [ "_node1"]}}
                          , "_source":["_node1", "_score"]}, explain=True) 
            for i in range(len(cnet_ngram["hits"]["hits"])):
                new_line_ngram['node_cn_'+str(i)][0]= cnet_ngram["hits"]["hits"][i]["_source"]["_node1"]
                new_line_ngram['score_cn_'+str(i)][0] = cnet_ngram["hits"]["hits"][i]["_score"]
            Data_ngram = Data_ngram.append(new_line_ngram)
            
    Data_ngram = Data_ngram.reset_index()
    Data_ngram = Data_ngram.drop(['index'], axis=1)
    
    Data_ngram.loc[:,['score_cn_0', 'score_cn_1','score_cn_2', 'score_cn_3',
                     'score_cn_4']] = Normalizer(norm='l1').fit_transform(Data_ngram[['score_cn_0',
                     'score_cn_1','score_cn_2', 'score_cn_3','score_cn_4']])
                                                                                      
    for i in range(len(Data_ngram)):
        mean_ngram = Data_ngram[['score_cn_0', 'score_cn_1', 'score_cn_2', 'score_cn_3',
                     'score_cn_4']].loc[i].mean()
        for k in reversed (range(5)):
            while Data_ngram['score_cn_'+str(k)][i]<1.05*mean_ngram:
                Data_ngram['score_cn_'+str(k)][i] = np.nan
    
    Data_ngram.rename(columns={'score_cn_0': 'score_cn_0_ngram', 'score_cn_1': 'score_cn_1_ngram',
                               'score_cn_2': 'score_cn_2_ngram', 'score_cn_3': 'score_cn_3_ngram',
                               'score_cn_4': 'score_cn_4_ngram'}, inplace=True)
    
    
    Data_Final = pd.concat([Data, Data_ngram], axis=1)
    
    for i in range(len(Data_Final)):
        mean_final = Data_Final[['score_cn_0', 'score_cn_1', 'score_cn_2', 'score_cn_3', 'score_cn_4',
                           'score_cn_0_ngram','score_cn_1_ngram', 'score_cn_2_ngram',
                           'score_cn_3_ngram', 'score_cn_4_ngram']].loc[i].mean()
        for k in reversed (range(5)):
            while Data_Final['score_cn_'+str(k)][i]<mean_final:
                Data_Final.loc[i,'score_cn_'+str(k)] = np.nan
        for k in reversed (range(5)):
            while Data_Final['score_cn_'+str(k)+'_ngram'][i]<mean_final:
                Data_Final.loc[i, 'score_cn_'+str(k)+'_ngram'] = np.nan
                
    return Data_Final.to_csv(csv_output, index=False)

if __name__ == '__main__':
    Link_CPF_CN("ConceptNet&CPF_Links_Dataset.csv")
