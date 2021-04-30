from elasticsearch import Elasticsearch as es
from elasticsearch import helpers
import pandas as pd

def CN_generator(df):
    """
    The aim of this function is index Cnet into ElasticSearch.
    We only import node1 for our purpose but we can still import node2 and the
    relation between them. We use panda to speed up the import in chunks instead
    of indexing one by one each line of conceptnet.
    """
    df_iter = df.iterrows()
    for index, row in df_iter:
        yield {
                "_index": 'conceptnet',
                "_node1": row["node1"],
##                "_node2": row["node2"],
##                "_relation": row["Relation"],
            }

def CPF_generator(df):
    """
    The aim of this function is index CPF into ElasticSearch.
    We import subject, predicate and object. 
    """
    df_iter = df.iterrows()
    for index, row in df_iter:
        yield {
                "_index": 'cpf',
                "s": row["s"],
                "p": row["p"],
                "o": row["o"],
            }

def import_on_es(csv_enter, data_generator, sep):
    """
    The aim of this function is to push CPF or Cnet into ElasticSearch.
    """
    es_client = es(http_compress=True)
    data = pd.read_csv(csv_enter, sep=sep)
    return(helpers.bulk(es_client, data_generator(data)))

if __name__ =='__main__':
    import_on_es('CPF.csv', CPF_generator, ',')
    import_on_es('ConceptNet_For_ElasticSearch.csv', CN_generator, '\t')