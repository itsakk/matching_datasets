from ConceptNet_filtering_functions import filter_by_language, delete_relation, clear_csv, clear_nan_values, CN_for_ES
from ElasticSearch_functions import import_on_es, CPF_generator, CN_generator
from Queries_es import Link_CPF_CN

"""
The aim of this file is to create links between our databases CPF and Cnet.

First, we filter Cnet to get only French or English words with filter_by_language.
We delete the relation ExternalURL with delete_relation and clear words to get clear
raw data with clear_csv. Then we delete duplicates nodes with CN_for_ES and nan values that
will create errors in ES with clear_nan_values.
We end this file with the creation of 2 databases on ElasticSearch, ready to be used.

"""

def main():
    filter_by_language('assertions.csv', ["/fr/"], 'conceptnet_fr_')
    delete_relation("conceptnet_fr_5694690.csv", "/r/ExternalURL", "concepnet_fr_filtered")
    clear_csv('concepnet_fr_filtered3859327.csv', 'ConceptNet_Cleared_')
    clear_nan_values('ConceptNet_Cleared_3859327.csv', 'ConceptNet_Final_Dataset_')
    CN_for_ES('ConceptNet_Final_Dataset_3859282.csv', 'ConceptNet_For_ElasticSearch')
    import_on_es('ConceptNet_For_ElasticSearch.csv', CN_generator, '\t')
    import_on_es('CPF.csv', CPF_generator, ',')
    Link_CPF_CN("ConceptNet&CPF_Links_Dataset.csv")

if __name__ == "__main__":
    main()