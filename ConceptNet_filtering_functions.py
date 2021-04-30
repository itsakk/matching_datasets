import pandas as pd
import numpy as np

def filter_by_language(csv_enter, lang_needed, csv_output):
    """
    The aim of this function is to filter data from ConceptNet file by language. csv_enter is the filename of our Conceptnet raw data 
    that we want to filter. lang_needed is a list of strings where we specify which type of language we want to keep. For instance,if
    we want to filter our data and only keep french & english words, we will have to specify lang_needed like this:
    lang_needed = ["/en/", "/fr/"]
    csv_ouput is the filename we want to give to the csv we are going to export which contains only french & english words.
    We took index values after looking 
    """

    columns = ["URI", "Relation", "node1", "node2", "edge"]
    conceptnet = pd.read_csv(csv_enter, skiprows=563505, chunksize=100000, sep='\t', names=columns, header=None)
    conceptnet_lang_needed= pd.DataFrame()

    for i, concept in enumerate(conceptnet):
        #concept.drop('Unnamed: 0',axis = 1)
        print(i)
        for k in range(10):
            loc=k*10000+i*100000
            if loc<=33500000:
                lang=concept['node1'][loc][2:6] 
                if lang in lang_needed:
                    if concept['node1'][loc+9999][2:6] in lang_needed:
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc:loc+9999],ignore_index=True)
                    else :
                        x1=0
                        while concept['node1'][loc+9999-x1][2:6] not in lang_needed:
                            x1=x1+1
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc:loc+9999-x1],ignore_index=True)
                else  :
                    x2=0
                    if concept['node1'][loc+9999][2:6]in lang_needed:
                        while concept['node1'][loc+9999-x2][2:6]  in lang_needed:
                            x2=x2+1
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc+10000-x2:loc+9999],ignore_index=True)
            else:
                loc=33510000
                lang=concept['node1'][loc][2:6]
                if lang in lang_needed:
                    if concept['node1'][loc+1410][2:6]in lang_needed:
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc:loc+1410],ignore_index=True)
                    else :
                        x1=0
                        while concept['node1'][loc+1410-x1][2:6] not in lang_needed:
                            x1=x1+1
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc:loc+1410],ignore_index=True)
                else  :
                    x2=0
                    if concept['node1'][loc+1410][2:6]in lang_needed:
                        while concept['node1'][loc+1410-x2][2:6]  in lang_needed:
                            x2=x2+1
                        conceptnet_lang_needed=conceptnet_lang_needed.append(concept.loc[loc-x2+1411:loc+1410],ignore_index=True)
                        
    len_csv=len(conceptnet_lang_needed)
    conceptnet_lang_needed=conceptnet_lang_needed.drop(["edge","URI"],axis=1)
    return conceptnet_lang_needed.to_csv(csv_output+str(len_csv)+".csv", sep='\t', index=False)

def relation(csv_enter):
    """
    The aim of this function is to give all relations that are used in the csv_enter.
    The output of this function is a text file with relations found in the csv.
    """

    conceptnet = pd.read_csv(csv_enter, sep='\t')
    relation=conceptnet["Relation"].unique()
    relation=relation.tolist()
    return(relation)

def delete_relation(csv_enter, relation_del, csv_output):
    """
    The aim of this function is to filter data from ConceptNet file by relation. csv_enter is the filename of our Conceptnet data 
    that we want to filter. Relation is a list of strings where we specify which type of relation we want to delete.
    csv_ouput is the filename we want to give to the csv we are going to export which contains only relations that we need.

    We don't treat the case of the last relation. 
    """

    conceptnet = pd.read_csv(csv_enter, sep='\t')

    relations=relation(csv_enter)
    pos=conceptnet["Relation"].searchsorted(relation_del)
    pos2=conceptnet["Relation"].searchsorted(relations[relations.index(relation_del)+1])-1

    conceptnet=conceptnet.drop(conceptnet.index[pos:pos2])
    return (conceptnet.to_csv(csv_output+""+str(len(conceptnet))+".csv", sep="\t",header=True,index=False))

def clear_csv(csv_enter, csv_output):
    """
    The aim of this function is to clean data from ConceptNet file. csv_enter is the filename of our Conceptnet data 
    that we want to clean.
    This function clear data to get only words. For example, we keep "take" instead of "/r/en/take".
    It also delete "_" in compounds words. For example, we keep "blé dur" instead of "blé_dur".
    csv_ouput is the filename we want to give to the csv we are going to export which contains clean data that we need.
    """
    
    conceptnet = pd.read_csv(csv_enter, sep='\t')
    conceptnet["Relation"] = conceptnet["Relation"].apply(lambda value:value[3:])
    conceptnet["node1"] = conceptnet["node1"].apply(lambda value:value[6:])
    conceptnet['node1'] = conceptnet['node1'].apply(lambda value:value.split("/"))
    conceptnet['node1'] = conceptnet['node1'].apply(lambda value:value[0])
    conceptnet['node2'] = conceptnet['node2'].apply(lambda value:value[6:])
    conceptnet['node2'] = conceptnet['node2'].apply(lambda value:value.split("/"))
    conceptnet['node1'] = conceptnet['node1'].apply(lambda value:value.replace("_", " "))
    conceptnet['node2'] = conceptnet['node2'].apply(lambda value:value[1] if value[0]=='' else value[0])
    conceptnet.dropna(axis=0, inplace=True)
    return conceptnet.to_csv(csv_output+str(len(conceptnet))+".csv", sep='\t', index=False)

def clear_nan_values(csv_enter, csv_output):
    """
    The aim of this function is to clean nan values from ConceptNet file.
    csv_enter is the filename of our Conceptnet data that we want to clean.
    csv_ouput is the filename we want to give to the csv we are going to export which contains clean data that we need.
    """
    conceptnet = pd.read_csv(csv_enter, sep='\t')
    conceptnet.dropna(axis=0, inplace=True)
    return conceptnet.to_csv(csv_output+str(len(conceptnet))+".csv", sep='\t', index=False)

def CN_for_ES(csv_enter, csv_output):
    """
    The aim of this function is to delete duplicates nodes in ConceptNet.
    csv_enter is the filename of our Conceptnet data that we want to clean.
    csv_ouput is the filename we want to give to the csv we are going to export which contains clean data that we need.
    """
    conceptnet = pd.read_csv(csv_enter, sep='\t')
    conceptnet = conceptnet.drop(["Relation", "node2"], axis=1)
    conceptnet = conceptnet.drop_duplicates(subset=['node1'], keep='first')
    conceptnet = conceptnet.reset_index()
    conceptnet = conceptnet.drop(['index'], axis=1)
    return conceptnet.to_csv(csv_output+".csv", sep='\t', index=False)
