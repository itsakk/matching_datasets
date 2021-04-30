import numpy as np 
import pandas as pd

def handLinks(csv_hand,csv_unmodified,csv_output):
    
    """ This function aims to create a csv file where all the nodes that we had to add manually are given. """
    
    hand = pd.read_csv(csv_hand, sep=",")
    unmodified = pd.read_csv(csv_unmodified, sep=",")
    temp_data=pd.DataFrame()
    for row in hand.iterrows():
        hand,hand_delete,hand_add=dict(),[],[]
        for k in range(len(row[-1])-1):
            if row[-1][k]==0:
                hand_delete.append(row[-1][k-1])
            elif row[-1][k]==1.:
                hand_add.append(row[-1][k-1])
        hand["hand_delete"],hand["hand_add"]=hand_delete,hand_add
        temp_data=temp_data.append(hand,ignore_index=True)
    Data_Final = pd.concat([unmodified, temp_data], axis=1)
    
    return Data_Final.to_csv(csv_output, index=False)

def neo4jfile(csv_hand, csv_output):
    
    """ This function aims to create a csv_file that will be only composed of the nodes from CPF file and the nodes from ConceptNet that are related to.
    We remove all es scores and we remove all the conceptnet nodes that we thought they were not close enough to the cpf node."""
    
    
    hand = pd.read_csv(csv_hand, sep=",")
    hand['score_cn_0'] = hand['score_cn_0'].apply(lambda x: float(x))
    hand['score_cn_1'] = hand['score_cn_1'].apply(lambda x: float(x))
    hand['score_cn_2'] = hand['score_cn_2'].apply(lambda x: float(x))
    hand['score_cn_3'] = hand['score_cn_3'].apply(lambda x: float(x))
    hand['score_cn_4'] = hand['score_cn_4'].apply(lambda x: float(x))
    hand['score_cn_0_ngram'] = hand['score_cn_0_ngram'].apply(lambda x: float(x))
    hand['score_cn_1_ngram'] = hand['score_cn_1_ngram'].apply(lambda x: float(x))
    hand['score_cn_2_ngram'] = hand['score_cn_2_ngram'].apply(lambda x: float(x))
    hand['score_cn_3_ngram'] = hand['score_cn_3_ngram'].apply(lambda x: float(x))
    hand['score_cn_4_ngram'] = hand['score_cn_4_ngram'].apply(lambda x: float(x))
    
    score = ['score_cn_0', 'score_cn_1', 'score_cn_2', 'score_cn_3', 'score_cn_4',
                               'score_cn_0_ngram','score_cn_1_ngram', 'score_cn_2_ngram',
                               'score_cn_3_ngram', 'score_cn_4_ngram']
    
    node = ['node_cn_0', 'node_cn_1', 'node_cn_2', 'node_cn_3', 'node_cn_4',
                               'node_cn_0_ngram','node_cn_1_ngram', 'node_cn_2_ngram',
                               'node_cn_3_ngram', 'node_cn_4_ngram']
    
    Data = pd.DataFrame(columns=['Links', 'cpf_nodes'])
    L = []
    
    hand.rename(columns={'node_cn_0.1': 'node_cn_0_ngram',
                         'node_cn_1.1': 'node_cn_1_ngram', 'node_cn_2.1': 'node_cn_2_ngram',
                         'node_cn_3.1': 'node_cn_3_ngram', 'node_cn_4.1': 'node_cn_4_ngram'}, inplace=True)
    
    for k in range(len(hand)):
        for i in range(len(score)):
            if hand[score[i]][k]!=0 and np.isnan(hand[score[i]][k])==False:
                L.append(hand[node[i]][k])
        Data.loc[k, 'Links'] = L
        Data.loc[k, 'cpf_nodes'] = hand['Node_CPF'][k]
        L =[]        
    return Data.to_csv(csv_output, index=False)

def clean_csv(csv_input):
    """
    This file add and delete links that we added by hand by exploring directly into conceptnet's dictionnary. The difference between this function
    and the function neo4jfile is that in the other function, we only added nodes that were already found by our es request. This csv file we put 
    in enter has two columns where we added nodes by exploring conceptnet's dictionnary.'
    """
    
    cnet=pd.read_csv(csv_input,sep = ',')
    cnet['Links'] = cnet['Links'].apply(lambda value:value[2:])
    cnet['Links'] = cnet['Links'].apply(lambda value:value[:-2])
    old_nodes = cnet["Links"].str.split("', '", expand = True)
    new_nodes = cnet["add"].str.split(",", expand = True)
    delete_nodes = cnet["delete"].str.split(",", expand = True)
    ##final_data = cnet["cpf_nodes"]
    final_data = pd.DataFrame(columns=['Node_CPF', 'Links'])
    final_list=[]
    final_list2=[]


    for k in range(len(cnet)):
        L=[]
        for i in range(len(old_nodes.columns)):
            L.append(old_nodes[i][k])
        for j in range(len(delete_nodes.columns)):
            if type(delete_nodes[j][k])==str:
                L.remove(delete_nodes[j][k])
        for l in range(len(new_nodes.columns)):
            if type(new_nodes[l][k])==str:
                L.append(new_nodes[l][k])
        L = list(filter(None, L))
        final_list.append(L)
        
    for k in range(len(final_list)):
        new_list = [] 
        for i in final_list[k] : 
            if i not in new_list: 
                new_list.append(i)
        final_list2.append(new_list)
    
    final_list=pd.DataFrame(final_list2)
    final_data=final_data["Links"].append(final_list)        
    final_data=pd.concat([final_data,cnet["cpf_nodes"]],axis=1)

    return(final_data.to_csv("Links.csv", index=False))


if __name__ =='__main__':
    handLinks("handlinks.csv","ConceptNetCPF_Links_Dataset.csv","CnetCpfFinalLinks.csv")
    neo4jfile('handlinks.csv', 'Final_Links.csv')
    clean_csv('Final_Links_2.csv')
