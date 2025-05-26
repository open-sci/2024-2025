import requests
import pandas as pd
import pprint

df= pd.read_csv("C:/Users/annan/Università/OPEN SCIENCE/iris-data-2024-06-04/ODS_L1_IR_ITEM_IDENTIFIER.csv") 
list_doi= df["IDE_DOI"].dropna().tolist() 
#print(len(list_doi))

def get_references(doi):        #outgoing citations
    url = f"https://opencitations.net/index/api/v2/references/doi:{doi}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching references for {doi}: {response.status_code}")
        return []

def get_citations(doi):     #ingoing citations
    url = f"https://opencitations.net/index/api/v2/citations/doi:{doi}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching citations for {doi}: {response.status_code}")
        return []
    
outgoing= list()    
for doi in list_doi[:20]:
    outgoing.append(get_references(doi))
#pprint.pp(outgoing)

ingoing= list()    
for doi in list_doi[:20]:
    ingoing.append(get_citations(doi))
#pprint.pp(ingoing)
