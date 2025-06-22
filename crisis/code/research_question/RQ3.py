import requests
import pandas as pd
import time
#import pprint

df= pd.read_csv("C:/Users/annan/Downloads/iris-data-2025-05-30/POSTPROCESS-iris-data-2025-05-27/ODS_L1_IR_ITEM_IDENTIFIER.csv", low_memory=False) 
list_doi= df["IDE_DOI"].dropna().tolist() 
#print(list_doi)

HTTP_HEADERS = {"authorization": "63f51be3-c310-43b7-b03a-478ff055ac62"} 

def get_references(doi):        #outgoing citations
    url = f"https://opencitations.net/index/api/v2/references/doi:{doi}"
    response = requests.get(url, headers=HTTP_HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching references for {doi}: {response.status_code}")
        return []

def get_citations(doi):     #ingoing citations
    url = f"https://opencitations.net/index/api/v2/citations/doi:{doi}"
    response = requests.get(url)
    print(response.status_code)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching citations for {doi}: {response.status_code}")
        return []
    
print(get_citations('10.6092/issn.1828-5961/3659'))
    
"""outgoing= list()    
for doi in list_doi[:20]:
    outgoing.append(get_references(doi))
    time.sleep(1)
#pprint.pp(outgoing)
print(outgoing)"""

ingoing_cit= list()
ingoing_oci = set()    
for doi in list_doi[:10]:
    citations = get_citations(doi)
    for cit in citations:
        oci = cit.get("oci")
        if oci is not None and oci not in ingoing_oci:
            ingoing_cit.append(cit)
            ingoing_oci.add(oci)
    time.sleep(1)
print(ingoing_cit)
#pprint.pp(ingoing)"""
