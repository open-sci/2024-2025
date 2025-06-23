import requests
import pandas as pd
import time

df= pd.read_csv("C:/Users/annan/Downloads/mashup.csv", low_memory=False)
list_doi= df["doi"].dropna().tolist() 


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
    

rows1 = []    #DF outgoing cit

for doi in list_doi:
    outgoing_cit= list()
    outgoing_oci = set() 
    citations = get_references(doi)
    for cit in citations:   #check duplicates
        oci = cit.get("oci")
        if oci is not None and oci not in outgoing_oci:
            outgoing_cit.append(cit)
            outgoing_oci.add(oci)
    rows1.append({
        'doi': doi,
        'cite_num_outgoing': len(outgoing_cit),
        'oci_outgoing': list(outgoing_oci)
        })
    time.sleep(1)

output_table1 = pd.DataFrame(rows1)


rows2 = []   #DF ingoing cit

for doi in list_doi:
    ingoing_cit= list()
    ingoing_oci = set() 
    citations = get_citations(doi)
    for cit in citations:
        oci = cit.get("oci")
        if oci is not None and oci not in ingoing_oci:
            ingoing_cit.append(cit)
            ingoing_oci.add(oci)
    rows2.append({
        'doi': doi,
        'cite_num_ingoing': len(ingoing_cit),
        'oci_ingoing': list(ingoing_oci)
        })
    time.sleep(1)

output_table2 = pd.DataFrame(rows2)


output = pd.merge(output_table1, output_table2, on='doi', how="outer")
output.to_csv("CitationCounts", index=False)

