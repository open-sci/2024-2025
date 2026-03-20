import pandas as pd
import pprint


df = pd.read_csv('data/mashup_IRIS_subset_v3.csv')
sh = df[df["src_repo"] == "software heritage"]

sh_sample = sh.sample(100, random_state=42)

columns = "title,id,doi,creators,orcid,date,description,resource_type,url,type,rights,publisher,relation,communities,swh_id,keywords,src_repo,issn,pmid,iris_cat"
sh_sample = sh_sample[columns.split(',')]
sh_sample = sh_sample[["title", "creators", "url"]]

sh_sample.to_csv('data/sh_sample.csv', index=False)

