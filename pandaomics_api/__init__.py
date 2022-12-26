import requests
from tqdm import tqdm
import pandas as pd
import json

def api_connect(login,passwd):
    session = requests.Session()
    session.auth = (login, passwd)
    return session


def get_project_metadata2json(project_id,param_dic):
    api_url = param_dic['api_url']
    session = api_connect(param_dic['login'],param_dic['passwd'])
    data = session.get(f'{api_url}/projects/{project_id}/overview/')
    if data.ok:
        return data.json()
    else:
        return print('wrong')

def get_projects_metadata2df(project_id_list,param_dic):
    global project_metadata
    global metadata_df
    metadata_df = []
    for project_id in tqdm(project_id_list):
        try:
            meta_json = get_project_metadata2json(project_id,param_dic)
            project_metadata = pd.json_normalize(meta_json)
            project_metadata_col = ['id', 'name','disease_name', 'efo_disease_id', 'disease_ontology_id', 'overview_status', 'omics_scores_status','team_name','user']
            project_metadata = project_metadata[project_metadata_col]
            comparison_id_list = list(meta_json['experiments'].keys())
            comparison_metadata_list = list()
            for comparison_id in comparison_id_list:
                comparison_metadata = pd.json_normalize(meta_json['experiments'][comparison_id])
                comparison_metadata = comparison_metadata.rename(columns={"id":"comparison_id"})
                comparison_metadata['project_id'] = project_id
                comparison_metadata_list.append(comparison_metadata)
            comparison_metadata_df = pd.concat(comparison_metadata_list,axis=0)
            comparison_metadata_df = pd.merge(left=project_metadata,right= comparison_metadata_df,left_on="id",right_on="project_id")
            metadata_df.append(comparison_metadata_df)
        except:
            pass
    metadata_df = pd.concat(metadata_df,axis=0)
    metadata_df = metadata_df.drop(columns=['project__name','project_id'])
    metadata_df = metadata_df.rename(columns={"id":"project_id","name":"project_name"})
    return metadata_df
