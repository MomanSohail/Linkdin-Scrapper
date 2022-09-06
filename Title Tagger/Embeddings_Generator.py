import os
import pickle
import logging
import pandas as pd
from sentence_transformers import SentenceTransformer, util


def generateEmbeddings(job_title_list,job_id_list):

    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = {}
    excluded_embeddings = []

    for job in job_title_list:
        vector = model.encode(job, convert_to_tensor=True)
        if not all(vector):
            excluded_embeddings.append(job)
        embeddings[job] = [job_id_list[job],vector]

    return embeddings, excluded_embeddings


def saveToPickle(data_frame,file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(data_frame, f)


def getFilesAndFolders():

    directory_path = os.getcwd()
    input_file_name=os.path.join(directory_path,"input_embedding_generator.csv")
    input_column_name="job_title_tag_name"
    output_file_name=os.path.join(directory_path,"input_embeddings_job_title_tagger.pickle")

    return input_file_name,input_column_name,output_file_name



if __name__ == '__main__':

    testing=False

    input_file_name,input_column_name,output_file_name=getFilesAndFolders()
    
    df= pd.read_csv(input_file_name)
    df = df.dropna()

    unique_jobs_titles_list=df[input_column_name].unique()

    job_id_list={}
    for row in df.iterrows():
        job_id_list[row[1][input_column_name]]=row[1]['id']

    if testing:
        print(f'Number of Unique Titles:{len(unique_jobs_titles_list)}')
    
    unique_embeddings,excluded_embeddings=generateEmbeddings(unique_jobs_titles_list,job_id_list)
    
    saveToPickle(unique_embeddings,output_file_name)