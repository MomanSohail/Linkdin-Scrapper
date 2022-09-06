import os 
import csv
import pickle
import logging
import pandas as pd

from math import nan, isnan
from collections import defaultdict
from sentence_transformers import SentenceTransformer, util

MODEL  = SentenceTransformer('all-MiniLM-L6-v2')


def readPickle(file_name):
    with open(file_name, 'rb') as f:
        return pickle.load(f)


def getEmbedding(strr):
    return MODEL.encode(strr, convert_to_tensor=True)


def getBatch(li):
    for index in range(0, len(li), 500):
        batch = li[index: index + 500]
        yield batch


def getFilesAndFolders():

    directory_path = os.getcwd()
    input_file_name=os.path.join(directory_path,"input_job_title_tagger.xlsx")
    input_column_name='# title'
    pickle_file_name=os.path.join(directory_path,"input_embeddings_job_title_tagger.pickle")

    output_folder_name=os.path.join(directory_path, "Title Tagger Output")
    if not os.path.exists(output_folder_name):
        os.mkdir(output_folder_name)
    
    df =  pd.read_excel(input_file_name)
    titles = df[input_column_name]
    pickle_embeddings = readPickle(pickle_file_name)
    
    return df,titles,pickle_embeddings,output_folder_name


def getOutputFile(output_folder_name,i):
    output_file_name=os.path.join(output_folder_name,f"job_title_tagger_batch{i}.txt")
    return open(output_file_name, 'a+')


def getSimilarity(title,pickle_embeddings):

    title_embedding = getEmbedding(title)
    
    ret = defaultdict(list)
    for embedding_title, data in pickle_embeddings.items():
        _id=data[0]
        pickle_embedding=data[1]
        similarity = util.cos_sim(title_embedding, pickle_embedding)
        ret[similarity].append([_id,title, embedding_title])

    t = sorted(ret, reverse=True)[0]
    
    return ret[t],t



##########################
if __name__ == '__main__':

    df,titles,pickle_embeddings,output_folder_name=getFilesAndFolders()

    for i, title_batch in enumerate(getBatch(titles)):

        f=getOutputFile(output_folder_name,i)
        writer = csv.writer(f)

        testing=False
        counter=0

        for title in title_batch:

            counter+=1
            if counter>200 and testing:
                break
            
            try:
                result,similarity=getSimilarity(title,pickle_embeddings)

                output = f'{str(result[0][0])},{str(result[0][1])},{result[0][2]},{round(float(similarity),3)}\n'

                if testing:
                    print(f'Batch:{i}:::ID:{str(result[0][0])},Title:{str(result[0][1])},Embedding_Title:{result[0][2]},Similarity:{round(float(similarity),3)}\n')
                
                f.write(output)

            except:
                continue
