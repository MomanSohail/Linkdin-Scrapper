import os
import pickle 
import difflib

#import DatabaseConnection
from collections import defaultdict
from sentence_transformers import SentenceTransformer, util

MODEL  = SentenceTransformer('all-MiniLM-L6-v2')


def readTitleEmbeddings():
    directory_path = os.getcwd()
    pickle_file_name=os.path.join(directory_path,"data","use_title_embeddings.pickle")

    with open(pickle_file_name, 'rb') as f:
        return pickle.load(f),f


def getEmbedding(strr):
    return MODEL.encode(strr, convert_to_tensor=True)


def getJobTitleTagId(id, jobtitle):
    
    title_embeddings,file = readTitleEmbeddings()
    file.close()

    try:
        title_vector = getEmbedding(jobtitle)

        ret = defaultdict(list)
        for embedding_title, data in title_embeddings.items():
            embedding_id=data[0]
            embedding_vector=data[1]
            similarity = util.cos_sim(title_vector, embedding_vector)
            ret[similarity].append([jobtitle, embedding_title,embedding_id])

        heighest = sorted(ret, reverse=True)[0]
        result = ret[heighest]

        title_tag_name=result[0][1]
        title_tag_id=result[0][2]

        return title_tag_id,title_tag_name

    except:
        return "N/A"

    
def map_title_jobtitletag(id,title):

    with DatabaseConnection.mariadb_connection() as connection:
        cur=connection.cursor()
        titletag_id,titletag_name=getJobTitleTagId(id,title)
        row_counter = 0

        if(titletag_id != 'N/A'):
            row_counter+=1
            # Insert in database
            sql = "Update castille_ecosystem.crawler_maintable set job_title_tag_id=%s where id= %s"
            val = (titletag_id, id)
            cur.execute(sql, val)
            connection.commit()
            row_counter += cur.rowcount



if __name__ == '__main__':

    testing=False

    if testing:

        id=111390
        title="Junior Back End Developer"

        title_tag_id,title_tag_name=getJobTitleTagId(id, title)

        print(f"Input Title::{title} ,Tagged Title::{title_tag_name} ,Title Id::{title_tag_id}")
   