import os
import pickle 
import difflib

import DatabaseConnection
from collections import defaultdict
from sentence_transformers import SentenceTransformer, util

MODEL  = SentenceTransformer('all-MiniLM-L6-v2')


def getJobTitleTagFuzzyWuzzy(id, jobtitle):
    from fuzzywuzzy import process

    with DatabaseConnection.mariadb_connection() as connection:

        cursor = connection.cursor()
        try:
            query = "Select id,job_title_tag_name from job_title_tag_category;"
            cursor.execute(query)
            mytags = cursor.fetchall()
        except:
            print("Error")
        mytitle_tag = []
        for a in mytags:
            mytitle_tag.append(a[1])

    ratios = process.extract(jobtitle, mytitle_tag)

    # search_param_title=DatabaseConnection.mariadb_connection.get_search_param_title(id)
    # ratios_2=process.extract(search_param_title, mytitle_tag)

    if(len(ratios)>0):

        suggested_title_tag=ratios[0][0]
        percent_similar = ratios[0][1]

        #50
        if(percent_similar<85):
            suggested_title_tag=None


    if(suggested_title_tag is not None):
        title_tag_id=[e[0] for e in mytags if e[1] == suggested_title_tag][0]

        return title_tag_id
    else:
        return "N/A"


def readTitleEmbeddings():
    directory_path = os.getcwd()
    pickle_file_name=os.path.join(directory_path,"data","use_title_embeddings.pickle")

    with open(pickle_file_name, 'rb') as f:
        return pickle.load(f),f


def getEmbedding(strr):
    return MODEL.encode(strr, convert_to_tensor=True)


def getJobTitleTagUSE(id, jobtitle):
    
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


def saveTitleTag(titletag_id,column_name):

    with DatabaseConnection.mariadb_connection() as connection:

        cur=connection.cursor()
        # cur.execute(query)
        # my_titles=cur.fetchall()
        row_counter = 0
        # for a in my_titles:
        #     id=a[0]
        #     title=a[1]
        #titletag_id=getJobTitleTagFuzzyWuzzy(id,title)

        if(titletag_id != 'N/A'):
            row_counter+=1
            # Insert in database
            sql = f"Update castille_ecosystem.crawler_maintable set {column_name}=%s where id= %s"
            val = (titletag_id, id)
            cur.execute(sql, val)
            connection.commit()
            row_counter += cur.rowcount


def map_title_jobtitletag(id,title):

    saveTitleTag(getJobTitleTagFuzzyWuzzy(id,title),'job_title_tag_id')

    titletag_id,titletag_name=getJobTitleTagUSE(id,title)
    saveTitleTag(titletag_id,'use_title_tag_id')



#if __name__ == '__main__':

    #testing=False

    #if testing:

        #id=111390
        #title="Junior Back End Developer"

        #title_tag_id,title_tag_name=getJobTitleTagId(id, title)

        #print(f"Input Title::{title} ,Tagged Title::{title_tag_name} ,Title Id::{title_tag_id}")
   