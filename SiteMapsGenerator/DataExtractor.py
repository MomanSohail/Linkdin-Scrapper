import os
import csv
import threading
import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen


def get_text_from_url(url_link):

    try:
        url=Request(url_link, headers={'User-Agent': 'Mozilla/5.0'})
        html_page = urlopen(url,timeout=5).read()

        soup = BeautifulSoup(html_page, "html.parser")
        html_text = soup.get_text()

        text_list=[]

        for line in html_text:
            text_list.append(line)

        return str(html_text).strip()

    except:
        return "Sorry!Text Not Found."


def write_output_csv(file_name,data):

    header = ["Company_Name","Company_Url","Text"]

    with open(file_name, 'w+', encoding='UTF8', newline='') as file:
        try:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(data)
        except:
            print(f"Error Saving:{file_name}")


def get_batches(number_of_batches,df):

    batch_size=int(len(df)/number_of_batches)
    
    batch=[]
    start=batch_size
    end=batch_size+batch_size

    batch.append(df.iloc[:batch_size])
    
    for i in range(number_of_batches-2):
        batch.append(df.iloc[start:end])
        start=end
        end=end+batch_size
    
    batch.append(df.iloc[end:])
    return batch


def get_output_folder_name(output_folder_name,i):

    output_folder_name=os.path.join(output_folder_name,f'Batch {i}')
    
    if not os.path.isdir (output_folder_name):
        os.mkdir (output_folder_name)
    
    return output_folder_name


def process_batch(output_file_name,i,df,testing):

    if not os.path.isdir(output_file_name):
        
        raw_data_set=[]

        for company in df.iterrows():
           
            url_text=get_text_from_url(company[1]['Url'])

            if url_text!=None:
                raw_data_set.append([company[1]['Company_Name'],company[1]['Url'],url_text])
            if testing:
                print(f"Trying Batch {i} Url:{company[1]['Url']}")
        write_output_csv(output_file_name,raw_data_set)
        
        print(f'Batch {i} | {output_file_name} saved.')


def getFilesAndFolders(testing):

    current_working_directory = os.getcwd()

    test_input_folder_name=None
    if testing:
        test_input_folder_name=os.path.join(current_working_directory,"Output","SiteMapsCSV","input_data_extractor1.csv")
    input_folder_name = os.path.join(current_working_directory,"Output","SiteMapsCSV")
    output_folder_name=os.path.join(current_working_directory,"Output","DataExtractor")
    if not os.path.exists(output_folder_name):
        os.mkdir(output_folder_name)

    number_of_batches=1000

    return test_input_folder_name,input_folder_name,output_folder_name,number_of_batches 



if __name__ == '__main__':

    testing=False
    testing1=True

    test_input_folder_name,input_folder_name,output_folder_name,number_of_batches=getFilesAndFolders(testing)
    
    os.chdir(input_folder_name)

    for i,file in enumerate(os.listdir()):

        if test_input_folder_name !=None:
            file=test_input_folder_name

        if file.endswith(".csv"):
            if testing:
                print(f"Started Working on File:{file}")
            
            df = pd.read_csv(file)
            df=df.dropna()

            output_folder_number=get_output_folder_name(output_folder_name,i)
            if testing:
                print(f"Converting DF into {number_of_batches} batches.")
            
            df_batches=get_batches(number_of_batches,df)
            if testing:
                print(f'Batch size:{len(df_batches[0])}')

            threads=[]

            for i,df in enumerate(df_batches):
                output_file_name=os.path.join(output_folder_number,f'data_extractor_output{i}.csv')
                threads.append(threading.Thread(target=process_batch, args=(output_file_name,i,df,testing1,)))
                
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
        
        if test_input_folder_name !=None:
            break
            
