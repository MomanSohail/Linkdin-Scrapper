import os
import csv
from bs4 import BeautifulSoup


def extract_url_from_xml(file_name,testing):

    urls_list=[]

    url_soup = BeautifulSoup(file_name,"xml")
    sitemapTags = url_soup.find_all("url")
    
    if testing:
        print ("The number of sitemaps are {0}".format(len(sitemapTags)))

    for url in sitemapTags:
        urls_list.append(url.findNext("loc").text)

    return urls_list


def write_output_csv(file_name,site_maps):

    header = ["Company_Name","Url"]
    data=[]

    for company in site_maps.keys():
        for url in site_maps[company]:
            data.append([company,url])

    with open(file_name, 'w+', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)


def read_input_file(file_name):
    with open(file_name, 'r') as f:
        file = f.read()
        return file


def getFilesAndFolders():
    current_working_directory = os.getcwd()
    input_folder_name = os.path.join(current_working_directory,"Output","SiteMaps")
    output_folder_name=os.path.join(current_working_directory,"Output","SiteMapsCSV")

    if not os.path.exists(output_folder_name):
        os.mkdir(output_folder_name)

    return input_folder_name,output_folder_name


def getThreshold(testing):

    counter=0
    for file in os.listdir():
        if file.endswith(".xml"):
            counter+=1

    threshold=1000
    if testing:
        threshold=counter-1
    
    return threshold



if __name__ == "__main__":

    input_folder_name,output_folder_name=getFilesAndFolders()
    os.chdir(input_folder_name)
    
    testing=True
    counter=0
    number=0
    threshold=getThreshold(testing)

    site_maps={}
    for file in os.listdir():
        if file.endswith(".xml"):
            counter+=1

            if counter>threshold:
                number+=1
                output_file_name=os.path.join(output_folder_name,f"input_data_extractor{number}.csv")
                write_output_csv(output_file_name,site_maps)
                counter=0
                site_maps={}
            else:
                if testing:
                    print(file)
                input_file_name=os.path.join(input_folder_name,file)
                crawled_urls=extract_url_from_xml(read_input_file(input_file_name),True)
                company_name=file.replace('.xml','')
                site_maps[company_name]=crawled_urls
