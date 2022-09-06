import os
import time
import threading
import pandas as pd
import email.utils as eut
from lxml import etree
from pprint import pprint

from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from urllib.request import Request
from urllib.request import HTTPError
from lxml.html.soupparser import fromstring



####################Global Variables##################
types = "text/html"

# adjust to your liking
MaxThreads = 200
MaxSubThreads = 100

InitialURL = ""
InitialURLInfo = ""
InitialURLLen = ""
InitialURLNetloc = ""
InitialURLScheme = ""
InitialURLBase = ""
netloc_prefix_len = ""
netloc_prefix_str = "www."

run_ini = None
run_end = None
run_dif = None

request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://coozila.com",
    "Connection": "keep-alive",
}
##################Global Variables##################



class Data():
    def __init__(self):
        self.queue = []
        self.checked = []
        self.threads = []
        self.link_threads = []



class RunCrawler(threading.Thread):

    # crawler start
    run_ini = time.time()
    run_end = None
    run_dif = None


    def __init__(self, url,output_file_name,v):
        threading.Thread.__init__(self)

        self.output_file_name=output_file_name
        self.v=v

        ProcessURL(url,None,None,self.v)
        self.start()


    def run(self):
        run = True
        counter=0

        while run:

            for index, thread in enumerate(self.v.threads):
                if thread.is_alive() is False:
                    del self.v.threads[index]

            for index, thread in enumerate(self.v.link_threads):
                if thread.is_alive() is False:
                    del self.v.link_threads[index]

            for index, obj in enumerate(self.v.queue):
                if len(self.v.threads) < MaxThreads:
                    thread = Crawl(index, obj,self.v)
                    self.v.threads.append(thread)
                    del self.v.queue[index]
                else:
                    break

            ####################################################
            if len(self.v.queue)==0:
                counter+=1

            if counter>100:
                self.done()
                run=False
            ####################################################
            if len(self.v.queue) == 0 and len(self.v.threads) == 0 and len(self.v.link_threads) == 0:
                run = False
                self.done()
            else:
                print(
                    "Threads: ",
                    len(self.v.threads),
                    " Queue: ",
                    len(self.v.queue),
                    " Checked: ",
                    len(self.v.checked),
                    " Link Threads: ",
                    len(self.v.link_threads),
                )

                time.sleep(1)


    def done(self):
        print("Checked: ", len(self.v.checked))
        print("Running XML Generator...")

        # Running sitemap-generating script
        Sitemap(self.output_file_name,self.v)

        self.run_end = time.time()
        self.run_dif = self.run_end - self.run_ini

        print(self.run_dif)



class Sitemap:

    urlset = None
    encoding = "UTF-8"
    xmlns = "http://www.sitemaps.org/schemas/sitemap/0.9"


    def __init__(self,output_file_name,v):
        self.root()
        self.output_file_name=output_file_name
        self.v=v
        self.children()
        self.xml()


    def done(self):
        print("Done")


    def root(self):
        self.urlset = etree.Element("urlset")
        self.urlset.attrib["xmlns"] = self.xmlns


    def children(self):
        for index, obj in enumerate(self.v.checked):
            url = etree.Element("url")
            loc = etree.Element("loc")
            lastmod = etree.Element("lastmod")
            changefreq = etree.Element("changefreq")
            priority = etree.Element("priority")

            loc.text = obj["url"]
            lastmod_info = None
            lastmod_header = None
            lastmod.text = None

            if hasattr(obj["obj"], "info"):
                lastmod_info = obj["obj"].info()
                lastmod_header = lastmod_info["Last-Modified"]
            # check if 'Last-Modified' header exists
            if lastmod_header is not None:
                lastmod.text = FormatDate(lastmod_header)
            if loc.text is not None:
                url.append(loc)
            if lastmod.text is not None:
                url.append(lastmod)
            if changefreq.text is not None:
                url.append(changefreq)
            if priority.text is not None:
                url.append(priority)

            self.urlset.append(url)


    def xml(self):
        f = open(self.output_file_name, "w")

        print(
            etree.tostring(
                self.urlset, pretty_print=True, encoding="unicode", method="xml"
            ),
            file=f,
        )

        f.close()
        print("Sitemap saved in: ", self.output_file_name)



class Crawl(threading.Thread):


    def __init__(self, index, obj,v):
        threading.Thread.__init__(self)

        self.index = index
        self.v=v
        self.obj = obj
        self.start()


    def run(self):
        temp_status = None
        temp_object = None

        try:
            print(self.obj["url"])
            temp_req = Request(self.obj["url"], headers=request_headers)

            try:
                temp_res = urlopen(temp_req)
                temp_code = temp_res.getcode()
                temp_type = temp_res.info()["Content-Type"]
                temp_status = temp_res.getcode()
                temp_object = temp_res

                if temp_code == 200:

                    if types in temp_type:
                        temp_content = temp_res.read()
                        # var_dump(temp_content)
                        temp_data = fromstring(temp_content)

                        temp_thread = threading.Thread(
                            target=ParseThread, args=(self.obj["url"], temp_data,self.v)
                        )

                        self.v.link_threads.append(temp_thread)
                        temp_thread.start()
            except:
                xl=1
        except HTTPError as e:
            temp_status = e.code
            pass

        self.obj["obj"] = temp_object
        self.obj["sta"] = temp_status

        ProcessChecked(self.obj,self.v)



def dump(obj):
    """return a printable representation of an object for debugging"""
    newobj = obj

    if "__dict__" in dir(obj):
        newobj = obj.__dict__

        if " object at " in str(obj) and "__type__" not in newobj:
            newobj["__type__"] = str(obj)
            for attr in newobj:
                newobj[attr] = dump(newobj[attr])

    return newobj


def FormatDate(datetime):
    datearr = eut.parsedate(datetime)
    date = None

    try:
        year = str(datearr[0])
        month = str(datearr[1])
        day = str(datearr[2])

        if int(month) < 10:
            month = "0" + month
        if int(day) < 10:
            day = "0" + day

        date = year + "-" + month + "-" + day
    except IndexError:
        pprint(datearr)

    return date


def ParseThread(url, data,v):
    temp_links = data.xpath("//a")

    for temp_index, temp_link in enumerate(temp_links):
        temp_attrs = temp_link.attrib

        if "href" in temp_attrs:
            temp_url = temp_attrs.get("href")
            temp_src = url
            # temp_value = temp_link.text
            temp_url = temp_attrs.get("href")

            path = JoinURL(temp_src, temp_url)

            # var_dump(path)
            exclude_list = [
                "/admin/",
                "/images/",
                "/cache/",
                "/includes/",
                "/templates/",
            ]

            if (path is not False) and not any(map(path.__contains__, exclude_list)):
                ProcessURL(path, temp_src,None,v)


def JoinURL(src, url):
    value = False

    url_info = urlparse(url)
    src_info = urlparse(src)
    # url_scheme = url_info.scheme
    # src_scheme = src_info.scheme
    url_netloc = url_info.netloc
    src_netloc = src_info.netloc

    if src_netloc.startswith(netloc_prefix_str):
        src_netloc = src_netloc[netloc_prefix_len:]
    if url_netloc.startswith(netloc_prefix_str):
        url_netloc = url_netloc[netloc_prefix_len:]
    if url_netloc == "" or url_netloc == InitialURLNetloc:
        url_path = url_info.path
        src_path = src_info.path
        src_new_path = urljoin(InitialURLBase, src_path)
        url_new_path = urljoin(src_new_path, url_path)
        path = urljoin(src_new_path, url_new_path)
        # print path
        value = path

    return value


def ProcessURL(url, src=None, obj=None,v=None):
    found = False

    for value in v.queue:
        if value["url"] == url:
            found = True
            break

    for value in v.checked:
        if value["url"] == url:
            found = True
            break

    if found is False:
        temp = {}
        temp["url"] = url
        temp["src"] = src
        temp["obj"] = obj
        temp["sta"] = None
        v.queue.append(temp)


def ProcessChecked(obj,v):
    found = False

    for item in v.checked:
        if item["url"] == obj["url"]:
            found = True
            break

    if found is False:
        v.checked.append(obj)


########################################

def getFilesAndFolders():
    directory_path = os.getcwd()
    input_file_name=os.path.join(directory_path,'input_site_maps_generator.csv')
    output_folder_name=os.path.join(directory_path, "Output")

    if not os.path.exists(output_folder_name):
        os.mkdir(output_folder_name)
    output_folder_name=os.path.join(output_folder_name, "SiteMaps 1.0")
    if not os.path.exists(output_folder_name):
        os.mkdir(output_folder_name)

    return input_file_name,output_folder_name


if __name__ == "__main__":

    input_file_name,output_folder_name=getFilesAndFolders()

    df = pd.read_csv(input_file_name)
    df=df.dropna()

    testing=False
    counter=0
    for company in df.iterrows():

        counter+=1
        if testing and counter>300:
            break

        InitialURL = company[1]['company_url']
        InitialURLInfo = urlparse(InitialURL)
        InitialURLLen = len(InitialURL.split("/"))
        InitialURLNetloc = InitialURLInfo.netloc
        InitialURLScheme = InitialURLInfo.scheme
        InitialURLBase = InitialURLScheme + "://" + InitialURLNetloc
        netloc_prefix_len = len(netloc_prefix_str)

        if InitialURLNetloc.startswith(netloc_prefix_str):
            InitialURLNetloc = InitialURLNetloc[netloc_prefix_len:]
        if testing:
            print(f"XML Generator started for:{counter} {company[1]['company_url']}")

        output_file_name=os.path.join(output_folder_name,f"{company[1]['company_name']}.xml")

        RunCrawler(InitialURL,output_file_name,Data())
