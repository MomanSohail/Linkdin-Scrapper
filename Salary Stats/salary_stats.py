import csv
import time
import pickle
import pandas as pd
import numpy as np
import json

from tqdm import tqdm
from collections import defaultdict
from statistics import median, mean
from forex_python.converter import CurrencyRates
from currency_converter import CurrencyConverter

from CalculateSalaries import salary_fixer
from FindCountry import get_country,read_country_file
from CalculateQs import calculate_qs



def read_input_file(file_name):

    print(f"Reading Input file:{file_name}")
    return pd.read_csv(file_name)



def calculate_stats(country_file_name,asian_country_file_name,df):

    print("Converting and Fixing Salaries.Please Wait!")
   
    c = CurrencyRates()
   
    currency_rates={
        "A$":c.get_rates('AUD'),
        "CA$":c.get_rates('CAD'),
        "$":c.get_rates('USD'),
        "£":c.get_rates('GBP'),
        "€":c.get_rates('EUR')
    }

    empty_countries=[]
    countries_list=read_country_file(country_file_name)

    df_countries=pd.read_csv(asian_country_file_name)
    asian_countries=[]
    for country in df_countries["Country"]:
        asian_countries.append(country)
    
    ret = defaultdict(lambda: defaultdict(lambda : defaultdict(lambda: defaultdict(list))))

    print("Finding Countries.Please Wait!")
    
    counter=0
    for row in tqdm((df.iterrows())):
    
        counter+=1
        #if counter>100:
            #return ret
    
        l = get_country(countries_list,row[1]['location'])
        if l is None or l == float("nan"):
            print(row[1]['location'])
            empty_countries.append(row[1]['location'])
    
        if not l:
            continue

        salary = salary_fixer(l, row[1]['salary'],asian_countries,currency_rates)
        c = row[1]['category_name']
        if row[1]['seniority_name']==float('nan') or row[1]['seniority_name']=='nan':
            print(type(row[1]['seniority_name']))
        senior = row[1]['seniority_name']
        title = row[1]['job_title_tag_name']    
        ret[l][c][title][senior].append(salary)
    
    return ret



def write_output_file(file_name,ret):

    print(f"Writing output file:{file_name}")
    header = [
        "Country", 
        "Talent Vertical", 
        "Job Title Tag", 
        "count of Job", 
        "Seniority Tag", 
        "Currency", 
        "Salaries",
        "Top 3 Benefits", 
        "Median", 
        "Mean", 
        "Q1", 
        "Q2", 
        "Q3", 
        "Q4", 
        "Mean Y-O-Y%",          
        "Mean YTD%",
        "Index per Talent Vertical for a specific year for a specific country"
        ]
    
    with open(file_name, 'w',  encoding='UTF8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for country, tv in ret.items():
            for title, senior in tv.items():
                for k, v in senior.items():
                    for i, j in v.items():
                        if i !=float('nan') or i !=None:
                            o = calculate_qs(j)
                            sal = '{}-{}'.format(o.get('min', None), o.get('max', None))
                            row = [country, title, k, len(v), i, o.get('currency',None) , sal,  0, o.get('median',None),o.get('mean',None),o.get('q1',None),o.get('q2',None),o.get('q3',None),o.get('q4',None), 0 , 0 ]
                            writer.writerow(row)



if __name__ == "__main__":

    file_names={
        "input":'Dataset/salary_stats_raw.csv',
        "output":'Output/salary_stats_output.csv',
        "country":'Dataset/input_countries.csv',
        "asian_country":"Dataset/input_asian_countries.csv",
    }

    start_time=time.perf_counter()

    df = read_input_file(file_names["input"])
    df=df.dropna()

    ret=calculate_stats(file_names["country"],file_names["asian_country"],df)

    write_output_file(file_names["output"],ret)

    end_time=time.perf_counter()

    print("Time For Calculation:", end_time - start_time)




