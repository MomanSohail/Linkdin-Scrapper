import numpy as np
from statistics import median, mean



def calculate_qs(l):

    CURRENCIES = {
        '$': 'Dollar',
        '€': 'Euro', 
        '£': 'Pound', 
        'CA$':'Canadian-Dollar', 
        'A$': 'AUS-Dollar', 
        'SGD': 'Singapore-Dollar',  
        'NZ':'New Zealand-Dollar', 
        'MYR': 'Malaysian-Ringet'
        }

    minilist = []
    maxilist = []
    avg_list = []

    for salary in l:

        if salary is None:
            continue
        
        minsal, maxsal = salary.split('-')

        for currency in CURRENCIES.keys():
        
            if minsal.startswith(currency):
                mini = float(minsal.replace(currency, ''))
                minilist.append(mini)
                maxi = float(maxsal.replace(currency, ''))
                maxilist.append(maxi)
                avg_Sal = (mini + maxi) / 2
                avg_list.append(avg_Sal)
                curr = currency
    
    try:
        q1=np.quantile(avg_list,.25,method='midpoint')
        q2=np.quantile(avg_list,.50,method='midpoint')
        q3=np.quantile(avg_list,.75,method='midpoint')
        q4  = q3 - q1
    
    except:
        return {}
    
    return {
        'q1': q1, 
        'q2': q2, 
        'q3': q3, 
        'q4': q4, 
        'min':min(avg_list),
        'max':max(avg_list), 
        'mean': mean(avg_list), 
        'median': median(avg_list), 
        'currency':CURRENCIES.get(curr, None)
        }