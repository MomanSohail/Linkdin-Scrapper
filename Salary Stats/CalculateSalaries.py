from tqdm import tqdm
from forex_python.converter import CurrencyRates



def convert_salary(salary):
 
    WORKING_DAYS = 230
    HOURS = 8
    MONTHS = 12
    PER_HOUR_THRESHOLD = 250
    CURRENCIES = [
        '$', 
        '€', 
        '£', 
        'CA$', 
        'A$', 
        'SGD', 
        'NZ', 
        'MYR']
    
    mini, maxi = salary.replace(',', '').split('-')
    
    for currency in CURRENCIES:
        
        if currency=="£":
            if mini.startswith("Â£"):
                mini=mini.replace("Â£", '£')
                maxi=maxi.replace("Â£", '£')
        
        if mini.startswith(currency):
            mini = float(mini.replace(currency, ''))
            maxi = float(maxi.replace(currency, ''))
            if mini <= 250.0 and maxi<=250.0:
                mini = mini * HOURS * WORKING_DAYS
                maxi = maxi * HOURS * WORKING_DAYS    
            if (mini >= 250.0 and mini <= 15000.0) and (maxi >= 250.0 and maxi <= 15000.0):
                mini = mini * MONTHS
                maxi = maxi * MONTHS  
            
            return '{} - {}'.format(int(mini),int(maxi)), currency
    
    return None



def get_new_salary(mini,maxi,currency,rate):

    mini = float(mini) * float(rate)
    maxi = float(maxi) * float(rate)            
    
    return '{}{} - {}{}'.format(currency, int(mini), currency, int(maxi))



def get_condition(condition,country,asian_countries=None):
    
    countries=None

    if condition=='Europe':
        countries=[
            'Europe',
            'Swedan',
            'Netherlands',
            'Ireland',
            'Spain',
            'Austria',
            'Germany',
            'Italy',
            'Malta'
            ]
    
    elif condition=='United States':
        countries=[
            'United States',
            'Singapore',
            'Argentina',
            'Honduras',
            'Mexico',
            'Brazil'
            ]
    
    if countries:

        if condition=='United States':
            for asian_country in asian_countries:
                if asian_country==country:
                    return True

        for c in countries:
            if c==country:
                return True

    return False




def salary_fixer(country, salary,asian_countries,currency_rates):

    try:

        converted_salary, currency = convert_salary(salary)
        rates=currency_rates[currency]
        mini, maxi = converted_salary.split('-')
    
        if country=="Australia":
            if currency=="A$":
                new_salary = '{}{} - {}{}'.format('A$', int(mini), 'A$', int(maxi))
            else:   
                new_salary=get_new_salary(mini,maxi,'A$',rates['AUD'])
        
        elif country=="Canada":
            if currency=="CA$":
                new_salary = '{}{} - {}{}'.format('CA$', int(mini), 'CA$', int(maxi))
            else:
                new_salary=get_new_salary(mini,maxi,'CA$',rates['CAD'])
            
        elif country=="United Kingdom":
            if currency=="£":
                new_salary = '{}{} - {}{}'.format('£', int(mini), '£', int(maxi))
            else:
                new_salary=get_new_salary(mini,maxi,'£',rates['GBP'])
        
        elif get_condition('Europe',country):
            new_salary=None
            if currency=="€":
                new_salary = '{}{} - {}{}'.format('€', int(mini), '€', int(maxi))
            else:
                new_salary=get_new_salary(mini,maxi,'€',rates['EUR'])
                
            if new_salary==None:
                print("None")

        elif get_condition('United States',country,asian_countries):
            new_salary=None
            if currency=="$":
                new_salary = '{}{} - {}{}'.format('$', int(mini), '$', int(maxi))
            else:
                new_salary=get_new_salary(mini,maxi,'$',rates['USD'])
    
            if new_salary==None:
                print("None")

        else:        
            new_salary=None
            if currency=="$":
                new_salary = '{}{} - {}{}'.format('$', int(mini), '$', int(maxi))
            else:
                new_salary=get_new_salary(mini,maxi,'$',rates['USD'])
            
    except Exception as err:
        new_salary=None
   
    return new_salary