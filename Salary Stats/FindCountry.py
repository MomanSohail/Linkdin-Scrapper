import pandas as pd
from tqdm import tqdm
from collections import defaultdict
from geopy.geocoders import Nominatim



def read_country_file(file_name):

    print(f"Reading Country File:{file_name}")
    
    df = pd.read_csv(file_name)
    countries_list= defaultdict(list)

    for data in tqdm(df.iterrows()):
        city = data[1]['name']
        state = data[1]['state_name']
        country = data[1]['country_name']
        countries_list[country].append('{}@{}'.format(city,state))
    
    return countries_list



def get_city_state_country(coord, geolocator):

    location = geolocator.reverse(coord, exactly_one=True)
    address = location.raw['address']
    country = address.get('country', None)
    
    return country



def get_country(countries_list,c):
    
    country = None

    if c=='Malta':
        return c

    for _country in (countries_list):
        for city in countries_list[_country]:
            _city=city.split('@')
            if _city[0] in c or _city[1] in c:
                country=_country
                return country
            
    if country is None or country == 'nan':
        print("Geolocator Started.")
        geolocator = Nominatim(user_agent="geoapiExercises", timeout=100)
        location = geolocator.geocode(c)
        try:
            country = get_city_state_country(str(location.latitude) + " , " + str(location.longitude),
                     geolocator=geolocator)
        except:
            print("Sorry!Country Not Found.")
            country = None

    return country