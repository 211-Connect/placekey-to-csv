import pandas as pd 
import placekey as pk
from placekey.api import PlacekeyAPI
import json

placekey_api_key = 'vDFGLZgdWaV81xxZF1WAP8tz6FL04s4M'

def api_connection(api_key):
    pk_api = PlacekeyAPI(api_key)
    return pk_api

pk_api = api_connection(placekey_api_key)

def csv_to_dict(table):
    tabledir = f'./{table}'
    df = pd.read_csv (tabledir, dtype = {'PhysicalPostalCode':str}, low_memory = False)
    id_range = []
    for x in range(df.shape[0]):
        id_range.append(f'{x}')
    df['id_num'] = id_range
    df['iso_country_code'] = 'US'
    places = df.to_dict('records')
    return places

#Created a tester csv because the 9k entries on the main one was taking forever to run

# unshortened_dictionaries = csv_to_dict('findhelp_icarol_site_v4.csv') 

#Raw dictionaries from converted csv
 
# unshortened_dictionaries = csv_to_dict('findhelp_site_tester.csv') 



#limiting the dicts to only have relevant keys
desired_keys = ["id_num", "SiteNamePublic", "PhysicalAddress1", "PhysicalAddress2", "PhysicalCommunity", "PhysicalCity", "PhysicalCounty", "PhysicalStateProvince", "PhysicalPostalCode", "iso_country_code"]

def dictionary_shortener(list_of_keys, dictionaries):
    return [{key: dictionary[key] for key in list_of_keys} for dictionary in dictionaries]
    

    
##shortened_dicts = dictionary_shortener(desired_keys, unshortened_dictionaries)

# shortened_dicts = [{desired_key: dictionary[desired_key] for desired_key in desired_keys} for dictionary in unshortened_dictionaries]

#Creating a new key that contains the final form of the address we want to put into placekey
#checks if either address line 1 or 2 are nulls and concats them if they are not, if only line 2 is null it puts line 1 into the concat column for easier processing


def concatenate_address(shortened_dicts):
    for shortened_dict in shortened_dicts: 
        if isinstance(shortened_dict["PhysicalAddress1"], float) == False and isinstance(shortened_dict["PhysicalAddress2"], float) == False:
            concat_address = f'{shortened_dict["PhysicalAddress1"]}, {shortened_dict["PhysicalAddress2"]}' 
            shortened_dict['concat_address'] = concat_address

        elif isinstance(shortened_dict["PhysicalAddress1"], float) == False:
            shortened_dict['concat_address'] = shortened_dict["PhysicalAddress1"]
    return shortened_dicts

##concatenate_address(shortened_dicts)
# print(shortened_dicts)


#Clears out entries that have nulls for all fields

def null_cleaner(shortened_dicts):
    addresses = [] 
    for dict in shortened_dicts:
        null_free_dict = {}
        for key in dict:
            if isinstance(dict[key], float) == False:
                null_free_dict[key] = dict[key]
            else: 
                pass
        if len(null_free_dict) >2:
            addresses.append(null_free_dict)
    return addresses 

##null_free_shortened_dicts = null_cleaner(shortened_dicts) 
# print(addresses)

#after cleaning up dicts this will turn them into clean dataframes
def dict_to_df(dict):
    shortened_df = pd.DataFrame(dict)
    return shortened_df

##short_df = dict_to_df(null_free_shortened_dicts) 

# print(short_df)

#mapping the names from the excel sheet to names that are readable to placekey API

query_id_col = 'id_num' 


column_map = {query_id_col: 'query_id',
                'SiteNamePublic': 'location_name',
                'concat_address': 'street_address',
                'PhysicalCity': 'city',
                'PhysicalStateProvince': 'region',
                'PhysicalPostalCode': 'postal_code',
                'iso_country_code': 'iso_country_code'
                #'LAT': 'latitude'
                # 'LON': 'longitude'
                }

#Renaming the columns before feeding df to api 

##df_for_api = short_df.rename(columns = column_map)
##cols = list(column_map.values())
##df_for_api = df_for_api[cols]
    


# print(df_for_api)
# print(list(addresses[0].keys())) 

#Must convert to json before feeding to api
def dataframe_to_json(dataframe):
    dataframe = json.loads(dataframe.to_json(orient = "records")) 
    return dataframe

##df_json = dataframe_to_json(df_for_api)

# print(df_json)


def api_request(input_json):
   responses = pk_api.lookup_placekeys(input_json, 
                                   strict_address_match= False,
                                   strict_name_match= False
                                   )
   return responses

##api_response = api_request(df_json)


# print(api_response) 

#new dataframe composed of api responses 
##df_placekeys = pd.read_json(json.dumps(api_response), dtype = {'query_id':str}) 

#converting df back to dict to add in the new placekeys
def df_to_dict(dataframe):
    return dataframe.to_dict('records')

##placekey_dicts = df_to_dict(df_placekeys)
# placekey_dicts = df_placekeys.to_dict('records')

# print(placekey_dicts[:6])

#matching up the placekeys to their original records via id num and adding a new placekey key to dict


##for placekey_dict in placekey_dicts:
    for unshortened_dictionary in unshortened_dictionaries:
       if placekey_dict['query_id'] == unshortened_dictionary['id_num']:
            unshortened_dictionary['placekey'] = placekey_dict['placekey']

# print(unshortened_dictionaries[0])

        
# print(df_for_api)
# print(df_placekeys)

#finally sending everything back to the original unshortened dict 
##final_df = dict_to_df(unshortened_dictionaries)

# print(final_df)

#return unshortened dict back to csv with the placekey column added 

def dataframe_to_csv(dataframe, file_name):
   return dataframe.to_csv(f'./{file_name[:-4]}_placekey.csv')



##dataframe_to_csv(final_df, 'findhelp_tester') 

