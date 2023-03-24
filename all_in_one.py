from placekey_script import *

csv_file = 'findhelp_icarol_site_v4.csv'

csv_file_test = 'findhelp_site_tester.csv'

api_key = 'vDFGLZgdWaV81xxZF1WAP8tz6FL04s4M'


def append_placekey_to_csv_automator(csv_file, api_key):
    pk_api = api_connection(api_key)
    unshortened_dictionaries = csv_to_dict(csv_file)
    desired_keys = ["id_num", "SiteNamePublic", "PhysicalAddress1", "PhysicalAddress2", "PhysicalCommunity", "PhysicalCity", "PhysicalCounty", "PhysicalStateProvince", "PhysicalPostalCode", "iso_country_code"]
    shortened_dicts = dictionary_shortener(desired_keys, unshortened_dictionaries)
    concatenate_address(shortened_dicts)
    null_free_shortened_dicts = null_cleaner(shortened_dicts) 
    short_df = dict_to_df(null_free_shortened_dicts) 

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

    df_for_api = short_df.rename(columns = column_map)
    cols = list(column_map.values())
    df_for_api = df_for_api[cols]
    
    df_json = dataframe_to_json(df_for_api)

    api_response = api_request(df_json) 

    df_placekeys = pd.read_json(json.dumps(api_response), dtype = {'query_id':str}) 

    placekey_dicts = df_to_dict(df_placekeys)

    for placekey_dict in placekey_dicts:
        for unshortened_dictionary in unshortened_dictionaries:
            if placekey_dict['query_id'] == unshortened_dictionary['id_num']:
                unshortened_dictionary['placekey'] = placekey_dict['placekey']
    
    final_df = dict_to_df(unshortened_dictionaries)

    return dataframe_to_csv(final_df, csv_file)  


append_placekey_to_csv_automator(csv_file, api_key)