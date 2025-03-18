
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time, certifi, os, shutil
from dotenv import load_dotenv


class DataHandler:
    def __init__(self):
        load_dotenv()
        # ESTABLISH CONNECTION TO MONGODB
        try:
            connect_str = os.environ['PARTNERSDB_URI']
            # certifi.where() bit has to do with SSL Certs
            # https://www.mongodb.com/community/forums/t/serverselectiontimeouterror-ssl-certificate-verify-failed-trying-to-understand-the-origin-of-the-problem/115288
            self.client = MongoClient(connect_str, tlsCAFile=certifi.where())
        except Exception as e:
            print('Error connecting to mongo client.')
            print(f'Error: \n{e}')
            exit(1)
        
        # Create MongoDB collections if needed
        self.db = self.client['costagg']
        self.new_raw_data = self.db['new_raw_data']

        # Load data info and create dictionaries for mapping labels and feature types
        self.data_info = pd.read_csv('costar/input/data_info.csv', header=0)
        self.orig_labels = self.data_info['orig_labels'].values
        self.db_labels = self.data_info['db_labels'].values

        self.label_map = dict(zip(self.orig_labels, self.db_labels))
        self.feature_types = dict(zip(self.db_labels, self.data_info['feature_types'].values))


    def download_log(self, log_message):
        with open('costar/logs/download.log', 'a') as f:
            f.write(log_message+'\n')

    ########################################################################################################################
    def Write_Raw_Data_To_Mongo(self, saved_search):
        data_dict_list = []
        props_added = set()

        present_data = pd.read_excel(f'costar/data/{saved_search}/{saved_search}.xlsx', engine='openpyxl')
        # Update all NaN values to '' in 'Property Name' column
        present_data['Property Name'].fillna('', inplace=True)
        # Ensure that all 'PropertyID' values are integer strings
        present_data['PropertyID'] = present_data['PropertyID'].apply(lambda x: str(int(x)))

        prop_log = pd.read_csv(f'costar/logs/prop_log/{saved_search}.csv', header=0)
        for prop_log_idx, prop in prop_log[prop_log['Complete'] == True].iterrows():
            # Can't go by idx, because can't assume the prop_log and present_data have same indexing.
            # Have to go by combination of address and building name    
            address = prop['Address']
            building = prop['Building']
            prop_id = str(int(prop['ID']))
            # Check if address is null. This could be result of an error in prop_log, where a Complete = True value is assigned to a row with no other data. 
            if pd.isnull(address):
                if pd.isnull(building) & pd.isnull(prop['ID']):
                    continue
            # Building in prop_log will be NaN if no building name, but present_data will have '' if no building name
            if pd.isnull(building): building = ''
            # Check if this property has already been added (sometimes CoStagg can download the same property twice)
            if (address, building) in props_added:
                continue
            # Find the index within the present_data dataframe that corresponds to the address & building name in prop_log
            present_data_idx = present_data[present_data['PropertyID'] == prop_id].index
            if len(present_data_idx) != 1:
                self.download_log(f'DATA INCONGRUITY FOUND: {saved_search}  --  {address} {building} is either not unique or non-existent in present data.')
                continue
            else:
                present_data_idx = present_data_idx[0]
            
            # prop_id ends up being read as a float, so convert to string and remove decimal if it exists
            prop_id = str(prop['ID'])
            if "." in prop_id:
                prop_id = prop_id.split('.')[0]

            # MOST PROBLEMS THAT ARISE IN THIS CLASS ARE DUE TO READING EXCEL FILES. 
            try:
                prop_hist_data = pd.read_excel(f'costar/data/{saved_search}/{prop_id}.xlsx', engine='openpyxl')
            except Exception as e:
                self.download_log(f'ERROR READING HISTORICAL DATA FOR {address}, {building}')
                self.download_log(f'ERROR: \n{e}')
                continue
            # prop_pres_data = present_data[(present_data['Property Address'] == address) & (present_data['Property Name'] == building)]
            prop_pres_data = present_data.iloc[present_data_idx]
            # self.clean_data(saved_search, present_data, historical_data)

            data_dict = {}
            for k, v in prop_hist_data.items():
                # Entries here are arrays of historical data
                if k in self.orig_labels:
                    hist_data_array = v.values
                    # Convert numpy array to list of strings -- Mongo doesn't like numpy arrays
                    hist_data_list = [str(x) for x in hist_data_array]
                    data_dict[self.label_map[k]] = hist_data_list
            
            for k, v in prop_pres_data.items():
                # Entries here are single values of present data
                if k in self.orig_labels:
                    # # v is a pandas Series object, so use empty attribute to check if it's empty
                    # if not v.empty: v = v.iloc[0]
                    # else: v = np.nan
                    pres_data_entry = str(v)
                    data_dict[self.label_map[k]] = pres_data_entry
            
            data_dict_list.append(data_dict)
            props_added.add((address, building))
        self.new_raw_data.insert_many(data_dict_list)
        self.download_log(f'\n###################################################')
        self.download_log(f'### Data for {saved_search} written to MongoDB!')
        self.download_log(f'### {len(data_dict_list)} entries added')
        self.download_log(f'###################################################\n')

    ########################################################################################################################
    def Close_Mongo(self):
        self.client.close()

