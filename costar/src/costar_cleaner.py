import numpy as np
import pandas as pd
from pymongo import MongoClient
import certifi, re, time, os
from model_definitions import GetForecast
import copy
from dotenv import load_dotenv
from tqdm import tqdm

class CostarCleaner:
    def __init__(self):
        try:
            load_dotenv()
            connect_str = os.environ['PARTNERSDB_URI']
            self.client = MongoClient(connect_str, tlsCAFile=certifi.where())
        except Exception as e:
            print('Error connecting to mongo client.')
            print(f'Error: \n{e}')
            exit(1)
        
        self.raw_db = self.client['costagg']
        self.new_raw_data = self.raw_db['new_raw_data']

        self.clean_db = self.client['partners-edge']
        self.properties_collection = self.clean_db['properties']
        self.comps_collection = self.clean_db['apto_comps']
        self.counties_collection = self.clean_db['counties']
        self.zip_codes_collection = self.clean_db['zip_codes']
        self.markets_collection = self.clean_db['markets']

        # If "new_properties" collection doesn't exist, create it
        # if 'new_properties' not in self.clean_db.list_collection_names():
        #     self.new_properties_collection = self.clean_db['new_properties']
        self.new_properties_collection = self.clean_db['new_properties']
        
        data_info_table = pd.read_csv('costar/input/data_info.csv', header=0, index_col=0)

        self.feature_types = dict(zip(data_info_table['db_labels'].values, 
                                      data_info_table['feature_types'].values))




    ############################################################
    def clean_and_set_data(self):
        def clean_str_array(raw_array):
            clean_array = []
            try:
                for val in raw_array:
                    clean_array.append(val)
            except Exception as e:
                print(f'Error in Cleaning String Array: \n{e}')
                return raw_array
            return clean_array
        

        def clean_float_array(raw_array):
            clean_array = []
            try:
                for val in raw_array: 
                    if val in ['nan', '-', '']:
                        clean_array.append(np.nan)
                    else:
                        clean_array.append(float(val))
            except Exception as e:
                print(f'Error in Cleaning Float Array: \n{e}')
                return raw_array
            return clean_array


        def clean_str_value(raw_value):
            try:
                if raw_value in ['nan', '-', '']:
                    return np.nan
                else:
                    return str(raw_value)
            except Exception as e:
                print(f'Error in Cleaning String Value: \n{e}')
                return raw_value


        def clean_float_value(raw_value):
            try:
                if raw_value in ['nan', '-', '']:
                    return np.nan
                else:
                    return float(raw_value)
            except Exception as e:
                print(f'Error in Cleaning Float Value: \n{e}')
                return raw_value


        def clean_misc_value(raw_value, feat_name):
            if raw_value in ['nan', '-', '']:
                return np.nan
            try:
                match feat_name:
                    ##############################
                    case 'statusHist':
                        clean_value = []
                        for val in raw_value:
                            if val == "nan":
                                clean_value.append("Under Construction")
                            else:
                                clean_value.append(val)
                    ##############################
                    case 'bldgTaxExpenses' | 'bldgOpExpenses':
                        rate_pattern = r'\$(\d+\.\d+)/sf'
                        year_pattern = r'\b(\d{4}) (Tax|Ops)\b'
                        est_year_pattern = r'\b(\d{4}) Est (Tax|Ops)\b'
                        rate_test = re.search(rate_pattern, raw_value)
                        year_test = re.search(year_pattern, raw_value)
                        est_year_test = re.search(est_year_pattern, raw_value)
                        if rate_test and year_test:
                            clean_value = {
                                'rate': float(rate_test.group(1)),
                                'year': int(year_test.group(1)), 
                                'est': False
                            }
                        elif rate_test and est_year_test:
                            clean_value = {
                                'rate': float(rate_test.group(1)),
                                'year': int(est_year_test.group(1)), 
                                'est': True
                            }
                        else:
                            print(f'Unexpected pattern in \'{feat_name}\' value: \n{raw_value}')
                            clean_value = raw_value
                    ##############################
                    case 'leasingCompanyPhone' | 'leasingCompanyFax':
                        raw_value = raw_value[:-2]
                        clean_value = raw_value[:3] + '-' + raw_value[3:6] + '-' + raw_value[6:]
                    ##############################
                    case 'features':
                        clean_value = raw_value.split(', ')
                    ##############################
                    case 'power':
                        clean_value = {}
                        amp_pattern = r'(\d+-\d+|\d+)a'
                        volt_pattern = r'(\d+-\d+|\d+)v'
                        phase_pattern = r'(\d+)p'
                        wire_pattern = r'(\d+)w'

                        if 'Heavy' in raw_value: 
                            clean_value['amps'] = (800, np.inf)
                        elif re.search(amp_pattern, raw_value):
                            amp_str = re.search(amp_pattern, raw_value).group(1)
                            if '-' in amp_str:
                                clean_value['amps'] = tuple(map(int, amp_str.split('-')))
                            else:
                                clean_value['amps'] = (int(amp_str), int(amp_str))
                        else:
                            clean_value['amps'] = (np.nan, np.nan)
                        
                        if re.search(volt_pattern, raw_value):
                            volt_str = re.search(volt_pattern, raw_value).group(1)
                            if '-' in volt_str:
                                clean_value['volts'] = tuple(map(int, volt_str.split('-')))
                            else:
                                clean_value['volts'] = (int(volt_str), int(volt_str))
                        else:
                            clean_value['volts'] = (np.nan, np.nan)
                        
                        if re.search(phase_pattern, raw_value):
                            clean_value['phases'] = int(re.search(phase_pattern, raw_value).group(1))
                        else:
                            clean_value['phases'] = np.nan
                        
                        if re.search(wire_pattern, raw_value):
                            clean_value['wires'] = int(re.search(wire_pattern, raw_value).group(1))
                        else:
                            clean_value['wires'] = np.nan
                    ##############################
                    case 'ceilingHeight':
                        raw_value = raw_value[:-1]
                        foot_inch = raw_value.split('\'')
                        clean_value = int(foot_inch[0]) + int(foot_inch[1])/12
                    ##############################
                    case 'driveIns':
                        # Quickly test is value is just "Yes"
                        if raw_value == 'Yes':
                            return {'exists': True, 'quantity': np.nan, 'width': np.nan, 'height': np.nan}
                        # Quickly test if value is just quantity
                        try: 
                            count = int(raw_value)
                            return {'exists': True, 'quantity': count, 'width': np.nan, 'height': np.nan}
                        except Exception: 
                            pass

                        clean_value = {}
                        clean_value['exists'] = True

                        quantity = int(raw_value.split('/')[0])
                        clean_value['quantity'] = quantity

                        width_pattern = r"(\d+)'(\d+)\"w"
                        height_pattern = r"(\d+)'(\d+)\"h"
                        if re.search(width_pattern, raw_value):
                            width_ft = int(re.search(width_pattern, raw_value).group(1))
                            width_in = int(re.search(width_pattern, raw_value).group(2))
                            width = width_ft + width_in/12
                        else:
                            width = np.nan
                        
                        if re.search(height_pattern, raw_value):
                            height_ft = int(re.search(height_pattern, raw_value).group(1))
                            height_in = int(re.search(height_pattern, raw_value).group(2))
                            height = height_ft + height_in/12
                        else:
                            height = np.nan
                        clean_value['width'] = width
                        clean_value['height'] = height
                    ##############################
                    case 'columnSpacing':
                        clean_value = {}
                        width_pattern = r"(\d+-\d+|\d+)'w"
                        depth_pattern = r"(\d+-\d+|\d+)'d"

                        if re.search(width_pattern, raw_value):
                            width_str = re.search(width_pattern, raw_value).group(1)
                            if '-' in width_str:
                                # Only take the minimum column spacing. 
                                clean_value['width'] = int(width_str.split('-')[0])
                            else:
                                clean_value['width'] = int(width_str)
                        else:
                            clean_value['width'] = np.nan

                        if re.search(depth_pattern, raw_value):
                            depth_str = re.search(depth_pattern, raw_value).group(1)
                            if '-' in depth_str:
                                # Only take the minimum column spacing. 
                                clean_value['depth'] = int(depth_str.split('-')[0])
                            else:
                                clean_value['depth'] = int(depth_str)
                        else:
                            clean_value['depth'] = np.nan
                    ##############################
                    case 'latitude' | 'longitude':
                        clean_value = round(float(raw_value), 7)
                    ##############################
                    case 'rent':
                        clean_value = {}
                        if "-" not in raw_value:
                            if "Est." in raw_value:
                                # Remove "$" and " (Est.)"
                                clean_rent = raw_value.replace("$", "")
                                clean_rent = clean_rent.replace(" (Est.)", "")
                                clean_value['rate'] = (clean_rent, clean_rent)
                                clean_value['est'] = True
                            else:
                                # Remove "$"
                                clean_rent = float(raw_value[1:])
                                clean_value['rate'] = (clean_rent, clean_rent)
                                clean_value['est'] = False
                        else:
                            if "Est." in raw_value:
                                clean_value['est'] = True
                                rents = raw_value.replace("$", "")
                                rents = rents.replace(" (Est.)", "")
                                rents = rents.split(" - ")
                                clean_value['rate'] = (float(rents[0]), float(rents[1]))
                            else:
                                clean_value['est'] = False
                                rents = raw_value[1:].split(" - ")
                                clean_value['rate'] = (float(rents[0]), float(rents[1]))
                    ##############################
                    case 'zip': 
                        clean_value = raw_value[0:5]

            except Exception as e:
                print(f'Error in Cleaning {feat} Value: \n(Raw Value: {raw_value}) \n{e}')
                return raw_value

            return clean_value


        market_names = self.new_raw_data.find().distinct('market')
        new_property_ids = []
        clean_doc_list = []
        for mkt in market_names:
            # Get data from MongoDB
            self.raw_batch = self.new_raw_data.find({'market': mkt})

            for raw_document in self.raw_batch:
                clean_document = {}
                for feat in raw_document:
                    if feat == '_id': continue
                    feat_type = self.feature_types[feat]
                    match feat_type:
                        case 'String Array':
                            clean_feat_val = clean_str_array(raw_document[feat])
                        case 'Float Array':
                            clean_feat_val = clean_float_array(raw_document[feat])
                        case 'String Value':
                            clean_feat_val = clean_str_value(raw_document[feat])
                        case 'Float Value':
                            clean_feat_val = clean_float_value(raw_document[feat])
                        case 'MISC':
                            clean_feat_val = clean_misc_value(raw_document[feat], feat)
                        case _:
                            clean_feat_val = raw_document[feat]

                    # Check if clean_feat_val is not an array and is nan
                    if not isinstance(clean_feat_val, list) and pd.isna(clean_feat_val):
                        # Don't create field in document if it's a non-array with a value of nan
                        continue
                    else:
                        clean_document[feat] = clean_feat_val
                        
                # Create additional fields here
                if 'latitude' in clean_document.keys() and 'longitude' in clean_document.keys():
                    clean_document['loc_geojson'] = {
                        'type': 'Point',
                        'coordinates': [clean_document['longitude'], clean_document['latitude']]
                    }


                clean_doc_list.append(clean_document)

        self.new_properties_collection.insert_many(clean_doc_list)
        
        # Only call update comps AFTER all new data has been added to properties collection
        # if new_property_ids: 
        #     self.update_comps(new_property_ids)
    



    ############################################################
    ############################################################
    # ONLY CALL AFTER ALL DATA HAS BEEN ADDED TO PROPERTIES COLLECTION
    def update_comps(self):

        # Get all unique costar IDs from properties collection
        costar_ids = self.new_properties_collection.find({}, {'costarID': 1, '_id': 0})
        costar_ids = pd.DataFrame(costar_ids)
        costar_ids = costar_ids.dropna(subset=['costarID'])
        costar_ids = list(costar_ids['costarID'].unique())

        for costarID in costar_ids: 
            # Check if costarID is in comps collection
            if self.comps_collection.count_documents({'costarID': costarID}) == 0:
                continue
            else:
                property_data = self.new_properties_collection.find_one({'costarID': costarID}, {"_id": 0, "rba": 1, "ceilingHeight": 1, "loc_geojson": 1})
                rba = property_data['rba']
                ceilingHeight = property_data['ceilingHeight']
                location = property_data['loc_geojson']

                # Compute bounds
                rba_lower_bound = rba - (rba * 0.2)
                rba_upper_bound = rba + (rba * 0.2)
                ceilingHeight_lower_bound = ceilingHeight - (ceilingHeight * 0.2)
                ceilingHeight_upper_bound = ceilingHeight + (ceilingHeight * 0.2)

                distanceInMeters = 5000

                ####################
                # SALE COMPS
                nearby_sale_comps = self.comps_collection.find({"transactionType": "Sale", "loc_geojson": {"$near": location, "$maxDistance": distanceInMeters}}, {"_id": 0, "costarID": 1})
                nearby_sale_comps = pd.DataFrame(nearby_sale_comps)
                if nearby_sale_comps.empty:
                    saleComps = []
                else:
                    nearby_sale_comps = nearby_sale_comps.dropna(subset=['costarID'])
                    nearby_sale_comps_costarIDs = list(nearby_sale_comps['costarID'].unique())
                    similar_properties = self.new_properties_collection.aggregate(
                        [
                        {
                            "$match": {
                                "rba": {"$gte": rba_lower_bound, "$lte": rba_upper_bound},
                                "ceilingHeight": {"$gte": ceilingHeight_lower_bound, "$lte": ceilingHeight_upper_bound},
                                "costarID": {"$in": nearby_sale_comps_costarIDs}
                            }
                        },
                        {
                            "$project": {"costarID": 1}
                        }
                        ])
                    saleCompIDs = pd.DataFrame(similar_properties)
                    if saleCompIDs.empty:
                        saleComps = []
                    else:
                        saleComps = list(saleCompIDs['costarID'].unique())
                    
                # Update the property document with the sale comps
                if saleComps:
                    self.new_properties_collection.update_one(
                        {"costarID": costarID},
                        {"$set": {"saleComps": saleComps}}
                    )

                ####################
                # LEASE COMPS
                nearby_lease_comps = self.comps_collection.find({"transactionType": "Lease", "loc_geojson": {"$near": location, "$maxDistance": distanceInMeters}}, {"_id": 0, "costarID": 1})
                nearby_lease_comps = pd.DataFrame(nearby_lease_comps)
                if nearby_lease_comps.empty:
                    leaseComps = []
                else:
                    nearby_lease_comps = nearby_lease_comps.dropna(subset=['costarID'])
                    nearby_lease_comps_costarIDs = list(nearby_lease_comps['costarID'].unique())
                    similar_properties = self.new_properties_collection.aggregate(
                        [
                        {
                            "$match": {
                                "rba": {"$gte": rba_lower_bound, "$lte": rba_upper_bound},
                                "ceilingHeight": {"$gte": ceilingHeight_lower_bound, "$lte": ceilingHeight_upper_bound},
                                "costarID": {"$in": nearby_lease_comps_costarIDs}
                            }
                        },
                        {
                            "$project": {"costarID": 1}
                        }
                        ])
                    leaseCompIDs = pd.DataFrame(similar_properties)
                    if leaseCompIDs.empty:
                        leaseComps = []
                    else:
                        leaseComps = list(leaseCompIDs['costarID'].unique())
                    
                # Update the property document with the sale comps
                if leaseComps:
                    self.new_properties_collection.update_one(
                        {"costarID": costarID},
                        {"$set": {"leaseComps": leaseComps}}
                    )




    ############################################################
    ############################################################
    def update_collections(self):
        # If collection "prev_properties" exists, drop it
        if 'prev_properties' in self.clean_db.list_collection_names():
            self.clean_db['prev_properties'].drop()
        # If collection "properties" exists, rename it to "prev_properties"
        if 'properties' in self.clean_db.list_collection_names():
            self.clean_db['properties'].rename('prev_properties')
        # Set new_properties as properties
        self.new_properties_collection.rename('properties')

        # If collection "prev_archive" exists, drop it
        if 'prev_archive' in self.raw_db.list_collection_names():
            self.raw_db['prev_archive'].drop()
        # If collection "archive" exists, rename it to "prev_archive"
        if 'archive' in self.raw_db.list_collection_names():
            self.raw_db['archive'].rename('prev_archive')
        # Set new_raw_data as archive
        self.new_raw_data.rename('archive')

















    ############################################################
    ############################################################
    # Replacements for Realm functions

    def get_quarter_names(self):
        current_date = time.localtime()
        current_year = current_date.tm_year
        current_month = current_date.tm_mon
        current_quarter = (current_month - 1) // 3 + 1
        start_year = 2011
        numQuarters = (current_year - start_year) * 4 + (current_month - 1) // 3

        quarterNames = []
        year = start_year
        quarter = 1
        # Going up to range(numQuarters) instead of range(numQuarters + 1) because we don't want to include the current quarter (QTD value is not useful for aggregation purposes)
        for i in range(numQuarters):
            quarterNames.append(f"{year} Q{quarter}")
            
            if quarter == 4:
                year += 1
                quarter = 1
            else:
                quarter += 1
        
        return quarterNames


    def get_prev_quarter(self, quarter):
        year, q_num = quarter.split("-Q")
        q_num = int(q_num)
        if q_num == 1:
            year = str(int(year) - 1)
            q_num = 4
        else:
            q_num -= 1
        return f"{year}-Q{q_num}"
    

    def get_next_quarter(self, quarter):
        year, q_num = quarter.split("-Q")
        q_num = int(q_num)
        if q_num == 4:
            year = str(int(year) + 1)
            q_num = 1
        else:
            q_num += 1
        return f"{year}-Q{q_num}"



    def aggregate_county_data(self):
        # Generate quarter list
        quarterNames = self.get_quarter_names()
        # Get list of unique (county, state) pairs via aggregation pipeline
        unique_counties_pipeline = [
            {"$group": {"_id": {"county": "$county", "state": "$state"}}},
            {"$project": {"county": "$_id.county", "state": "$_id.state", "_id": 0}}
        ]
        unique_counties = list(self.counties_collection.aggregate(unique_counties_pipeline))

        # For each county
        for cty in unique_counties:
            properties_in_county = self.new_properties_collection.find({"county": cty['county'], "state": cty['state']},
                                                        {'_id': 0, 
                                                         'address': 1, 
                                                         'quarter': 1, 
                                                         'statusHist': 1, 
                                                         'occupancySF': 1, 
                                                         'netAbsorptionSFTotal': 1, 
                                                         'status': 1,
                                                         'rba': 1, 
                                                         'ceilingHeight': 1, 
                                                         'yearBuilt': 1, 
                                                         'propertyType': 1})
            region_df = pd.DataFrame(properties_in_county)

            # If no properties in county, skip to next county
            if region_df.empty:
                continue

            # Initialize each aggregate field in a dataframe
            aggregate_df = pd.DataFrame({'quarter': quarterNames})
            aggregate_df['propertyCount'] = 0
            aggregate_df['rbaSum'] = 0
            aggregate_df['ceilingHeightSum'] = 0
            aggregate_df['yearBuiltSum'] = 0
            aggregate_df['occupancySFSum'] = 0
            aggregate_df['netAbsSum'] = 0

            # Initialize dataframe for forecasting input
            hist_df = pd.DataFrame(columns=['Quarter', 'NA', 'DEL'])
            # Initialize under construction list for forecasting input
            uc_sf = [0]*6
            # Initialize total existing rba
            total_rba = 0

            # Get most recent quarter in data
            sample_quarters = region_df.loc[0]['quarter']
            sample_quarters = [q for q in sample_quarters if "QTD" in q]
            if len(sample_quarters) == 0:
                continue
            else:
                most_recent_quarter = sample_quarters[0]
            most_recent_quarter = most_recent_quarter.replace(" QTD", "")
            most_recent_year, most_recent_quarter_number = most_recent_quarter.split(" Q")

            # Get historical data quarters
            hist_qtr = f"{most_recent_year}-Q{most_recent_quarter_number}"
            historical_quarters = []
            for _ in range(24):
                hist_qtr = self.get_prev_quarter(hist_qtr)
                historical_quarters.append(hist_qtr)
            
            hist_df['Quarter'] = historical_quarters
            hist_df['NA'] = 0
            hist_df['DEL'] = 0


            # For each property
            for idx, prop in region_df.iterrows():
                # Create new df for quarter, statusHist, occupancySF, netAbsorptionSFTotal
                prop_df = pd.DataFrame({'quarter': prop['quarter'],
                                        'statusHist': prop['statusHist'],
                                        'occupancySF': prop['occupancySF'],
                                        'netAbsorptionSFTotal': prop['netAbsorptionSFTotal']})
                # If prop_df is empty, continue
                if prop_df.empty:
                    continue
                # Remove row where quarter that includes QTD
                prop_df = prop_df[prop_df['quarter'].str[-3:] != 'QTD']
                # Remove rows where quarter is before 2011 Q1
                prop_df = prop_df[prop_df['quarter'].str[:4] > "2010"]

                # Create separate df for hist data
                hist_prop_df = copy.deepcopy(prop_df)
                hist_prop_df['netAbsorptionSFTotal'] = hist_prop_df['netAbsorptionSFTotal'].fillna(0)

                # Remove rows where statusHist is not "Existing"
                prop_df = prop_df[prop_df['statusHist'] == "Existing"]
                # Replace nan values in netAbsorptionSFTotal with 0
                prop_df['netAbsorptionSFTotal'] = prop_df['netAbsorptionSFTotal'].fillna(0)

            
                # For each quarter left in prop_df
                for idx, row in prop_df.iterrows():

                    qtr = row['quarter']

                    # Add values to aggregate fields
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'propertyCount'] += 1
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'rbaSum'] += prop['rba']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'ceilingHeightSum'] += prop['ceilingHeight']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'yearBuiltSum'] += prop['yearBuilt']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'occupancySFSum'] += row['occupancySF']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'netAbsSum'] += row['netAbsorptionSFTotal']


                if prop['status'] == "Existing":
                    total_rba += prop['rba']

                # Check if property is under construction
                current_status = hist_prop_df['statusHist']
                if current_status.empty:
                    continue
                else:
                    current_status = current_status.iloc[0]
                if current_status == "Under Construction":
                    num_uc_quarters = list(prop['statusHist'])[1:].count("Under Construction")
                    if num_uc_quarters == 1:
                        uc_sf[0] += prop['rba']
                    elif num_uc_quarters == 2:
                        uc_sf[1] += prop['rba']
                    elif num_uc_quarters == 3:
                        uc_sf[2] += prop['rba']
                    elif num_uc_quarters == 4:
                        uc_sf[3] += prop['rba']
                    elif num_uc_quarters == 5:
                        uc_sf[4] += prop['rba']
                    elif num_uc_quarters == 6:
                        uc_sf[5] += prop['rba']
                    continue

                for idx, row in hist_prop_df.iterrows():
                    qtr = row['quarter']
                    if qtr.replace(" ", "-") not in hist_df['Quarter'].values:
                        continue

                    # Add values to historical data
                    prev_qtr = self.get_prev_quarter(qtr.replace(" ", "-"))
                    # Get status
                    qtr_status = row['statusHist']
                    prev_qtr_status = list(hist_prop_df[hist_prop_df['quarter'] == prev_qtr.replace("-", " ")]['statusHist'].values)
                    # if prop['status'] in ['Demolished', 'Converted']:
                    if qtr_status in ['Demolished', 'Converted']:
                        # Check if prev quarter was Existing or Under Renovation. If so, subtract rba from DEL
                        if "Existing" in prev_qtr_status:
                            hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'DEL'] -= prop['rba']
                    elif qtr_status in ['Existing', 'Under Renovation']:
                        hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'NA'] += hist_prop_df[hist_prop_df['quarter'] == qtr]['netAbsorptionSFTotal'].values[0]
                        if "Under Construction" in prev_qtr_status:
                            hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'DEL'] += prop['rba']

            # Create new columns for meanRba, meanCeilingHeight, meanYearBuilt
            aggregate_df['meanRba'] = aggregate_df['rbaSum'] / aggregate_df['propertyCount']
            aggregate_df['meanCeilingHeight'] = aggregate_df['ceilingHeightSum'] / aggregate_df['propertyCount']
            aggregate_df['meanYearBuilt'] = aggregate_df['yearBuiltSum'] / aggregate_df['propertyCount']
            # Create new column for occupancy rate
            aggregate_df['occupancyRate'] = aggregate_df['occupancySFSum'] / aggregate_df['rbaSum']

            # Convert dataframe to array of dictionaries {label: quarter, value: v} for propertyCount, rba, meanRba, meanCeilingHeight, meanYearBuilt, occupancyRate, netAbsorption
            propertyCount = []
            rba = []
            meanRba = []
            meanCeilingHeight = []
            meanYearBuilt = []
            occupancyRate = []
            netAbsorption = []
            for idx, row in aggregate_df.iterrows():
                propertyCount.append({'label': row['quarter'], 'value': row['propertyCount']})
                rba.append({'label': row['quarter'], 'value': row['rbaSum']})
                meanRba.append({'label': row['quarter'], 'value': row['meanRba']})
                meanCeilingHeight.append({'label': row['quarter'], 'value': row['meanCeilingHeight']})
                meanYearBuilt.append({'label': row['quarter'], 'value': row['meanYearBuilt']})
                occupancyRate.append({'label': row['quarter'], 'value': row['occupancyRate']})
                netAbsorption.append({'label': row['quarter'], 'value': row['netAbsSum']})
            
            # Get forecasts for county
            forecast_result = self.generate_forecasts(total_rba, hist_df, uc_sf)
            # print(f"{cty['county']}, {cty['state']}")
            # print(total_rba)
            # print(uc_sf)
            # print(hist_df)
            # print(na_forecast)
            # print(del_forecast)
            
            
            # Update the county document with the aggregate fields
            if not forecast_result or type(forecast_result) == str:
                self.counties_collection.update_one({'county': cty['county'], 'state': cty['state']},
                                        {'$set': {'propertyCount': propertyCount,
                                                'rba': rba,
                                                'meanRba': meanRba,
                                                'meanCeilingHeight': meanCeilingHeight,
                                                'meanYearBuilt': meanYearBuilt,
                                                'occupancyRate': occupancyRate,
                                                'netAbsorption': netAbsorption},
                                        '$unset': {'netAbsForecast': "", 
                                                   'delForecast': ""}})
            else:
                na_forecast, del_forecast = forecast_result
                # Convert na_forecast and del_forecast into lists from torch tensor
                na_forecast = na_forecast.tolist()
                del_forecast = del_forecast.tolist()
                # Get current quarter
                current_quarter = "2024-Q3"
                # Get next 7 quarters
                next_quarters = [current_quarter]
                for _ in range(7):
                    current_quarter = self.get_next_quarter(current_quarter)
                    next_quarters.append(current_quarter)
                # Convert na_forecast and del_forecast into dictionaries {label: quarter, value: v}
                na_forecast = [{'label': qtr, 'value': val} for qtr, val in zip(next_quarters, na_forecast)]
                del_forecast = [{'label': qtr, 'value': val} for qtr, val in zip(next_quarters, del_forecast)]
                self.counties_collection.update_one({'county': cty['county'], 'state': cty['state']},
                                        {'$set': {'propertyCount': propertyCount,
                                                'rba': rba,
                                                'meanRba': meanRba,
                                                'meanCeilingHeight': meanCeilingHeight,
                                                'meanYearBuilt': meanYearBuilt,
                                                'occupancyRate': occupancyRate,
                                                'netAbsorption': netAbsorption,
                                                'netAbsForecast': na_forecast,
                                                'delForecast': del_forecast}})





    def aggregate_zip_data(self):
        # Generate quarter list
        quarterNames = self.get_quarter_names()
        # Get list of unique zips via aggregation pipeline
        unique_zips_pipeline = [
                                    {"$group": {"_id": None, "uniqueZips": {"$addToSet": "$zip"}}},
                                    {"$project": {"_id": 0, "uniqueZips": 1,}},
        ]
        unique_zips = list(self.zip_codes_collection.aggregate(unique_zips_pipeline))
        unique_zips = unique_zips[0]['uniqueZips']

        # For each county
        for zip_code in unique_zips:
            properties_in_zip = self.new_properties_collection.find({"zip": zip_code},
                                                        {'_id': 0, 
                                                         'address': 1, 
                                                         'quarter': 1, 
                                                         'statusHist': 1, 
                                                         'occupancySF': 1, 
                                                         'netAbsorptionSFTotal': 1, 
                                                         'status': 1,
                                                         'rba': 1, 
                                                         'ceilingHeight': 1, 
                                                         'yearBuilt': 1, 
                                                         'propertyType': 1})
            region_df = pd.DataFrame(properties_in_zip)

            # If no properties in zip, skip to next zip
            if region_df.empty:
                continue

            # Initialize each aggregate field in a dataframe
            aggregate_df = pd.DataFrame({'quarter': quarterNames})
            aggregate_df['propertyCount'] = 0
            aggregate_df['rbaSum'] = 0
            aggregate_df['ceilingHeightSum'] = 0
            aggregate_df['yearBuiltSum'] = 0
            aggregate_df['occupancySFSum'] = 0
            aggregate_df['netAbsSum'] = 0

            # Initialize dataframe for forecasting input
            hist_df = pd.DataFrame(columns=['Quarter', 'NA', 'DEL'])
            # Initialize under construction list for forecasting input
            uc_sf = [0]*6
            # Initialize total existing rba
            total_rba = 0

            # Get most recent quarter in data
            sample_quarters = region_df.loc[0]['quarter']
            sample_quarters = [q for q in sample_quarters if "QTD" in q]
            if len(sample_quarters) == 0: 
                continue
            else:
                most_recent_quarter = sample_quarters[0]
            most_recent_quarter = most_recent_quarter.replace(" QTD", "")
            most_recent_year, most_recent_quarter_number = most_recent_quarter.split(" Q")

            # Get historical data quarters
            hist_qtr = f"{most_recent_year}-Q{most_recent_quarter_number}"
            historical_quarters = []
            for _ in range(24):
                hist_qtr = self.get_prev_quarter(hist_qtr)
                historical_quarters.append(hist_qtr)
            
            hist_df['Quarter'] = historical_quarters
            hist_df['NA'] = 0
            hist_df['DEL'] = 0


            # For each property
            for idx, prop in region_df.iterrows():
                # Create new df for quarter, statusHist, occupancySF, netAbsorptionSFTotal
                prop_df = pd.DataFrame({'quarter': prop['quarter'],
                                        'statusHist': prop['statusHist'],
                                        'occupancySF': prop['occupancySF'],
                                        'netAbsorptionSFTotal': prop['netAbsorptionSFTotal']})

                # Remove row where quarter that includes QTD
                prop_df = prop_df[prop_df['quarter'].str[-3:] != 'QTD']
                # Remove rows where quarter is before 2011 Q1
                prop_df = prop_df[prop_df['quarter'].str[:4] > "2010"]

                # Create separate df for hist data
                hist_prop_df = copy.deepcopy(prop_df)
                hist_prop_df['netAbsorptionSFTotal'] = hist_prop_df['netAbsorptionSFTotal'].fillna(0)

                # Remove rows where statusHist is not "Existing"
                prop_df = prop_df[prop_df['statusHist'] == "Existing"]
                # Replace nan values in netAbsorptionSFTotal with 0
                prop_df['netAbsorptionSFTotal'] = prop_df['netAbsorptionSFTotal'].fillna(0)
            
                # For each quarter left in prop_df
                for idx, row in prop_df.iterrows():
                    qtr = row['quarter']
                    # Add values to aggregate fields
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'propertyCount'] += 1
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'rbaSum'] += prop['rba']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'ceilingHeightSum'] += prop['ceilingHeight']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'yearBuiltSum'] += prop['yearBuilt']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'occupancySFSum'] += row['occupancySF']
                    aggregate_df.loc[aggregate_df['quarter'] == qtr, 'netAbsSum'] += row['netAbsorptionSFTotal']


                if prop['status'] == "Existing":
                    total_rba += prop['rba']

                # Check if property is under construction
                current_status = hist_prop_df['statusHist']
                if current_status.empty:
                    continue
                else:
                    current_status = current_status.iloc[0]
                if current_status == "Under Construction":
                    num_uc_quarters = list(prop['statusHist'])[1:].count("Under Construction")
                    if num_uc_quarters == 1:
                        uc_sf[0] += prop['rba']
                    elif num_uc_quarters == 2:
                        uc_sf[1] += prop['rba']
                    elif num_uc_quarters == 3:
                        uc_sf[2] += prop['rba']
                    elif num_uc_quarters == 4:
                        uc_sf[3] += prop['rba']
                    elif num_uc_quarters == 5:
                        uc_sf[4] += prop['rba']
                    elif num_uc_quarters == 6:
                        uc_sf[5] += prop['rba']
                    continue

                for idx, row in hist_prop_df.iterrows():
                    qtr = row['quarter']
                    if qtr.replace(" ", "-") not in hist_df['Quarter'].values:
                        continue

                    # Add values to historical data
                    prev_qtr = self.get_prev_quarter(qtr.replace(" ", "-"))
                    # Get status
                    qtr_status = row['statusHist']
                    prev_qtr_status = list(hist_prop_df[hist_prop_df['quarter'] == prev_qtr.replace("-", " ")]['statusHist'].values)
                    # if prop['status'] in ['Demolished', 'Converted']:
                    if qtr_status in ['Demolished', 'Converted']:
                        # Check if prev quarter was Existing or Under Renovation. If so, subtract rba from DEL
                        if "Existing" in prev_qtr_status:
                            hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'DEL'] -= prop['rba']
                    elif qtr_status in ['Existing', 'Under Renovation']:
                        hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'NA'] += hist_prop_df[hist_prop_df['quarter'] == qtr]['netAbsorptionSFTotal'].values[0]
                        if "Under Construction" in prev_qtr_status:
                            hist_df.loc[hist_df['Quarter'] == qtr.replace(" ", "-"), 'DEL'] += prop['rba']


            # Create new columns for meanRba, meanCeilingHeight, meanYearBuilt
            aggregate_df['meanRba'] = aggregate_df['rbaSum'] / aggregate_df['propertyCount']
            aggregate_df['meanCeilingHeight'] = aggregate_df['ceilingHeightSum'] / aggregate_df['propertyCount']
            aggregate_df['meanYearBuilt'] = aggregate_df['yearBuiltSum'] / aggregate_df['propertyCount']
            # Create new column for occupancy rate
            aggregate_df['occupancyRate'] = aggregate_df['occupancySFSum'] / aggregate_df['rbaSum']

            # Convert dataframe to array of dictionaries {label: quarter, value: v} for propertyCount, rba, meanRba, meanCeilingHeight, meanYearBuilt, occupancyRate, netAbsorption
            propertyCount = []
            rba = []
            meanRba = []
            meanCeilingHeight = []
            meanYearBuilt = []
            occupancyRate = []
            netAbsorption = []
            for idx, row in aggregate_df.iterrows():
                propertyCount.append({'label': row['quarter'], 'value': row['propertyCount']})
                rba.append({'label': row['quarter'], 'value': row['rbaSum']})
                meanRba.append({'label': row['quarter'], 'value': row['meanRba']})
                meanCeilingHeight.append({'label': row['quarter'], 'value': row['meanCeilingHeight']})
                meanYearBuilt.append({'label': row['quarter'], 'value': row['meanYearBuilt']})
                occupancyRate.append({'label': row['quarter'], 'value': row['occupancyRate']})
                netAbsorption.append({'label': row['quarter'], 'value': row['netAbsSum']})
            
            # Get forecasts for county
            forecast_result = self.generate_forecasts(total_rba, hist_df, uc_sf)


            # Update the county document with the aggregate fields
            if not forecast_result or type(forecast_result) == str:
                self.zip_codes_collection.update_one({'zip': zip_code},
                                        {
                                            '$set': {
                                                'propertyCount': propertyCount,
                                                'rba': rba,
                                                'meanRba': meanRba,
                                                'meanCeilingHeight': meanCeilingHeight,
                                                'meanYearBuilt': meanYearBuilt,
                                                'occupancyRate': occupancyRate,
                                                'netAbsorption': netAbsorption},
                                            '$unset': {
                                                'netAbsForecast': "",
                                                'delForecast': ""
                                                }
                                        })
            else:
                na_forecast, del_forecast = forecast_result
                # Convert na_forecast and del_forecast into lists from torch tensor
                na_forecast = na_forecast.tolist()
                del_forecast = del_forecast.tolist()                
                # Get current quarter
                current_quarter = "2024-Q3"
                # Get next 7 quarters
                next_quarters = [current_quarter]
                for _ in range(7):
                    current_quarter = self.get_next_quarter(current_quarter)
                    next_quarters.append(current_quarter)
                # Convert na_forecast and del_forecast into dictionaries {label: quarter, value: v}
                na_forecast = [{'label': qtr, 'value': val} for qtr, val in zip(next_quarters, na_forecast)]
                del_forecast = [{'label': qtr, 'value': val} for qtr, val in zip(next_quarters, del_forecast)]
                self.zip_codes_collection.update_one({'zip': zip_code},
                                        {
                                            '$set': {
                                                'propertyCount': propertyCount,
                                                'rba': rba,
                                                'meanRba': meanRba,
                                                'meanCeilingHeight': meanCeilingHeight,
                                                'meanYearBuilt': meanYearBuilt,
                                                'occupancyRate': occupancyRate,
                                                'netAbsorption': netAbsorption,
                                                'netAbsForecast': na_forecast,
                                                'delForecast': del_forecast}
                                        })



    def set_market_centers(self):
        unique_market_pipeline = [
            {
                "$group": {
                    "_id": {
                        "market": "$market",
                        "state": "$state"
                    }
                }
            }
        ]

        unique_markets = list(self.new_properties_collection.aggregate(unique_market_pipeline))
        unique_markets = [market['_id'] for market in unique_markets]

        for mkt in unique_markets:
            if 'market' not in mkt.keys() or 'state' not in mkt.keys():
                continue
            props = self.new_properties_collection.find({"market": mkt['market'], "state": mkt['state']})
            prop_df = pd.DataFrame(list(props))

            # Get average latitude and longitude
            avg_lat = prop_df['latitude'].mean()
            avg_lon = prop_df['longitude'].mean()

            # If market is already in market_coll, update the marketCenter, else insert a new document
            market = self.markets_collection.find_one({"market": mkt['market'], "state": mkt['state']})
            if market:
                self.markets_collection.update_one({"_id": market["_id"]}, {"$set": {"marketCenter": {"latitude": avg_lat, "longitude": avg_lon}}})
            else:
                self.markets_collection.insert_one({
                    "market": mkt['market'],
                    "state": mkt['state'],
                    "marketCenter": {"latitude": avg_lat, "longitude": avg_lon}
                })
        




    def generate_forecasts(self, total_rba, hist_df, uc_sf):
        forecast_class = GetForecast
        na_forecast_model, del_forecast_model = forecast_class.match_model(total_rba)
        if na_forecast_model == None:
            return None
        na_smoothed_data, del_smoothed_data = forecast_class.data_preprocessing(total_rba, hist_df, uc_sf)
        na_forecast, del_forecast = forecast_class.forecast(na_smoothed_data, del_smoothed_data, na_forecast_model, del_forecast_model)
        na_forecast = na_forecast*total_rba/1000
        del_forecast = del_forecast*total_rba/1000
        return na_forecast, del_forecast




    def get_and_set_fips_codes(self):
        # Get unique county and state pairs using aggregation pipeline
        unique_counties_pipeline = [
            {"$group": {"_id": {"county": "$county", "state": "$state"}}},
            {"$project": {"county": "$_id.county", "state": "$_id.state", "_id": 0}}
        ]

        unique_counties = list(self.new_properties_collection.aggregate(unique_counties_pipeline))

        for cty in unique_counties:
            county = cty['county']
            county_title = cty['county'].title()
            state = cty['state']

            # Get fips code for county
            fips_code = self.counties_collection.find_one({"county": county_title, "state": state}, {"_id": 0, "fips": 1})
            if fips_code:
                fips_code = fips_code['fips']
            else:
                edge_cases = [
                    {
                        "prop_county": "St Charles", 
                        "county": "St. Charles", 
                        "state": "LA"
                    },
                    {
                        "prop_county": "Jeff n Davis",
                        "county": "Jefferson Davis",
                        "state": "LA"
                    },
                    {
                        "prop_county": "Miami/Dade", 
                        "county": "Miami-Dade",
                        "state": "FL"
                    }, 
                    {
                        "prop_county": "St Francis",
                        "county": "St. Francis",
                        "state": "AR"
                    },
                    {
                        "prop_county": "W Baton Rouge",
                        "county": "West Baton Rouge",
                        "state": "LA"
                    },
                    {
                        "prop_county": "St Tammany",
                        "county": "St. Tammany",
                        "state": "LA"
                    },
                    {
                        "prop_county": "Dona Ana", 
                        "county": "Doña Ana",
                        "state": "NM"
                    },
                    {
                        "prop_county": "De Kalb",
                        "county": "Dekalb", 
                        "state": "AL"
                    },
                    {
                        "prop_county": "E Baton Rouge",
                        "county": "East Baton Rouge",
                        "state": "LA"
                    },
                    {
                        "prop_county": "St Lucie",
                        "county": "St. Lucie",
                        "state": "FL"
                    },
                    {
                        "prop_county": "St Martin",
                        "county": "St. Martin",
                        "state": "LA"
                    }
                ]

                # Check edge cases
                for case in edge_cases:
                    if case['prop_county'] == county_title and case['state'] == state:
                        county = case['county']
                        break
                
                # Get fips code for county
                fips_code = self.counties_collection.find_one({"county": county, "state": state}, {"_id": 0, "fips": 1})
                if fips_code:
                    fips_code = fips_code['fips']

            # Update properties with fips code
            if fips_code:
                self.new_properties_collection.update_many({"county": county, "state": state}, {"$set": {"fips": fips_code}})




    ############################################################
    ############################################################
    def close_upon_error(self):
        # self.new_properties_collection.drop()
        self.client.close()



# TEST RUN
# if __name__ == '__main__':
#     cleaner = CostarCleaner()