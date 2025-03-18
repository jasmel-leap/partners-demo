from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
import pandas as pd
import numpy as np
import time, io, os, requests
from datetime import datetime
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



############################################################
def write_to_log_file(message):
    with open("apto/apto_collection_log.log", "a") as f:
        f.write(str(message) + "\n")

def write_to_google_error_log(error_line):
    error_df = pd.read_csv("apto/google_error_log.csv")
    error_df.loc[len(error_df)] = error_line
    error_df.to_csv("apto/google_error_log.csv", index=False)

def send_missing_costarID_email(aptoID):
    load_dotenv()
    smtp_server = "mail.gmx.com"
    port = 587
    from_email = os.environ.get("EMAIL")
    from_email_password = os.environ.get("EMAIL_PASSWORD")
    to_email = "clark.mask123@gmail.com"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = f"Costar ID Missing ({aptoID})"

    body = f"Costar ID missing for Apto comp with ID: {aptoID}."
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(smtp_server, port)
    server.ehlo()
    server.starttls()
    server.login(from_email, from_email_password)
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()




############################################################
def get_apto_comps():
    load_dotenv()
    salesforce_username = os.environ.get("SALESFORCE_USERNAME")
    salesforce_password = os.environ.get("SALESFORCE_PASSWORD")
    salesforce_security_token = os.environ.get("SALESFORCE_SECURITY_TOKEN")

    sf = Salesforce(username=salesforce_username,  
                    password=salesforce_password, 
                    security_token=salesforce_security_token)

    bulk = SalesforceBulk(username=salesforce_username,  
                        password=salesforce_password, 
                        security_token=salesforce_security_token)

    # Get Fields from "Comp Search" object
    comp_search_report_id = '00OKa000003AxtcMAC'
    report = sf.restful(f'analytics/reports/{comp_search_report_id}')
    comp_search_columns = [col.split(".")[1] for col in report['reportMetadata']['detailColumns']]
    comp_search_fields = ""
    for col in comp_search_columns:
        # FOR SOME REASON, REPORT RETURNS FIELD THAT IS NOT PRESENT IN OBJECT ("RecordType" instead of "Record_Type__c")
        if col == "RecordType":
            col = "Record_Type__c"
        comp_search_fields += f"{col},"
    comp_search_fields = comp_search_fields[:-1] # Remove trailing comma

    # Get latest apto collection date for filter
    apto_collection_dates = pd.read_csv("apto/apto_collection_dates.csv")
    apto_collection_dates.sort_values("Collection Date", ascending=False, inplace=True)
    if apto_collection_dates.empty:
        latest_collection_date = "1900-01-01"
    else:
        latest_collection_date = apto_collection_dates.iloc[0]["Collection Date"]

    # Use Bulk API to query "Comp Search" object
    comp_search_soql_query = f"""
    SELECT {comp_search_fields} 
    FROM Comp_Search__c
    WHERE Property_Type_Formula__c = 'Industrial' 
    AND Created_Date__c >= {latest_collection_date}
    """
 

    comp_search_job = bulk.create_query_job("Comp_Search__c", contentType='CSV')

    comp_search_batch = bulk.query(comp_search_job, comp_search_soql_query)
    while not bulk.is_batch_done(comp_search_batch):
        time.sleep(10)

    comp_search_results = bulk.get_all_results_for_query_batch(comp_search_batch)

    # Get results into a dataframe and rename columns
    comp_search_csv_data = '\n'.join(item.read().decode() for item in comp_search_results)
    comp_search_df = pd.read_csv(io.StringIO(comp_search_csv_data))
    for col in comp_search_df.columns:
        new_col_name = col[:-3] if col[-3:] == "__c" else col
        comp_search_df.rename(columns={col: new_col_name}, inplace=True)

    return comp_search_df





############################################################
# Clean apto comps
def clean_apto_comps(raw_df):
    clean_col_names = [
        # ID Data
        "aptoID", # Name
        "costarID", # CoStar_Link
        "googleID", # Google Places API via Address
        "transactionType", # Record_Type
        "sourceType", # Record_Type
        "address", # Address
        "city", # City
        "state", # State
        "zip", # Zip_Code
        "market",    # Market_ExternalComp
        "submarket", # Sub_market
        "closeDate", # Close_Date   AND    Close_Date_External
        "closeDatetime", # Close_Date   AND    Close_Date_External
        "primaryBroker", # Primary_Broker_Name
        "transactionSF", # Square_Footage   AND   Ext_Square_Footage
        "latitude", # latitude
        "longitude", # Longitude
        "loc_geojson", # Longitude and latitude as GeoJSON
        # Property Data
        "propertyType", # Property_Type_Formula
        "propertySF", # Property_SF
        "yearBuilt", # Year_Built   AND   Year_Built_del1
        "clearHeight", # Clear_Height   AND   Max_Clear_Height
        "propertyTenancy", # Property_Tenancy
        # Lease Data
        "monthlyOperatingExpenses", # Operating_Expenses_SF_Mo
        "yearlyOperatingExpenses", # Operating_Expenses
        "monthlyBaseRentalRate", # Base_Rental_Rate_SF_Mo
        "yearlyBaseRentalRate", # Base_Rental_Rate_SF_Yr
        "monthlyAvgRentalRateGross", # Average_Rental_Rate_SF_Mo_Gross
        "yearlyAvgRentalRateGross", # Average_Rental_Rate
        "leaseType", # Lease_Type
        "directOrSublease", # Direct_Sublease
        "leaseTerm", # Lease_Term_Months
        "leaseCommencementDate", # Lease_Commencement_Date
        "leaseExpirationDate", # Lease_Expiration_Date
        "freeRent", # Free_Rent_Months
        "freeRentType", # Free_Rent_Type
        "rentEscalations", # Escalations
        # Sale Data
        "salePrice", # Sales_Price
        "salePricePerSF", # Price_SF_Formula
        "acres", # Acres
        "askingPrice", # Asking_Price
        "capRate", # CAP_Rate
        "occupancyAtListing", # Occupancy_at_Listing
        "occupancyAtClose", # Occupancy_at_Close
        # Landlord & Tenant Data
        "landlord", # Landlord
        "tenant", # Tenant
        # Misc
        "compNotes" # Comp_Notes
    ]


    clean_df = pd.DataFrame(columns=clean_col_names)

    for idx, raw_comp in raw_df.iterrows():
        clean_comp = dict(zip(clean_col_names, [np.nan]*len(clean_col_names)))

        ##########
        # ID & Location Data

        clean_comp["aptoID"] = raw_comp["Name"]

        if pd.notna(raw_comp["CoStar_Link"]):
            clean_comp["costarID"] = raw_comp["CoStar_Link"].split("/")[-2]
        else:
            write_to_log_file(f"CoStar ID not found for {clean_comp['aptoID']}")

        if "Sale" in raw_comp["Record_Type"]:
            clean_comp["transactionType"] = "Sale"
        else:
            clean_comp["transactionType"] = "Lease"
        
        if "(External)" in raw_comp["Record_Type"]:
            clean_comp["sourceType"] = "External"
        else:
            clean_comp["sourceType"] = "Internal"
        
        if raw_comp["Address"] == "Not Provided":
            continue
        else:
            clean_comp["address"] = raw_comp["Address"]
        
        clean_comp["city"] = raw_comp["City"]

        clean_comp["state"] = raw_comp["State"]

        if pd.notna(raw_comp["Zip_Code"]) and raw_comp['Zip_Code'] != 0:
            clean_comp["zip"] = str(raw_comp["Zip_Code"])[:5]
        
        clean_comp["market"] = raw_comp["Market_ExternalComp"]

        clean_comp["submarket"] = raw_comp["Sub_market"] 
        clean_comp["latitude"] = float(raw_comp["latitude"])
        clean_comp["longitude"] = float(raw_comp["Longitude"])

        # Get google data
        google_data = google_data_validation(clean_comp["address"], clean_comp["city"], clean_comp["state"], clean_comp["zip"])
        if google_data:
            clean_comp["googleID"] = google_data["googleID"]
            clean_comp["latitude"] = google_data["latitude"]
            clean_comp["longitude"] = google_data["longitude"]
            clean_comp["address"] = google_data["address"]
            clean_comp["city"] = google_data["city"]
            clean_comp["state"] = google_data["state"]
            clean_comp["zip"] = google_data["zip"]

        if pd.notna(clean_comp['latitude']) and pd.notna(clean_comp['longitude']):
            clean_comp["loc_geojson"] = {
                "type": "Point",
                "coordinates": [clean_comp['longitude'], clean_comp['latitude']]
            }

        if pd.notna(raw_comp["Close_Date"]) and pd.notna(raw_comp["Close_Date_External"]) and raw_comp["Close_Date"] != raw_comp["Close_Date_External"]:
            write_to_log_file(f"Close Date mismatch at {clean_comp['address']}, {clean_comp['city']}, {clean_comp['state']}: {raw_comp['Close_Date']} vs {raw_comp['Close_Date_External']}")
        if pd.notna(raw_comp["Close_Date"]):
            clean_comp["closeDate"] = raw_comp["Close_Date"]
            clean_comp["closeDatetime"] = pd.to_datetime(raw_comp["Close_Date"])
        elif pd.notna(raw_comp["Close_Date_External"]):
            clean_comp["closeDate"] = raw_comp["Close_Date_External"]
            clean_comp["closeDatetime"] = pd.to_datetime(raw_comp["Close_Date_External"])

    
        if raw_comp['Primary_Broker_Name'] != ' ':
            clean_comp["primaryBroker"] = raw_comp["Primary_Broker_Name"]
        
        if pd.notna(raw_comp["Square_Footage"]) and pd.notna(raw_comp["Ext_Square_Footage"]) and raw_comp["Square_Footage"] != raw_comp["Ext_Square_Footage"]:
            write_to_log_file(f"Transaction SF mismatch at {clean_comp['address']}, {clean_comp['city']}, {clean_comp['state']}: {raw_comp['Square_Footage']} vs {raw_comp['Ext_Square_Footage']}")
        if pd.notna(raw_comp["Square_Footage"]):
            clean_comp["transactionSF"] = int(raw_comp["Square_Footage"])
        elif pd.notna(raw_comp["Ext_Square_Footage"]):
            clean_comp["transactionSF"] = int(raw_comp["Ext_Square_Footage"])


        ##########
        # Property Data
        clean_comp["propertyType"] = raw_comp["Property_Type_Formula"]
        if pd.notna(raw_comp["Property_SF"]):
            clean_comp["propertySF"] = int(raw_comp["Property_SF"])
        if pd.notna(raw_comp["Year_Built"]):
            clean_comp["yearBuilt"] = int(raw_comp["Year_Built"])

        if pd.notna(raw_comp["Clear_Height"]) and pd.notna(raw_comp["Max_Clear_Height"]) and raw_comp["Clear_Height"] != raw_comp["Max_Clear_Height"]:
            write_to_log_file(f"Clear Height mismatch at {clean_comp['address']}, {clean_comp['city']}, {clean_comp['state']}: {raw_comp['Clear_Height']} vs {raw_comp['Max_Clear_Height']}")
        if pd.notna(raw_comp["Clear_Height"]):
            clean_comp["clearHeight"] = raw_comp["Clear_Height"]
        elif pd.notna(raw_comp["Max_Clear_Height"]):
            clean_comp["clearHeight"] = raw_comp["Max_Clear_Height"]
        
        clean_comp["propertyTenancy"] = raw_comp["Property_Tenancy"]


        ##########
        # Lease Data
        if pd.notna(raw_comp["Operating_Expenses_SF_Mo"]):
            clean_comp["monthlyOperatingExpenses"] = float(raw_comp["Operating_Expenses_SF_Mo"])
        if pd.notna(raw_comp["Operating_Expenses"]):
            clean_comp["yearlyOperatingExpenses"] = float(raw_comp["Operating_Expenses"])
        if pd.notna(raw_comp["Base_Rental_Rate_SF_Mo"]):
            clean_comp["monthlyBaseRentalRate"] = float(raw_comp["Base_Rental_Rate_SF_Mo"])
        if pd.notna(raw_comp["Base_Rental_Rate_SF_Yr"]):
            clean_comp["yearlyBaseRentalRate"] = float(raw_comp["Base_Rental_Rate_SF_Yr"])
        if pd.notna(raw_comp["Average_Rental_Rate_SF_Mo_Gross"]):
            clean_comp["monthlyAvgRentalRateGross"] = float(raw_comp["Average_Rental_Rate_SF_Mo_Gross"])
        if pd.notna(raw_comp["Average_Rental_Rate"]):
            clean_comp["yearlyAvgRentalRateGross"] = float(raw_comp["Average_Rental_Rate"])
        clean_comp["leaseType"] = raw_comp["Lease_Type"]
        clean_comp["directOrSublease"] = raw_comp["Direct_Sublease"]
        if pd.notna(raw_comp["Lease_Term_Months"]):
            clean_comp["leaseTerm"] = float(raw_comp["Lease_Term_Months"])
        clean_comp["leaseCommencementDate"] = raw_comp["Lease_Commencement_Date"]
        clean_comp["leaseExpirationDate"] = raw_comp["Lease_Expiration_Date"]
        if pd.notna(raw_comp["Free_Rent_Months"]):
            clean_comp["freeRent"] = float(raw_comp["Free_Rent_Months"])
        clean_comp["freeRentType"] = raw_comp["Free_Rent_Type"]
        clean_comp["rentEscalations"] = raw_comp["Escalations"]


        ##########
        # Sale Data
        if pd.notna(raw_comp["Sales_Price"]):
            clean_comp["salePrice"] = float(raw_comp["Sales_Price"])
        if pd.notna(raw_comp["Price_SF_Formula"]):
            clean_comp["salePricePerSF"] = float(raw_comp["Price_SF_Formula"])
        if pd.notna(raw_comp["Acres"]):
            clean_comp["acres"] = float(raw_comp["Acres"])
        if pd.notna(raw_comp["Asking_Price"]):
            clean_comp["askingPrice"] = float(raw_comp["Asking_Price"])
        if pd.notna(raw_comp["CAP_Rate"]):
            clean_comp["capRate"] = float(raw_comp["CAP_Rate"])
        if pd.notna(raw_comp["Occupancy_at_Listing"]):
            clean_comp["occupancyAtListing"] = float(raw_comp["Occupancy_at_Listing"])
        if pd.notna(raw_comp["Occupancy_at_Close"]):
            clean_comp["occupancyAtClose"] = float(raw_comp["Occupancy_at_Close"])


        ##########
        # Landlord & Tenant Data
        if pd.notna(raw_comp["Landlord"]) and pd.notna(raw_comp["Landlord_Company"]):
            if raw_comp["Landlord"] not in ["-", "Unknown", " "]:
                clean_comp["landlord"] = raw_comp["Landlord"]
            elif raw_comp["Landlord_Company"] not in ["-", "Unknown", " "]:
                clean_comp["landlord"] = raw_comp["Landlord_Company"]
        elif pd.notna(raw_comp["Landlord"]) and not pd.notna(raw_comp["Landlord_Company"]):
            if raw_comp["Landlord"] not in ["-", "Unknown", " "]:
                clean_comp["landlord"] = raw_comp["Landlord"]
        elif pd.notna(raw_comp["Landlord_Company"]) and not pd.notna(raw_comp["Landlord"]):
            if raw_comp["Landlord_Company"] not in ["-", "Unknown", " "]:
                clean_comp["landlord"] = raw_comp["Landlord_Company"]

        if pd.notna(raw_comp["Tenant"]) and pd.notna(raw_comp["Tenant_Company_External"]):
            if raw_comp["Tenant"] not in ["-", "Unknown", " "]:
                clean_comp["tenant"] = raw_comp["Tenant"]
            elif raw_comp["Tenant_Company_External"] not in ["-", "Unknown", " "]:
                clean_comp["tenant"] = raw_comp["Tenant_Company_External"]
        elif pd.notna(raw_comp["Tenant"]) and not pd.notna(raw_comp["Tenant_Company_External"]):
            if raw_comp["Tenant"] not in ["-", "Unknown", " "]:
                clean_comp["tenant"] = raw_comp["Tenant"]
        elif pd.notna(raw_comp["Tenant_Company_External"]) and not pd.notna(raw_comp["Tenant"]):
            if raw_comp["Tenant_Company_External"] not in ["-", "Unknown", " "]:
                clean_comp["tenant"] = raw_comp["Tenant_Company_External"]



        

        ##########
        # Misc
        clean_comp["compNotes"] = raw_comp["Comp_Notes"]

        clean_df.loc[len(clean_df)] = clean_comp

    return clean_df





############################################################
# Validate comp location data using Google Places API
def google_data_validation(address, city, state, zip):
    load_dotenv()
    google_api_key = os.environ["GOOGLE_API_KEY"]

    # Collate address data (address, city, state, zip)
    formatted_raw_address = address
    if not pd.isna(city):
        formatted_raw_address += f", {city}"
    if not pd.isna(state):
        formatted_raw_address += f", {state}"
    if not pd.isna(zip):
        formatted_raw_address += f", {zip}"

    # Fetch Google Places API data
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={formatted_raw_address}&key={google_api_key}"
    try:
        response = requests.get(url)
        data = response.json()
        if data['status'] != "OK":
            error_line = {
                "Error": data['status'],
                "Exception": None,
                "Comp Address": f"{address}, {city}, {state} {zip}",
                "Google Response": data
            }
            write_to_google_error_log(error_line)
            return None
    except Exception as e:
        error_line = {
            "Error": "EXCEPTION",
            "Exception": e,
            "Comp Address": f"{address}, {city}, {state}, {zip}",
            "Google Response": None
        }
        write_to_google_error_log(error_line)
        return None

    results = data['results'][0]
    address_components = results['address_components']
    # Get all address component types to ensure street number and route are present
    component_types = [component["types"] for component in address_components]
    component_types = [item for sublist in component_types for item in sublist]
    if "street_number" not in component_types or "route" not in component_types:
        error_line = {
            "Error": "NO ADDRESS RETURNED",
            "Exception": None,
            "Comp Address": f"{address}, {city}, {state}, {zip}",
            "Google Response": address_components
        }
        write_to_google_error_log(error_line)
        return None
    
    # Unpack results returned by google api for address, city, state, zip
    google_address = ""
    for component in address_components:
        if "street_number" in component["types"]:
            google_address += component["long_name"] + " "
        if "route" in component["types"]:
            google_address += component["long_name"]
        if "locality" in component["types"]:
            city = component["long_name"]
        if "administrative_area_level_1" in component["types"]:
            state = component["short_name"]
        if "postal_code" in component["types"]:
            zip = component["short_name"]

    google_data = {
        "googleID": results['place_id'],
        'latitude': results['geometry']['location']['lat'],
        'longitude': results['geometry']['location']['lng'],
        'address': google_address,
        'city': city, 
        'state': state,
        'zip': zip
    }
    return google_data





############################################################
# Post apto comps
def post_apto_comps(df):
    load_dotenv()
    mongo_connection_str = os.environ.get("PARTNERSDB_URI")
    client = MongoClient(mongo_connection_str, tlsCAFile=certifi.where())
    db = client['partners-edge']
    collection = db['apto_comps']

    for idx, comp in df.iterrows():
        # Convert to dictionary
        comp_dict = comp.to_dict()
        # Check if any values are nan, and remove them
        comp_dict = {k: v for k, v in comp_dict.items() if pd.notna(v)}
        # Check if comp_dict['apto_id'] already exists in collection
        apto_id = comp_dict['aptoID']
        if collection.count_documents({"aptoID": apto_id}) == 0:
            collection.insert_one(comp_dict)
    
    apto_collection_dates = pd.read_csv("apto/apto_collection_dates.csv")
    current_date = datetime.now().strftime("%Y-%m-%d")
    apto_collection_dates.loc[len(apto_collection_dates)] = current_date
    apto_collection_dates.to_csv("apto/apto_collection_dates.csv", index=False)





############################################################
# Updated saleComps and leaseComps in properties collection given a new costarID
def update_properties_comps(costarID):
    load_dotenv()
    mongo_connection_str = os.environ.get("PARTNERSDB_URI")
    client = MongoClient(mongo_connection_str, tlsCAFile=certifi.where())
    db = client['partners-edge']
    properties_collection = db['properties']
    apto_collection = db['apto_comps']

    # Get property data
    property_data = properties_collection.find_one({"costarID": costarID}, {"_id": 0, "rba": 1, "ceilingHeight": 1, "loc_geojson": 1})
    if property_data is None:
        return
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
    # Get comps near given costarID
    nearby_comp_cursor = apto_collection.find({"transactionType": "Sale", "loc_geojson": {"$near": location, "$maxDistance": distanceInMeters}}, {"_id": 0, "costarID": 1})
    nearby_comps = pd.DataFrame(nearby_comp_cursor)
    if nearby_comps.empty:
        saleComps = []
    else:
        # Get properties for each comp costarID that match rba and ceilingHeight
        nearby_comps = nearby_comps.dropna(subset=['costarID'])
        nearby_comps_costarIDs = list(nearby_comps['costarID'].unique())
        similar_properties = properties_collection.aggregate(
            [
            {
                "$match": {
                    "rba": {"$gte": rba_lower_bound, "$lte": rba_upper_bound},
                    "ceilingHeight": {"$gte": ceilingHeight_lower_bound, "$lte": ceilingHeight_upper_bound},
                    "costarID": {"$in": nearby_comps_costarIDs}
                }
            },
            {
                "$project": {"costarID": 1}
            }
            ])
        compIDs = pd.DataFrame(similar_properties)
        if compIDs.empty:
            saleComps = []
        else:
            saleComps = list(compIDs['costarID'].unique())

    ####################
    # LEASE COMPS
    nearby_comp_cursor = apto_collection.find({"transactionType": "Lease", "loc_geojson": {"$near": location, "$maxDistance": distanceInMeters}}, {"_id": 0, "costarID": 1})
    nearby_comps = pd.DataFrame(nearby_comp_cursor)
    if nearby_comps.empty:
        leaseComps = []
    else:
        # Get properties for each comp costarID that match rba and ceilingHeight
        nearby_comps = nearby_comps.dropna(subset=['costarID'])
        nearby_comps_costarIDs = list(nearby_comps['costarID'].unique())
        similar_properties = properties_collection.aggregate(
            [
            {
                "$match": {
                    "rba": {"$gte": rba_lower_bound, "$lte": rba_upper_bound},
                    "ceilingHeight": {"$gte": ceilingHeight_lower_bound, "$lte": ceilingHeight_upper_bound},
                    "costarID": {"$in": nearby_comps_costarIDs}
                }
            },
            {
                "$project": {"costarID": 1}
            }
            ])
        compIDs = pd.DataFrame(similar_properties)
        if compIDs.empty:
            leaseComps = []
        else:
            leaseComps = list(compIDs['costarID'].unique())


    ####################
    # Update saleComps and leaseComps for nearby properties
    for compID in saleComps:
        properties_collection.update_one({"costarID": compID}, {"$addToSet": {"saleComps": costarID}})
    for compID in leaseComps:
        properties_collection.update_one({"costarID": compID}, {"$addToSet": {"leaseComps": costarID}})





############################################################
############################################################
if __name__ == '__main__':
    raw_comp_df = get_apto_comps()
    clean_comp_df = clean_apto_comps(raw_comp_df)

    post_apto_comps(clean_comp_df)

    new_costarIDs = [id for id in clean_comp_df['costarID'].unique() if pd.notna(id)]
    for costarID in new_costarIDs:
        update_properties_comps(costarID)

    
