import json
from costagg_webscraper import CostaggWebscraper
from data_handler import DataHandler
from memory_profiler import profile
import shutil
import os

# @profile
def webscraping_loop():
    with open('costar/input/input.json', 'r') as f:
        INPUT_FILE = json.load(f)
        SAVED_SEARCHES = INPUT_FILE['SAVED_SEARCHES']
    
    with open('costar/input/scraping_status.json', 'r') as f:
        SCRAPING_STATUS = json.load(f)

    data_handler = DataHandler()

    # Set headless to False to see browser on screen as script runs. 
    webscraper = CostaggWebscraper(SCRAPING_STATUS, headless=False)

    webscraper.Login_To_Homepage()

    complete = False
    while not complete:
        if webscraper.Homepage_To_Data_Collection():

            webscraper.Get_Historical_Data()
            
        # data_handler.Write_Raw_Data_To_Mongo(webscraper.saved_search)

        webscraper.Reset_Webscraping_Session()
        complete = webscraper.Get_Completion_Status()

    data_handler.Close_Mongo()
    webscraper.Close_Webscraping_Session()
    
	# Delete the existing scraping_status.json file if it exists
    status_file_path = 'costar/input/scraping_status.json'
    if os.path.exists(status_file_path):
        os.remove(status_file_path)
        print(f"Deleted '{status_file_path}'.")
    

############################################################
############################################################
if __name__ == '__main__':
    # Clone the base scraping status file to the target location
    base_file_path = 'costar/input/scraping_status_base.json'
    target_file_path = 'costar/input/scraping_status.json'

    # Check if the base file exists before cloning
    if os.path.exists(base_file_path):
        shutil.copy(base_file_path, target_file_path)
        print(f"Cloned '{base_file_path}' to '{target_file_path}'.")
    else:
        print(f"Warning: The base file '{base_file_path}' does not exist. Cannot clone.")

    webscraping_loop()