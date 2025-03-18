import time, os, json, imaplib
import pandas as pd
# import numpy as np

from PIL import Image

import boto3
import zoom_service

from dotenv import load_dotenv


from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException



class CostaggWebscraper:
    def __init__(self, scraping_status, headless=True):
        # Load environment variables
        load_dotenv()
        self.email = os.environ['EMAIL']
        self.email_password = os.environ['EMAIL_PASSWORD']
        self.username = os.environ['COSTAR_USERNAME']
        self.password = os.environ['COSTAR_PASSWORD']
        # Load config & status of webscraping session
        self.step_list = pd.read_csv('costar/input/steps.csv', header=0)
        self.scraping_status = scraping_status
        self.saved_search_list = [search for search, status in self.scraping_status.items() if status == 0]
        if self.saved_search_list == []: exit(0) # In case there's an issue between a complete scraping session & closing the session
        self.saved_search = self.saved_search_list.pop(0)
        self.saved_search_size = -1

        # Check if the file exists and is not empty
        file_path = 'costar/logs/prop_log/' + self.saved_search + '.csv'
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            self.prop_log = pd.read_csv(file_path, header=0)
        else:
            print(f"Warning: The file '{file_path}' is empty or does not exist.")
            self.prop_log = pd.DataFrame()  # Initialize with an empty DataFrame or handle as needed

        self.init_prop_log_len = 0
        self.saved_search_downloaded = len(self.prop_log)

        # Configure Firefox and Geckodriver
        opts = FirefoxOptions()
        if headless:
            opts.add_argument("--headless")
            opts.add_argument("--width=2000")
            opts.add_argument("--height=2000")
        opts.set_preference("browser.download.dir", os.getcwd() + '/costar/data')
        opts.set_preference("browser.download.folderList", 2)
        driver_loc = "/usr/local/bin/geckodriver"
        service = FirefoxService(executable_path=driver_loc)
        self.driver = webdriver.Firefox(options=opts, service=service)
        self.action = ActionChains(self.driver)

        # Log in to email account to clear any leftover 2FA code emails
        imap = imaplib.IMAP4_SSL("imap.gmx.com")
        imap.login(self.email, self.email_password)

        imap.select("2FA")
        resp, items = imap.uid('search', None, "ALL")
        items = items[0].split()
        if items:
            for item in items:
                imap.uid('store', item, '+FLAGS', '\\Deleted')
            imap.expunge()

        
        imap.close()
        imap.logout()

        # Start browser session
        url = 'https://www.costar.com/'
        self.driver.get(url)
        if not headless:
            self.driver.maximize_window()


        # Set up some variables for later use
        self.prop_types = ["Industrial", "Flex", "Office", "Retail", "Multi-Family", "Student", "Land", "Hospitality", "Health Care", "Specialty", "Sports & Entertainment",
                           "Flex (Neighborhood Center)", "Industrial (Community Center)"]
        self.secondary_prop_types = ['Distribution', 'Manufacturing', 'Truck Terminal', 'Service', 'Warehouse', 
                                     'Airplane Hangar', 'Airport', 'Auto Salvage Facility', 'Cement/Gravel Plant',
                                     'Chemical/Oil Refinery', 'Contractor Storage Yard', 'Food Processing',
                                     'Landfill', 'Lumberyard', 'Railroad Yard', 'Refrigeration/Cold Storage',
                                     'Self-Storage', 'Shipyard', 'Showroom', 'Telecom Hotel', 'Data Hosting',
                                     'Utility Sub-Station', 'Water Treatment Facility',
                                     # Other observed values
                                     'R&D', 'Telecom Hotel/Data Hosting', 
                                     'Light Manufacturing', 'Light Distribution']
        self.by_types = dict(zip(['By.ID', 'By.NAME', 'By.XPATH', 'By.LINK_TEXT', 'By.PARTIAL_LINK_TEXT', 'By.TAG_NAME', 'By.CLASS_NAME', 'By.CSS_SELECTOR'], 
                                 [By.ID, By.NAME, By.XPATH, By.LINK_TEXT, By.PARTIAL_LINK_TEXT, By.TAG_NAME, By.CLASS_NAME, By.CSS_SELECTOR])) 


########################################################################################################################
########################################################################################################################


    ########################################
    def download_log(self, log_message):
        with open('costar/logs/download.log', 'a') as f:
            f.write(log_message+'\n')
    
    def sync_prop_log(self):
        self.prop_log.to_csv('costar/logs/prop_log/'+self.saved_search+'.csv', index=False, sep=',')

    def sync_scraping_status(self):
        json.dump(self.scraping_status, open('costar/input/scraping_status.json', 'w'))

    ########################################
    # def get_2fa_code(self):
    #     imap = imaplib.IMAP4_SSL("imap.gmx.com")
    #     imap.login(self.email, self.email_password)
    #     imap.select("2FA")

    #     resp, items = imap.uid('search', None, "ALL")
    #     items = items[0].split()

    #     email_wait_counter = 0
    #     while len(items) == 0 and email_wait_counter < 5:
    #         # print("No 2FA Code Received -- Waiting 60 seconds for email...")
    #         self.download_log("No 2FA Code Received -- Waiting 60 seconds for email...")
    #         time.sleep(60)
    #         imap.select("2FA")
    #         resp, items = imap.uid('search', None, "UNSEEN")
    #         items = items[0].split()
    #         email_wait_counter += 1
            
    #     try:
    #         latest_email_uid = items[-1]
    #     except:
    #         self.download_log("No 2FA Code Received -- Closing Driver")
    #         self.driver.close()
    #         self.driver.quit()
    #         exit(1)
                
    #     resp, email_data = imap.uid('fetch', latest_email_uid, "(BODY[TEXT])")
    #     raw_email = email_data[0][1].decode("utf-8")

    #     # Look for \n (6 digit number) \n in email body using regular expression
    #     code_match = re.search(r'\b\d{6}\b', raw_email)
    #     if code_match:
    #         two_factor_code = code_match.group(0)
    #     else:
    #         self.download_log("No 2FA Code Received -- Closing Driver")
    #         self.driver.close()
    #         self.driver.quit()
    #         exit(1)

    #     return two_factor_code



    ########################################
    def interference_check(self, nl_step="", table_check=False, expected_start_url=None):

        tic = time.perf_counter()

        if table_check:
            if self.driver.current_url != "https://product.costar.com/search/all-properties/list-view/properties":
                self.download_log(f"\n!!! REDIRECTED TO NEW URL: {self.driver.current_url}\n")
                self.driver.get("https://product.costar.com/search/all-properties/list-view/properties")
                time.sleep(2)
                self.driver.refresh()
                time.sleep(5)
        
        if expected_start_url:
            if self.driver.current_url != expected_start_url:
                self.download_log(f"\n!!! REDIRECTED TO NEW URL: {self.driver.current_url}\n")
                self.driver.get(expected_start_url)
                time.sleep(2)
                self.driver.refresh()
                time.sleep(5)

        if (self.driver.find_elements(by=By.ID, value="username") or self.driver.find_elements(by=By.ID, value="password")) \
            and nl_step not in ["Fill username", "Fill password", "Click login button"]:
            self.download_log(f"\n!!! REDIRECTED TO LOGIN PAGE AT STEP: {nl_step}\n")
            try:
                if self.driver.find_elements(by=By.ID, value="username"):
                    username_field = self.driver.find_element(by=By.ID, value="username")
                    username_field.send_keys(self.username)
                if self.driver.find_elements(by=By.ID, value="password"):
                    password_field = self.driver.find_element(by=By.ID, value="password")
                    password_field.send_keys(self.password)
                    password_field.send_keys(Keys.ENTER)
                time.sleep(3)
                if self.driver.find_elements(by=By.ID, value="code"):
                    self.download_log("2FA Code Requested -- Waiting 30 seconds for email...")
                    time.sleep(30)

                    two_factor_code = zoom_service.get_2fa_code()

                    self.step("Fill 2FA code", keys=two_factor_code, wait_time=1)
                    self.step("Click confirm 2FA button", wait_time=1)
            except Exception:
                pass
        # If redirected to homepage, try going back to previous page
        if self.driver.current_url == "https://product.costar.com/home/"\
            and nl_step not in ["Click CoStar Icon for Homepage", "Click confirm 2FA button"]:
            self.download_log(f"\n!!! REDIRECTED TO HOMEPAGE AT STEP: {nl_step}\n")
            self.driver.back()
            time.sleep(2)
        # Click Accept if given Terms of Service Page
        if self.driver.find_elements(by=By.XPATH, value="//button[contains(.,'Accept')]"):
            self.download_log(f"\n!!! TERMS OF SERVICE POP UP AT STEP: {nl_step}\n")
            try:
                accept_tos_button = self.driver.find_element(by=By.XPATH, value="//button[contains(.,'Accept')]")
                accept_tos_button.click()
                time.sleep(2)
            except Exception:
                pass
        # Dismiss Helper Guide
        if self.driver.find_elements(by=By.XPATH, value="//button[contains(.,'Ok')]"):
            self.download_log(f"\n!!! HELPER GUIDE POP UP AT STEP: {nl_step}\n")
            try:
                close_button = self.driver.find_element(by=By.XPATH, value="//button[contains(.,'Ok')]")
                close_button.click()
                time.sleep(2)
            except Exception:
                pass
        # Dismiss Webinar Pop-Up
        if self.driver.find_elements(by=By.XPATH, value="//button[contains(.,'Never Interested')]"):
            self.download_log(f"\n!!! WEBINAR POP UP AT STEP: {nl_step}\n")
            try:
                decline_webinar = self.driver.find_element(by=By.XPATH, value="//button[contains(.,'Never Interested')]")
                decline_webinar.click()
                time.sleep(2)
            except Exception:
                pass
        
        # Dismiss "Exceeded Maximum Activity Duration" pop-up
        if self.driver.find_elements(by=By.XPATH, value="//button[contains(.,'Okay, got it')]"):
            self.download_log(f"\n!!! MAX ACTIVITY DURATION POP UP AT STEP: {nl_step}\n")
            try:
                close_button = self.driver.find_element(by=By.XPATH, value="//button[contains(.,'Okay, got it')]")
                close_button.click()
                time.sleep(2)
            except Exception:
                pass
        
        toc = time.perf_counter()
    


    ########################################
    def step(self, 
             nl_step, 
             wait_time=None, 
             webel_id=None, 
             keys=None, 
             end_on_fail=True, 
             multiple_tries=True,
             expected_start_url=None):
        tic = time.perf_counter()
        # Wait for specified time before executing step
        if wait_time:
            time.sleep(wait_time)

        # Get step details from input & steps.csv
        current_step = self.step_list[self.step_list['description'] == nl_step]
        webel_by = self.by_types[current_step['by'].values[0]]

        if not webel_id:
            webel_id = current_step['value'].values[0]

        if not keys:
            keys = current_step['keys'].values[0]
        
        action = current_step['action'].values[0]

        self.interference_check(nl_step, expected_start_url=expected_start_url)

        # Main routine
        if multiple_tries:
            wait_times = [5, 10, 15]
        else:
            wait_times = [10]

        for wait_timeout in wait_times:
            inner_tic = time.perf_counter()
            try:
                webelement = wait(self.driver, wait_timeout).until(EC.presence_of_element_located((webel_by, webel_id)))
                if action == 'click':
                    webelement.click()
                else:
                    webelement.send_keys(keys)
                inner_toc = time.perf_counter()
                toc = time.perf_counter()
                return
            except Exception as e:
                if nl_step in ["Fill 2FA code", "Click confirm 2FA button", "Select given saved search"]:
                    self.download_log(f"Exception at \'{nl_step}\' -- Trying Again...\n")
                    continue
                else:
                    self.download_log(f"Exception at \'{nl_step}\' -- Trying Again...\n")
                    time.sleep(2)
                    self.download_log(f"\nURL at Exception: {self.driver.current_url}\n")
                    self.driver.refresh()
                    time.sleep(5)
                    self.interference_check(nl_step, expected_start_url=expected_start_url)
                inner_toc = time.perf_counter()
        
        # Analytics tab and Data tab steps have other measures in place to mitigate problems. 
        # See Download_And_Delete_Hist() for more details.
        if end_on_fail:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            # self.driver.save_screenshot(f'costar/logs/screenshots/FAILED_{nl_step}_{timestamp}.png')
            toc = time.perf_counter()
            self.download_log(f"###### (FAILED STEP) {nl_step} Time: {toc - tic:0.4f} seconds")
            self.download_log("STEP FUNCTION HAS FAILED, CLOSING DRIVER")
            self.driver.close()
            self.driver.quit()
            exit(1)
        else:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            # self.driver.save_screenshot(f'costar/logs/screenshots/FAILED_{nl_step}_{timestamp}.png')
            toc = time.perf_counter()
            self.download_log(f"###### (FAILED STEP) {nl_step} Time: {toc - tic:0.4f} seconds")
            self.download_log(f"###### CONTINUING WITHOUT SUCCESSFUL STEP\n")


    ########################################
    def populate_prop_log(self):
        # Get "PropertyID", "Property Address", and "Property Name" columns from present data
        present_data = pd.read_excel(f'costar/data/{self.saved_search}/{self.saved_search}.xlsx', engine='openpyxl', dtype={'PropertyID': str})
        present_data = present_data[['PropertyID', 'Property Address', 'Property Name']]
        present_data['Property Name'].fillna('', inplace=True)
        present_data['PropertyID'] = present_data['PropertyID'].apply(lambda x: str(int(x)))

        # Fill prop log columns Address, Building, and ID with data from present data
        self.prop_log['Address'] = present_data['Property Address']
        self.prop_log['Building'] = present_data['Property Name']      
        self.prop_log['ID'] = present_data['PropertyID']
        self.prop_log['Complete'] = False

        self.init_prop_log_len = len(self.prop_log)
        self.sync_prop_log()

    
    ########################################
    def check_for_saved_image(self, costarID):
        load_dotenv()
        s3 = boto3.resource('s3')
        bucket_name = 'costar-images'
        file_name = f'{costarID}.jpg'
        try:
            s3.Object(bucket_name, file_name).load()
        except Exception as e:
            return False
        return True


    def standardize_image(self, image_path):
        # Open image
        img = Image.open(image_path)
        # Get image size
        width, height = img.size
        # Resize image to 16:9 aspect ratio
        if width/height < 16/9:
            # Crop top and bottom of image to bring to 16:9 aspect ratio
            ideal_height  = width * (9 / 16)
            crop_length = (height - ideal_height) / 2
            img = img.crop((0, crop_length, width, height - crop_length))
        elif width/height > 16/9:
            # Crop sides of image to bring to 16:9 aspect ratio
            ideal_width = height * (16 / 9)
            crop_length = (width - ideal_width) / 2
            img = img.crop((crop_length, 0, width - crop_length, height))
        else:
            # Image is already 16:9 aspect ratio
            pass

        # # Downscale image to 1920x1080
        img.thumbnail((1920, 1080))

        # Save image as jpg
        if image_path.split(".")[-1] == "png":
            img = img.convert('RGB')
            os.remove(image_path)
            image_path = image_path.replace(".png", ".jpg")
        img.save(image_path, 'JPEG', quality=100)


    def post_image_to_s3(self, costarID):
        load_dotenv()
        s3 = boto3.resource('s3')
        bucket_name = 'costar-images'
        file_name = f'{costarID}.jpg'
        s3.meta.client.upload_file(f'costar/images/{costarID}.jpg', bucket_name, file_name)
        self.download_log(f"### Image Uploaded to S3 ({costarID})")


    def get_property_image(self, costarID):
        # Check for leftover images (if there are any jpg or png files in costar/data, delete them)
        for file in os.listdir('costar/data'):
            if file.endswith('.jpg') or file.endswith('.png'):
                os.remove(f'costar/data/{file}')

        # CHECK S3 TO SEE IF IMAGE ALREADY EXISTS
        image_exists = self.check_for_saved_image(costarID)
        if image_exists:
            self.download_log(f"### Image Already Exists ({costarID})")
            return
        else:
            self.driver.get(f"https://product.costar.com/detail/all-properties/{costarID}/summary")
            time.sleep(5)

        try:
            if self.driver.find_elements(By.XPATH, "//img[@id='0']"):
                img = self.driver.find_element(By.XPATH, "//img[@id='0']")
                img.click()
                time.sleep(2)
                if self.driver.find_elements(By.CSS_SELECTOR, ".carousel__carousel-toolbar-button--JUXMS:nth-child(1) > span"):
                    img_download_button = self.driver.find_element(By.CSS_SELECTOR, ".carousel__carousel-toolbar-button--JUXMS:nth-child(1) > span")
                    img_download_button.click()
                    download_wait_counter = 0
                    while not (os.path.exists('costar/data/PrimaryPhoto.jpg') 
                               or os.path.exists('costar/data/PlatMap.jpg') 
                               or os.path.exists('costar/data/PrimaryPhoto.png') 
                               or os.path.exists('costar/data/PlatMap.png')) and download_wait_counter < 20:
                        time.sleep(5)
                        download_wait_counter += 5
                    if download_wait_counter == 20:
                        self.download_log(f"!!! Image Download Timeout ({costarID})")

                    for file in os.listdir('costar/data'):
                        if file.endswith('.jpg'):
                            os.replace(f'costar/data/{file}', f'costar/images/{costarID}.jpg')
                            self.download_log(f"### Image Downloaded ({costarID})")
                            self.standardize_image(f'costar/images/{costarID}.jpg')
                            self.download_log(f"### Image Standardized ({costarID})")
                        elif file.endswith('.png'):
                            os.replace(f'costar/data/{file}', f'costar/images/{costarID}.png')
                            self.download_log(f"### Image Downloaded ({costarID})")
                            self.standardize_image(f'costar/images/{costarID}.png')
                            self.download_log(f"### Image Standardized ({costarID})")
                    
                    # POST IMAGE TO S3
                    self.post_image_to_s3(costarID)

            else:
                self.download_log(f"!!! No Image Found ({costarID})")
        except Exception as e:
            self.download_log(f"!!! Image Download Exception ({costarID}) \n{e}")



########################################################################################################################
########################################################################################################################


    ########################################
    def Login_To_Homepage(self):
        self.step("Navigate to login page", wait_time=1)
        self.step("Fill username", keys=self.username, wait_time=1)
        self.step("Fill password", keys=self.password, wait_time=1)
        self.step("Click login button", wait_time=1)

        self.download_log("Login Credentials Accepted, awaiting 2FA code...")

        # If CoStar asks for 2FA code, email it to costagg_2fa@outlook.com
        if self.driver.find_elements(By.ID, "code"):
            self.download_log("2FA Code Requested -- Waiting 30 seconds for email...")
            time.sleep(30)

            two_factor_code = zoom_service.get_2fa_code()

            self.step("Fill 2FA code", keys=two_factor_code, wait_time=1)
            self.step("Click confirm 2FA button", wait_time=1)
            self.download_log("2FA Code Accepted -- Logged in to CoStar\n")



    ########################################
    def Homepage_To_Data_Collection(self):
        # Unexpected Pop-Up Handling
        time.sleep(2)
        popup_close_button = self.driver.find_elements(by=By.CLASS_NAME, value="_pendo-close-guide")
        if popup_close_button:
            popup_close_button[0].click()
        # If prop_log exists, then return to immediately resume historical data collection
        if not self.prop_log.empty:
            return True
        self.driver.get('https://product.costar.com/search/all-properties/list-view/properties')
        self.step("Open saved search dropdown menu", webel_id="//button[contains(.,'Save')]", wait_time=2)
        self.step("Open saved search full page menu", wait_time=2)
        self.step("Search for given saved search", keys=self.saved_search, wait_time=3)
        self.step("Select given saved search", webel_id=self.saved_search, wait_time=3)
        self.step("Select list view for saved search results", wait_time=3)

        # Saved search validation
        time.sleep(5)
        wait_counter = 0
        while not self.driver.find_elements(By.CSS_SELECTOR, ".css-uui-swygdm") and wait_counter < 60:
            try:
                self.driver.refresh()
            except Exception:
                self.driver.get('https://product.costar.com/search/all-properties/list-view/properties')
                time.sleep(5)
            time.sleep(5)
            wait_counter += 5
        if not self.driver.find_elements(By.CSS_SELECTOR, ".css-uui-swygdm"):
            self.download_log("ERROR: Saved Search Validation Failed -- Closing Driver")
            self.driver.close()
            self.driver.quit()
            exit(1)
        num_query_results = self.driver.find_elements(By.CSS_SELECTOR, ".css-uui-swygdm")[0].text.split(" ")[0]
        num_query_results = num_query_results.replace(",", "")
        num_query_results = int(num_query_results)
        self.saved_search_size = num_query_results
        self.saved_search_downloaded = len(self.prop_log)
        if num_query_results > 499:
            self.download_log(f"ERROR: Results exceed 499. Break down \'{self.saved_search}\' into two separate searches.")
            self.scraping_status[self.saved_search] = -1
            self.sync_scraping_status()
            return False
        elif num_query_results > 450:
            self.download_log(f'WARNING: Results exceed 450. Consider breaking \'{self.saved_search}\' into two separate searches.')
  
        # Present Data Download
        if not os.path.exists(f'costar/data/{self.saved_search}/{self.saved_search}.xlsx'):
            self.step("Open More dropdown menu", wait_time=2)
            self.step("Click first present data Export button", wait_time=2)
            self.step("Open saved export formats dropdown menu", wait_time=2)
            self.step("Select Industrial Data Project", wait_time=2)
            self.step("Initiate present data export", wait_time=2)

            # Wait for the download to complete
            wait_timer = 0
            while not os.path.exists('costar/data/CostarExport.xlsx') and wait_timer < 300:
                self.download_log("Waiting for present data download...")
                time.sleep(2)
                wait_timer += 2

            # Check if the file was downloaded successfully
            if not os.path.exists('costar/data/CostarExport.xlsx'):
                self.download_log("Present data download stalled. Closing driver")
                exit(1)

            # Move the file to the desired location
            os.replace('costar/data/CostarExport.xlsx', f'costar/data/{self.saved_search}/{self.saved_search}.xlsx')
            present_data = pd.read_excel(f'costar/data/{self.saved_search}/{self.saved_search}.xlsx', engine='openpyxl')
            present_data['Property Class'] = ''
            present_data['Rent'] = ''
            present_data['Property Type'] = ''
            present_data.to_excel(f'costar/data/{self.saved_search}/{self.saved_search}.xlsx', engine='openpyxl', index=False)
            self.download_log(f'\n############################################################\n###  Successful Present Data Download for {self.saved_search}!\n############################################################\n')

        # Continue to historical data collection
        return True



    ########################################
    def Get_Historical_Data(self):

        # If prop log is empty, then populate it with data from present data
        if self.prop_log.empty:
            self.populate_prop_log()
        else:
            self.init_prop_log_len = len(self.prop_log)


        # First pass to get data for all properties

        tic = time.perf_counter()

        # Iterate over rows of self.prop_log
        # num_deleted = 0
        for index, row in self.prop_log[self.prop_log['Complete'] == False].iterrows():
            address = row['Address']
            building = row['Building']
            costarID = row['ID']
            download_success = False
            attempts = 0
            while not download_success and attempts < 2:
                try:
                    attempts += 1

                    self.get_property_image(costarID)

                    self.driver.get(f"https://product.costar.com/detail/all-properties/{costarID}/analytics")

                    # FAIL CHECK
                    # Check if text "Analytics data is not available for this property" is present on the page
                    # If so, then skip the property immediately and save it for later
                    if self.driver.find_elements(by=By.XPATH, value="//span[contains(.,'Analytic data is not available for this property.')]"):
                        self.download_log(f"\n!!! NO DATA FOR {address}, {building} ({costarID}) -- {index+1}/{self.init_prop_log_len}\n")
                        self.sync_prop_log()
                        break
            

                    # self.step("Navigate to Analytics tab", wait_time=1, end_on_fail=False, multiple_tries=False,
                    #           expected_start_url=f"https://product.costar.com/detail/all-properties/{costarID}/summary")

                    
                    self.step("Navigate to Data tab", wait_time=2, end_on_fail=False, multiple_tries=False,
                              expected_start_url=f"https://product.costar.com/detail/all-properties/{costarID}/analytics")

                    self.step("Click historical data Export button", 
                              webel_id="//button[contains(.,'Export')]", 
                              wait_time=1, end_on_fail=False, multiple_tries=False,
                              expected_start_url=f"https://product.costar.com/detail/all-properties/{costarID}/analytics/property/history")

                    # FAIL CHECK
                    # Check if "Click historical data Export button" was successful & await download
                    wait_timer = 0
                    while not os.path.exists('costar/data/PropertyDetailDataTable.xlsx') and wait_timer < 30:
                        if wait_timer != 0 and wait_timer % 6 == 0:
                            self.download_log("Waiting for download...")
                        time.sleep(2)
                        wait_timer += 2
                    
                    # FAIL CHECK
                    # If the download exceeds 5 mins, then something is wrong, so try again
                    if not os.path.exists('costar/data/PropertyDetailDataTable.xlsx'):
                        # print("Download stalled (> 30s), trying again...")
                        self.download_log("Download stalled (> 30s), trying again...")
                        self.driver.get(f"https://product.costar.com/detail/all-properties/{costarID}/analytics")
                        self.driver.refresh()
                        time.sleep(2)
                    # SUCCESS
                    # If this point is reached, then the download was successful
                    else:
                        os.replace('costar/data/PropertyDetailDataTable.xlsx', f'costar/data/{self.saved_search}/{costarID}.xlsx')
                        self.download_log(f"\n### Downloaded {address}, {building} ({costarID}) -- {index+1}/{self.init_prop_log_len}\n")
                        download_success = True
                        self.prop_log.loc[index, 'Complete'] = True
                        self.sync_prop_log()

                except Exception as e:
                    self.download_log(f'\n!!! DOWNLOAD EXCEPTION FOR {address}, {building} ({costarID}) -- {index+1}/{self.init_prop_log_len}\n')
                    self.download_log(str(e))
                    continue
        

        
        num_incomplete = len(self.prop_log[self.prop_log['Complete'] == False])
        self.download_log(f"\n########################################\n### {num_incomplete} INCOMPLETE DOWNLOADS\n########################################\n")
        
        toc = time.perf_counter()
        self.download_log("\n\n"+"#"*50)
        self.download_log(f"### Get Data Time: {toc - tic:0.4f} seconds")
        self.download_log("#"*50+"\n\n")
        self.download_log(f"########################################\n### NUMBER OF PROPERTIES DOWNLOADED: {len(self.prop_log[self.prop_log['Complete'] == True])}\n########################################")



    ########################################
    def Get_Completion_Status(self):
        if not self.saved_search_list:
            return True
        else:
            self.saved_search = self.saved_search_list.pop(0)
            self.prop_log = pd.read_csv('costar/logs/prop_log/'+self.saved_search+'.csv', header=0)
            self.saved_search_downloaded = len(self.prop_log)
            return False



    ########################################
    def Reset_Webscraping_Session(self):
        self.scraping_status[self.saved_search] = 1
        self.sync_scraping_status()
        # self.step("Click CoStar Icon for Homepage", wait_time=2)
        time.sleep(2)



    ########################################
    def Close_Webscraping_Session(self):
        self.driver.close()
        self.driver.quit()
