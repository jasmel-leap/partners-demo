import os, json, subprocess, datetime, time, shutil, imaplib
from costar_cleaner import CostarCleaner
import pandas as pd

from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



############################################################
############################################################
# Email functions
############################################################
# Send an email to the email address in the .env file
def send_alert(subject, body):
    load_dotenv()
    smtp_server = "mail.gmx.com"
    port = 587
    from_email = os.environ["EMAIL"]
    from_email_password = os.environ["EMAIL_PASSWORD"]
    to_email = "clark.mask123@gmail.com"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        if os.path.exists('costar/logs/main.log'):
            error_log_filepath = 'costar/logs/main.log'
            with open(error_log_filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(error_log_filepath)
                )
                msg.attach(part)
        
        if os.path.exists('costar/logs/download.log'):
            download_log_filepath = 'costar/logs/download.log'
            with open(download_log_filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=os.path.basename(download_log_filepath)
                )
                msg.attach(part)
    except Exception as e:
        pass

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()
        server.starttls()
        server.login(from_email, from_email_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
    except Exception as e:
        pass

############################################################
# Clears the inbox that receives messages to restart the script (to be used after the script has been restarted)
def clear_resume_collection_folder():
    load_dotenv()
    email = os.environ['EMAIL']
    email_password = os.environ['EMAIL_PASSWORD']
    imap = imaplib.IMAP4_SSL('imap.gmx.com')
    imap.login(email, email_password)
    imap.select("main_run")

    resp, items = imap.uid('search', None, "ALL")
    items = items[0].split()
    if items:
        for item in items:
            imap.uid('store', item, '+FLAGS', '\\Deleted')
        imap.expunge()
        
    imap.close()
    imap.logout()


############################################################
# Waits for an email to restart the script after a crash
def await_restart_cue():
    send_alert("Costagg has crashed", "Send email to pe_collection@gmx.com with subject 'start' and body 'start' to resume, or send with subject 'start' and body 'cancel' to cancel.")

    load_dotenv()
    email = os.environ['EMAIL']
    email_password = os.environ['EMAIL_PASSWORD']
    imap = imaplib.IMAP4_SSL('imap.gmx.com')
    imap.login(email, email_password)
    imap.select("main_run")

    resp, items = imap.uid('search', None, 'ALL')
    items = items[0].split()

    email_wait_counter = 0
    while len(items) == 0 and email_wait_counter < 86400:
        time.sleep(60)
        email_wait_counter += 60
        resp, items = imap.uid('search', None, 'ALL')
        items = items[0].split()
    
    try:
        latest_email = items[-1]
    except:
        send_alert("No Restart Cue Email", "No email was received to restart the collection. Shutting down data collection.")
        exit(1)
    
    resp, data = imap.uid('fetch', latest_email, '(BODY[TEXT])')
    raw_email = data[0][1].decode('utf-8')

    if 'start' in raw_email.lower():
        send_alert("Restart Cue Received", "Restart cue received. Restarting data collection.")
        clear_resume_collection_folder()
        return
    elif 'cancel' in raw_email.lower():
        send_alert("Cancel Cue Received", "Cancel cue received. Cancelling data collection.")
        exit(1)





############################################################
############################################################
# File management functions
############################################################
# Check to ensure that the necessary input files are present (input.json, steps.csv)
def check_for_input_files():
    if not os.path.exists('costar/input/input.json'):
        send_alert('Costagg Error: Missing input.json', 'Missing input.json, see docs for details.')
        exit(1)
    
    if not os.path.exists('costar/input/steps.csv'):
        send_alert('Costagg Error: Missing steps.csv', 'Missing steps.csv, see docs for details.')
        exit(1)


############################################################
# Initialize scraping_status.json and prop_log files to track scraping progress
def initialize_scraping_status_and_prop_log():
    with open('costar/input/input.json', 'r') as f:
        INPUT_FILE = json.load(f)
        SAVED_SEARCHES = INPUT_FILE['SAVED_SEARCHES']
        if not os.path.exists('costar/input/scraping_status.json'):
            SCRAPING_STATUS = dict(zip(SAVED_SEARCHES, [0]*len(SAVED_SEARCHES)))
            with open('costar/input/scraping_status.json', 'w') as f:
                json.dump(SCRAPING_STATUS, f)
            
    if not os.path.exists('costar/logs/prop_log') or not os.listdir('costar/logs/prop_log'):
        os.makedirs('costar/logs/prop_log', exist_ok=True)
        # os.makedirs('costar/logs/screenshots', exist_ok=True)
        prop_log_template = pd.DataFrame(columns=['Address', 'Building', 'ID', 'Complete'])
        for S in SAVED_SEARCHES:
            prop_log_template.to_csv(f'costar/logs/prop_log/{S}.csv', index=False, sep=',')
            os.makedirs(f'costar/data/{S}', exist_ok=True)


############################################################
# Initialize logs and data directories to log scraping activity and store data
def intialize_logs_and_data_dirs():
    if not os.path.exists('costar/logs'):
        os.makedirs('costar/logs', exist_ok=True)
    
    if not os.path.exists('costar/data'):
        os.makedirs('costar/data', exist_ok=True)
    
    if not os.path.exists('costar/imges'):
        os.makedirs('costar/images', exist_ok=True)

    if not os.path.exists('costar/logs/main.log'):
        with open('costar/logs/main.log', 'w') as f:
            f.write("Main Log\n")
            f.write("--------------------\n")
            f.write("--------------------\n")
    if not os.path.exists('costar/logs/download.log'):
        with open('costar/logs/download.log', 'w') as f:
            f.write("Download Log\n")
            f.write("--------------------\n")
            f.write("--------------------\n")

############################################################
# Clear session data
def clear_session_data():
    pass
    # Delete scraping_status.json
    os.remove('costar/input/scraping_status.json')
    # Delete logs directory
    shutil.rmtree('costar/logs')
    # Delete data dir
    shutil.rmtree('costar/data')
    # Delete image dir
    shutil.rmtree('costar/images')





############################################################
############################################################
# Main run for costar collection
if __name__ == '__main__':


    check_for_input_files()
    initialize_scraping_status_and_prop_log()
    intialize_logs_and_data_dirs()
    clear_resume_collection_folder()


    # PROGRAM IS READY TO BEGIN OR RESUME RUNNING


    # Run webscraping script, and await restart cue to rerun if script crashes
    with open('costar/logs/main.log', 'a') as error_log:
        webscraping_script = subprocess.run(" ".join(['python3', 'costar/src/webscraping.py']), shell=True, stderr=error_log)
        while webscraping_script.returncode != 0:
            print('\n##################################################')
            print('######  COSTAGG HAS CRASHED, STARTING OVER  ######')
            print('##################################################\n')
            # Ensure that previous firefox window is closed before opening another
            kill_firefox = subprocess.run(" ".join(['pkill', '-f', 'firefox']), shell=True, stderr=error_log)

            await_restart_cue()

            webscraping_script = subprocess.run(" ".join(['python3', 'costar/src/webscraping.py']), shell=True, stderr=error_log)


    # ALL NEW DATA IS IN MONGO ARCHIVE, READY TO CLEAN AND POST TO MAIN COLLECTIONS


    # cleaner = CostarCleaner()
    # try:
    #     # Clean and set new data
    #     cleaner.clean_and_set_data()

    #     cleaner.set_market_centers()

    #     # Set aggregate region data
    #     cleaner.aggregate_county_data()

    #     cleaner.aggregate_zip_data()

    #     cleaner.get_and_set_fips_codes()

    #     cleaner.update_comps()

    #     # Update collections
    #     cleaner.update_collections()

    # except Exception as e:
    #     print(e)
    #     print('Error cleaning data.')
    #     cleaner.close_upon_error()
    #     cleaner.client.close()
    #     exit(1)
    

    clear_session_data()
############################################################
############################################################