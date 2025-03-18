import os, time, subprocess
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

############################################################
# Create logs for apto collection
def setup_log_file():
    if not os.path.exists("apto/apto_collection_log.log"):
        with open("apto/apto_collection_log.log", "w") as f:
            f.write("Apto Collection Log\n")
            f.write("Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
            f.write("--------------------\n")
            f.write("--------------------\n")



def setup_google_error_log():
    if not os.path.exists("apto/google_error_log.csv"):
        with open("apto/google_error_log.csv", "w") as f:
            f.write("Error,Exception,Comp Address,Google Response\n")



def email_exit_status(successful_status):
    load_dotenv()
    smtp_server = "mail.gmx.com"
    port = 587
    from_email = os.environ.get("EMAIL")
    from_email_password = os.environ.get("EMAIL_PASSWORD")
    to_email = "clark.mask123@gmail.com"
    if successful_status:
        body = "Attached are the logs from the latest run of the Apto comp collection script."
    else:
        body = "The Apto comp collection script failed. See the collection log for more information."
    filepath1 = "apto/apto_collection_log.log"
    filepath2 = "apto/google_error_log.csv"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    if successful_status:
        msg['Subject'] = "Apto Collection -- SUCCESS"
    else:
        msg['Subject'] = "Apto Collection -- FAILURE"

    msg.attach(MIMEText(body, 'plain'))

    try:
        with open(filepath1, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(filepath1)
            )
            msg.attach(part)
        with open(filepath2, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(filepath2)
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



def delete_logs():
    os.remove("apto/apto_collection_log.log")
    os.remove("apto/google_error_log.csv")



############################################################
# Main run for apto collection
if __name__ == "__main__":
    setup_log_file()
    setup_google_error_log()

    with open("apto/apto_collection_log.log", "a") as error_log:
        apto_collection_script = subprocess.run(" ".join(['python3', 'apto/get_and_set_apto_comps.py']), shell=True, stderr=error_log)
        if apto_collection_script.returncode != 0:
            email_exit_status(False)
        else:
            email_exit_status(True)
        
    delete_logs()
