import requests
import base64
import pandas as pd
from dotenv import load_dotenv
import os
import re

load_dotenv()

############################################################
# Below is a routine for getting messages from a specific SMS exchange for a specific user. (In this case, Brad Murray's messages from CoStar)
# This only requires the phone:read:sms_session:admin and phone:read:sms_session:master scopes.
# You must know the session_id for the SMS exchange you want to get messages from. 
############################################################
#  OAuth Flow for Access Token
account_id = os.getenv("ZOOM_ACCOUNT_ID")
client_id = os.getenv("ZOOM_CLIENT_ID")
client_secret = os.getenv("ZOOM_CLIENT_SECRET")


credentials = f"{client_id}:{client_secret}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

at_url = "https://zoom.us/oauth/token"

at_headers = {
    "Authorization": f"Basic {encoded_credentials}",
    "Content-Type": "application/x-www-form-urlencoded"
}

at_payload = {
    "grant_type": "account_credentials",
    "account_id": account_id
}

def extract_number_from_string(input_string):
    # Use regular expression to find all numbers in the string
    numbers = re.findall(r'\d+', input_string)
    # Return the first number found, or None if no numbers are found
    return int(numbers[0]) if numbers else None

def get_2fa_code():
	at_response = requests.post(
			at_url,
			headers=at_headers,
			data=at_payload
	)

	if at_response.status_code == 200:
			access_token = at_response.json().get("access_token")
	else:
			print("Failed to get access token:", at_response.status_code, at_response.text)
			exit(1)


	# Get messages for Brad's CoStar session
	session_id = os.getenv("ZOOM_SESSION_ID") # SESSION ID FOR BRAD'S COSTAR

	messages_url = f"https://api.zoom.us/v2/phone/sms/sessions/{session_id}"
	messages_headers = {
			"Authorization": f"Bearer {access_token}"
	}
	messages_payload = {
			"grant_type": "account_credentials",
			"account_id": account_id,
	}
	response = requests.get(messages_url, headers=messages_headers, params=messages_payload)

	if response.status_code == 200:
			messages_info = response.json()
		
			otp = None
			
			# Check if 'sms_histories' key exists and is not empty
			if 'sms_histories' in messages_info and messages_info['sms_histories']:  
				last_record = messages_info['sms_histories'][-1]
		
				otp = extract_number_from_string(last_record['message']);
		
			else:
				last_record = None

			return otp;
			# print(messages_info)
			# message_df = pd.DataFrame(messages_info['sms_histories'])
			# print(message_df)
	else:
			print("Failed to get messages info:", response.status_code, response.text)
			return None