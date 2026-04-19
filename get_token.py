"""
Run this ONCE locally to get your OAuth2 refresh token.
Store the printed values as Railway environment variables.

Steps:
  1. Go to Google Cloud Console -> APIs & Services -> Credentials
  2. Create an OAuth 2.0 Client ID (Desktop app type)
  3. Download the client_secret JSON and copy client_id and client_secret below
  4. Run: python get_token.py
  5. Copy the refresh_token into Railway env vars
"""

import os
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = input("Paste your Google OAuth Client ID: ").strip()
CLIENT_SECRET = input("Paste your Google OAuth Client Secret: ").strip()

client_config = {
    "installed": {
        "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
        "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}

SCOPES = ["https://www.googleapis.com/auth/bigquery.readonly"]

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0)

print("\n=== Copy these into Railway environment variables ===")
print(f"GOOGLE_CLIENT_ID={CLIENT_ID}")
print(f"GOOGLE_CLIENT_SECRET={CLIENT_SECRET}")
print(f"GOOGLE_REFRESH_TOKEN={creds.refresh_token}")
