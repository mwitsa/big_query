"""
Run this ONCE locally to get your Google Drive refresh token.
Use the Desktop app OAuth client JSON you downloaded from Google Cloud Console.

Steps:
  1. Open the downloaded client_secret_xxx.json file
  2. Copy client_id and client_secret from it
  3. Run: python get_drive_token.py
  4. Browser opens -> log in with your personal Google account
  5. Copy the printed GOOGLE_DRIVE_CREDENTIALS_JSON value -> paste into Railway env vars
"""

import json
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = input("Paste your Client ID: ").strip()
CLIENT_SECRET = input("Paste your Client Secret: ").strip()

client_config = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}

SCOPES = ["https://www.googleapis.com/auth/drive"]

flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
creds = flow.run_local_server(port=0)

credentials_json = json.dumps({
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "refresh_token": creds.refresh_token,
    "token_uri": "https://oauth2.googleapis.com/token",
    "type": "authorized_user"
})

print("\n=== Add this to Railway environment variables ===")
print(f"GOOGLE_DRIVE_CREDENTIALS_JSON={credentials_json}")
