from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import os

ROOT_FOLDER_ID = "1hlM5KooGKaMvOdD_CshdyP0w-L37zK81"


def build_drive_service():
    info = json.loads(os.environ["GOOGLE_DRIVE_CREDENTIALS_JSON"])
    creds = Credentials(
        token=None,
        refresh_token=info["refresh_token"],
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=info["client_id"],
        client_secret=info["client_secret"],
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(service, name, parent_id):
    """Find folder by name under parent, create if not exists."""
    query = (
        f"name='{name}' and '{parent_id}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    folder = service.files().create(
        body={"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
        fields="id",
    ).execute()
    return folder["id"]


def upload_to_drive(service, local_path, factory_folder_name, month_folder, filename):
    """Upload file to Drive: {factory folder}/Data/{YYYY-MM}/{filename}"""
    factory_folder_id = get_or_create_folder(service, factory_folder_name, ROOT_FOLDER_ID)
    data_folder_id = get_or_create_folder(service, "Data", factory_folder_id)
    month_folder_id = get_or_create_folder(service, month_folder, data_folder_id)

    # Delete existing file with same name to avoid duplicates
    query = f"name='{filename}' and '{month_folder_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    for f in existing:
        service.files().delete(fileId=f["id"]).execute()

    media = MediaFileUpload(local_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    uploaded = service.files().create(
        body={"name": filename, "parents": [month_folder_id]},
        media_body=media,
        fields="id",
    ).execute()
    print(f"  Uploaded to Drive: {factory_folder_name}/Data/{month_folder}/{filename} (id={uploaded['id']})")
    return uploaded["id"]
