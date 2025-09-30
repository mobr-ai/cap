# cap/src/cap/core/google_oauth.py
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_userinfo_from_access_token(access_token: str):
    creds = Credentials(access_token)
    service = build("people", "v1", credentials=creds)
    person = service.people().get(
        resourceName="people/me",
        personFields="names,emailAddresses,photos"
    ).execute()

    google_id = person["resourceName"].split("/")[-1]
    email = person["emailAddresses"][0]["value"]
    display_name = person["names"][0]["displayName"]
    avatar = person["photos"][0]["url"] if "photos" in person else ""
    return {
        "sub": google_id,
        "email": email,
        "name": display_name,
        "picture": avatar,
    }
