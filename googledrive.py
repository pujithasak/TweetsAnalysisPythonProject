import requests
import os.path
from os import path

def download_file_from_google_drive(id, destination):
    URL = "https://drive.google.com/uc?id=1rwwM1NgrYDWHabvfmanS-C2uD43I-wA7&export=download" #https://drive.google.com/file/d/1rwwM1NgrYDWHabvfmanS-C2uD43I-wA7/view

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

if __name__ == "__main__":
    if not path.exists("tweetsfile.json"):
        print("file not exist..");
        file_id = 'tweets.json'
        destination = 'tweetsfile.json'
        download_file_from_google_drive(file_id, destination)
    else:
        print("file exist..");
