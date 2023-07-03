import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__)))
import requests
import env_settings


def send_line_message(message):
    url = "https://notify-api.line.me/api/notify"
    access_token = env_settings.LINE_ACCESS_TOKEN
    headers = {"Authorization": "Bearer " + access_token}
    payload = {"message": message}
    requests.post(
        url,
        headers=headers,
        params=payload,
    )
