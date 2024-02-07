from datetime import datetime, timedelta
import os
import requests
import json

time_zone = datetime.now().astimezone().tzname()
TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00{time_zone}:00"

end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(days=-7)).strftime(TIMESTAMP_FORMAT)

query = "data science"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

print(url_raw)
bearer_token = os.environ.get("BEARER_TOKEN")
headers = {"Authorization": "Bearer {}".format(bearer_token)}
response = requests.request("GET", url_raw, headers=headers)
json_response = response.json()

print(json.dumps(json_response, indent=4, sort_keys=True))


while "next_token" in json_response.get("meta", {}):
    next_token = json_response['meta']['next_token']
    url= f"{url_raw}&next_token={next_token}"
    response = requests.request("GET", url, headers=headers)
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))

