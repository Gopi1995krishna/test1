import requests
import json
from requests.auth import HTTPBasicAuth

def get_token_internal(token_url, client_id, client_secret, username, password, identity_domain):

    params = {
        "grant_type" : "password",
        "username" : username,
        "password" : password
        #"scope" : "{}urn:opc:resource:consumer::all".format(instance_url)
    }

    headers = {
        "X-Resource-Identity-Domain-Name" : identity_domain
    }
    #print(token_url)
    #print(params)
    response = requests.post(token_url, data=params, headers=headers, auth=HTTPBasicAuth(client_id, client_secret), verify=False)
    if response.status_code != 200:
        print("OAuth token request ended with unexpected server response: {}. Please check the below details messages for details.".format(response.status_code))
        print(response.headers)
        print(response.text)
    return response.json().get("access_token")


def get_token_external(instance_url, token_url, client_id, client_secret, username, password):

    params = {
        "grant_type" : "password",
        "username" : username,
        "password" : password,
        "scope" : "{}urn:opc:resource:consumer::all".format(instance_url)
    }
    #print(token_url)
    #print(params)
    response = requests.post(token_url, data=params, auth=HTTPBasicAuth(client_id, client_secret))
    if response.status_code != 200:
        print("OAuth token request ended with unexpected server response: {}. Please check the below details messages for details.".format(response.status_code))
        print(response.headers)
        print(response.text)
    return response.json().get("access_token")

def get_amce_token(instance):
    return get_token_external(
        instance.get("instance_url"),
        instance.get("token_url"),
        instance.get("client_id"),
        instance.get("client_secret"),
        instance.get("username"),
        instance.get("password")
    )
def get_mcs_token(instance):
    return get_token_internal(
        instance.get("token_url"),
        instance.get("client_id"),
        instance.get("client_secret"),
        instance.get("username"),
        instance.get("password"),
        instance.get("identity_domain")
    )



if __name__ == "__main__":
    with open("config.json") as config_file:
        config = json.load(config_file)

    instance = config.get("source_instance")
    instance_url = instance.get("instance_url")
    username = instance.get("username")
    password = instance.get("password")
    token_url = instance.get("token_url")
    client_id = instance.get("client_id")
    client_secret = instance.get("client_secret")

    source_token = get_token_external(instance_url, token_url, client_id, client_secret, username, password)
    print("SRC_TOKEN={}".format(source_token))
