import base64
import hashlib
import hmac
import time
import urllib

import requests

base_url = "https://api.kraken.com"
API_KEY = None
SECRET_KEY = None

def nonce():
    return int(1000*time.time())

def sign(data, urlpath):
    if not SECRET_KEY:
        raise Exception("SECRET_KEY not set")

    postdata = urllib.parse.urlencode(data)

    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(base64.b64decode(SECRET_KEY),
                            message, hashlib.sha512)
    sigdigest = base64.b64encode(signature.digest())

    return sigdigest.decode()

def post_request(path, data):
    if not API_KEY:
        raise Exception("API_KEY not set") 
    url  = base_url + path
    data['nonce'] = nonce()
    headers = {
        'API-Key': API_KEY,
        'API-Sign': sign(data, path)
    }
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    if error := response_json.get('error'):
        raise Exception(error)
    if not (result := response_json.get('result')):
        raise Exception(f"'result' key not found in json: {response}")
    print(result)
    return result

def withdraw(amount, asset, destination):
    withdraw_path = "/0/private/Withdraw"
    data = {
        'asset': asset,
        'key': destination,
        'amount': amount
    }
    post_request(withdraw_path, data)

def balance(asset):
    balance_path = "/0/private/Balance"
    response = post_request(balance_path, {})
    balance = float(response.get(asset, 0))
    return balance

def market_trade(pair, side, amount):
    trade_path = "/0/private/AddOrder"
    trade_data = {
        'pair': pair,
        'type': side,
        'ordertype': "market",
        'volume': amount
    }
    post_request(trade_path, trade_data)

def list_earn_strategies():
    earn_path = "/0/private/Earn/Strategies"
    response = post_request(earn_path, {})
    return response['items']

def allocate_earn_funds(strategy_id, amount):
    allocate_path = "/0/private/Earn/Allocate"
    allocate_data = {
        'strategy': strategy_id,
        'amount': amount
    }
    post_request(allocate_path, allocate_data)

def allocate_status(strategy_id):
    status_path = "/0/private/Earn/AllocateStatus"
    status_data = {
        'strategy': strategy_id
    }
    return post_request(status_path, status_data)

