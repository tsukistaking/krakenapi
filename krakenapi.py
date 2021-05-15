import urllib
import time
import hmac
import hashlib
import base64
import requests

base_url = "https://api.kraken.com"

def nonce():
    return int(1000*time.time())

def sign(secret_key, data, urlpath):
    postdata = urllib.parse.urlencode(data)

    # Unicode-objects must be encoded before hashing
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(base64.b64decode(secret_key),
                            message, hashlib.sha512)
    sigdigest = base64.b64encode(signature.digest())

    return sigdigest.decode()

def withdraw(api_key, secret_key, amount, asset, destination):
    withdraw_path = "/0/private/Withdraw"
    full_withdraw_url = base_url + withdraw_path
    data = {
        'nonce': nonce(),
        'asset': asset,
        'key': destination,
        'amount': amount
    }
    headers = {
        'API-Key': api_key,
        'API-Sign': sign(secret_key, data, withdraw_path)
    }
    response = requests.post(full_withdraw_url, data=data, headers=headers)
    print(response.text)

def balance(api_key, secret_key, asset):
    balance_path = "/0/private/Balance"
    balance_url = base_url + balance_path
    balance_data = {
        'nonce': nonce(),
    }
    balance_headers = {
        'API-Key': api_key,
        'API-Sign': sign(secret_key, balance_data, balance_path)
    }
    response = requests.post(balance_url, data=balance_data, headers=balance_headers)
    response.raise_for_status()
    balance = float(response.json()['result'].get(asset, 0))
    return balance

def market_trade(api_key, secret_key, pair, side, amount):
    trade_path = "/0/private/AddOrder"
    trade_url = base_url + trade_path
    trade_data = {
        'nonce': nonce(),
        'pair': pair,
        'type': side,
        'ordertype': "market",
        'volume': amount
    }
    trade_headers = {
        'API-Key': api_key,
        'API-Sign': sign(secret_key, trade_data, trade_path)
    }
    response = requests.post(trade_url, data=trade_data, headers=trade_headers)
    response.raise_for_status()
    print(response.text)

