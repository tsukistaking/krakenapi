import asyncio
import base64
import hashlib
import hmac
import math
import time
import urllib

import aiohttp

base_url = "https://api.kraken.com"
API_KEY = None
SECRET_KEY = None

def nonce():
    return int(1000000*time.time())

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

nonce_lock = asyncio.Lock()

session = None

async def get_session():
    global session
    if session is None:
        session = aiohttp.ClientSession(raise_for_status=True)
    return session

async def post_request(path, data):
    if not API_KEY:
        raise Exception("API_KEY not set") 
    url  = base_url + path
    async with nonce_lock:
        data['nonce'] = nonce()
        headers = {
            'API-Key': API_KEY,
            'API-Sign': sign(data, path)
        }
        session = await get_session()
    async with session.post(url, data=data, headers=headers) as response:
        response_json = await response.json()
        if error := response_json.get('error'):
            raise Exception(error)
        if not (result := response_json.get('result')):
            raise Exception(f"'result' key not found in json: {response}")
        return result

async def withdraw(amount, asset, destination):
    withdraw_path = "/0/private/Withdraw"
    data = {
        'asset': asset,
        'key': destination,
        'amount': amount
    }
    await post_request(withdraw_path, data)

async def all_balances():
    balance_path = "/0/private/Balance"
    response = await post_request(balance_path, {})
    return response

async def balance(asset):
    all_balances_response = await all_balances()
    balance = float(all_balances_response[asset])
    return balance

async def all_balances_extended():
    balance_extended_path = "/0/private/BalanceEx"
    response = await post_request(balance_extended_path, {})
    return response

async def balance_extended(asset):
    all_balances_extended_response = await all_balances_extended()
    balance = float(all_balances_extended_response[asset])
    return balance

async def market_trade(pair, side, amount):
    trade_path = "/0/private/AddOrder"
    trade_data = {
        'pair': pair,
        'type': side,
        'ordertype': "market",
        'volume': amount
    }
    await post_request(trade_path, trade_data)

async def list_earn_strategies():
    earn_path = "/0/private/Earn/Strategies"
    response = await post_request(earn_path, {})
    return response['items']

async def allocate_earn_funds(strategy_id, amount):
    allocate_path = "/0/private/Earn/Allocate"
    allocate_data = {
        'strategy_id': strategy_id,
        'amount': amount
    }
    await post_request(allocate_path, allocate_data)

async def allocate_status(strategy_id):
    status_path = "/0/private/Earn/AllocateStatus"
    status_data = {
        'strategy_id': strategy_id
    }
    return await post_request(status_path, status_data)

async def list_earn_allocations():
    allocations_path = "/0/private/Earn/Allocations"
    response = await post_request(allocations_path, {})
    return response['items']

async def trade_history(from_datetime = None, to_datetime = None):
    trade_history_path = "/0/private/TradesHistory"
    trade_history_data = {
        "ofs": 0
    }
    if from_datetime:
        trade_history_data['start'] = int(from_datetime.timestamp())
    if to_datetime:
        trade_history_data['end'] = int(to_datetime.timestamp())
    trades = {}
    trade_count = 50
    offset_page = 0
    while offset_page < math.ceil(trade_count / 50):
        response = await post_request(trade_history_path, trade_history_data)
        trades |= response['trades']
        trade_count = response['count']
        offset_page += 1
        trade_history_data['ofs'] = offset_page * 50
        # Decaying rate limit. Decays at 0.33 requests per second.
        # Limit is 15/20 and history requests are 2 requests.
        if offset_page > 6:
            await asyncio.sleep(7)
    
    return trades
