import asyncio
import base64
import hashlib
import hmac
import math
import time
from typing import Any
from urllib import parse

from niquests import AsyncSession

base_url = "https://api.kraken.com"
API_KEY = None
SECRET_KEY = None

def nonce():
    return int(1000000000 * time.time())

def sign(data, urlpath):
    if not SECRET_KEY:
        raise Exception("SECRET_KEY not set")

    postdata = parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(base64.b64decode(SECRET_KEY),
                         message, hashlib.sha512)
    sigdigest = base64.b64encode(signature.digest())

    return sigdigest.decode()

nonce_lock = asyncio.Lock()

async def post_request(session: AsyncSession, path: str, data: dict[str, Any]):
    if not API_KEY:
        raise Exception("API_KEY not set")
    url = base_url + path

    async with nonce_lock:
        data['nonce'] = nonce()

    headers = {
        'API-Key': API_KEY,
        'API-Sign': sign(data, path)
    }

    response = await session.post(url, data=data, headers=headers)
    response_json = response.json()
    if error := response_json.get('error'):
        raise Exception(f"Kraken API error: {error}")
    if 'result' not in response_json:
        raise Exception(f"'result' key not found in response: {response_json}")
    return response_json['result']

async def withdraw(session: AsyncSession, amount, asset, destination):
    withdraw_path = "/0/private/Withdraw"
    data = {
        'asset': asset,
        'key': destination,
        'amount': amount
    }
    await post_request(session, withdraw_path, data)

async def all_balances(session: AsyncSession):
    balance_path = "/0/private/Balance"
    response = await post_request(session, balance_path, {})
    return response

async def balance(session: AsyncSession, asset):
    all_balances_response = await all_balances(session)
    balance = float(all_balances_response.get(asset, 0))
    return balance

async def balances(session: AsyncSession, assets):
    all_balances_response = await all_balances(session)
    balances = {asset: float(all_balances_response.get(asset, 0)) for asset in assets}
    return balances

async def all_balances_extended(session: AsyncSession):
    balance_extended_path = "/0/private/BalanceEx"
    response = await post_request(session, balance_extended_path, {})
    return response

async def balance_extended(session: AsyncSession, asset):
    all_balances_extended_response = await all_balances_extended(session)
    balance = float(all_balances_extended_response.get(asset, 0))
    return balance

async def balances_extended(session: AsyncSession, assets):
    all_balances_extended_response = await all_balances_extended(session)
    balances = {asset: float(all_balances_extended_response.get(asset, 0)) for asset in assets}
    return balances

async def market_trade(session: AsyncSession, pair, side, amount):
    trade_path = "/0/private/AddOrder"
    trade_data = {
        'pair': pair,
        'type': side,
        'ordertype': "market",
        'volume': amount
    }
    await post_request(session, trade_path, trade_data)

async def list_earn_strategies(session: AsyncSession):
    earn_path = "/0/private/Earn/Strategies"
    response = await post_request(session, earn_path, {})
    return response['items']

async def allocate_earn_funds(session: AsyncSession, strategy_id, amount):
    allocate_path = "/0/private/Earn/Allocate"
    allocate_data = {
        'strategy_id': strategy_id,
        'amount': amount
    }
    await post_request(session, allocate_path, allocate_data)

async def allocate_status(session: AsyncSession, strategy_id):
    status_path = "/0/private/Earn/AllocateStatus"
    status_data = {
        'strategy_id': strategy_id
    }
    return await post_request(session, status_path, status_data)

async def list_earn_allocations(session: AsyncSession):
    allocations_path = "/0/private/Earn/Allocations"
    response = await post_request(session, allocations_path, {})
    return response['items']

async def trade_history(session: AsyncSession, from_datetime=None, to_datetime=None):
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
        response = await post_request(session, trade_history_path, trade_history_data)
        trades.update(response['trades'])
        trade_count = response['count']
        offset_page += 1
        trade_history_data['ofs'] = offset_page * 50
        # Decaying rate limit. Decays at 0.33 requests per second.
        # Limit is 15/20 and history requests are 2 requests.
        if offset_page > 6:
            await asyncio.sleep(7)

    return trades
