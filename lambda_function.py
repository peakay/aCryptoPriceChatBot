import boto3
import json
import skype_chatbot
import os
import time
import requests
import math
from collections import defaultdict
import decimal
import operator
    


def lambda_handler(event, context):
    bot = skype_chatbot.SkypeBot(os.environ['app_id'], os.environ['app_secret'])
    time.sleep(0.25)
    data = event['data']
    bot_id = data['recipient']['id']
    bot_name = data['recipient']['name']
    recipient = data['from']
    service = data['serviceUrl']
    sender = data['conversation']['id']
    coin = data['text'].split(' ')[1]
    price = get_price(coin.lower())
    print_me = f'Top 5 markets sorted by volume for {coin.upper()}\n'
    for price_pair in price:
        print_me += f'{price_pair["pair"].upper()}: {price_pair["price"]} Direction: {price_pair["direction"]}\n'
    bot.send_message(bot_id, bot_name, recipient, service, sender, print_me)

def get_price(token):
    def float_to_str(f):
        """
        Convert the given float to a string,
        without resorting to scientific notation
        """
        d1 = ctx.create_decimal(repr(f))
        return format(d1, 'f')

    ctx = decimal.Context()
    ctx.prec = 6
    BASE_URL = 'https://api.cryptowat.ch'
    pairs = requests.get(f'{BASE_URL}/assets').json()['result']
    market_summary = requests.get(f'{BASE_URL}/markets/summaries').json()['result']
    pair_dict = {}
    for asset in pairs:
      pair_dict[asset['symbol']] = asset['route']
      pair_dict[asset['name'].lower()] = asset['route']
    
    asset_detail = requests.get(pair_dict[token]).json()['result']['markets']['base']
    volumes = defaultdict(int)
    totals = defaultdict(int)
    volumes_base = defaultdict(int)
    for market in asset_detail:
      if market['active'] and len(market['pair']) < 9:
        exchange_pair = f'{market["exchange"]}:{market["pair"]}'
        pair = market['pair']
        price_info = market_summary[exchange_pair]
        volumes[pair] += price_info['volumeQuote']
        volumes_base[pair] += price_info['volume']
        totals[pair] += price_info['price']['last']*price_info['volumeQuote']
    
    sorted_x = sorted(volumes_base.items(), key=operator.itemgetter(1), reverse=True)[:5]
    sorted_by_volume = [{'pair': pair, 
                        'price': float_to_str(round_sigfigs(totals[pair]/volumes[pair], 6)),
                        'direction': get_direction(pair, decimal.Decimal(str(round_sigfigs(totals[pair]/volumes[pair], 6))))
                        } for pair, volume in sorted_x]
    return sorted_by_volume

def get_direction(pair, new_price):
    session = boto3.session.Session(
          aws_access_key_id=os.environ['aws_access_key_id'], 
          aws_secret_access_key=os.environ['aws_secret_access_key'])
    table = session.resource('dynamodb', region_name='us-east-1').Table('prices')
    response = table.get_item(Key={'pair': pair})
    if 'Item' in response:
        item = response['Item']
        old_price = item['price']
        item['price'] = new_price
        table.put_item(Item=item)
        if old_price > new_price:
            rounded_percent = round_sigfigs((1-(new_price/old_price))*100, 3)
            return f'(n) {new_price-old_price} -{rounded_percent}%' 
        elif old_price < new_price:
            rounded_percent = round_sigfigs((1-(new_price/old_price))*100, 3)
            return f'(y) +{new_price-old_price} +{rounded_percent}%'
        elif old_price == new_price:
            return '='
        else:
            return 'an error lol'
    else:
        item = {'pair': pair, 'price': new_price}
        table.put_item(Item=item)
        return 'adding price to table...'


def round_sigfigs(num, sig_figs):
    if num != 0:
        return round(num, -int(math.floor(math.log10(abs(num))) - (sig_figs - 1)))
    else:
        return 0  # Can't take the log of 0
