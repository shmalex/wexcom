#!python -m pip install --upgrade pip
#!pip install aiohttp
#!pip install elasticsearch

from elasticsearch import Elasticsearch
import asyncio
import aiohttp
import async_timeout
import numpy as np
import requests as rq
import json

es = Elasticsearch(["10.11.26.54"])
tickers = 'https://wex.nz/api/3/ticker/{0}'
depth = 'https://wex.nz/api/3/depth/{0}'
resp = rq.get(url = 'https://wex.nz/api/3/info')

prices = json.loads(resp.content)

doc = prices['pairs']

urls = [(p, tickers.format(p),depth.format(p)) for p in doc.keys()]

print(doc.keys())

# we need that function
async def await_get_and_store(ticker, ticker_url, depth_url):
    try:
        #print('await_get_and_store', ticker)
        loop = asyncio.get_event_loop()
        task1 = loop.create_task(load_ticker(ticker, ticker_url,loop))
        task2 = loop.create_task(load_depth(ticker, depth_url, loop))
        await asyncio.wait([task1, task2], loop=loop)
        res1  =task1.result()
        res2  =task2.result()
        await asyncio.sleep(0.0001)
        ticker_doc, doc_id, depth_doc = (res1[0],res1[1],res2)

        idx_task1 = index_doc(f"ticker_{ticker}", 'ticker', ticker_doc, doc_id)
        idx_task2 = index_doc(f"depth_{ticker}", 'depth', depth_doc, doc_id)
        await asyncio.wait([idx_task1, idx_task2], loop=loop)
        #print('save ', ticker,  doc_id, 'done')
        #return doc_id
    except  Exception as ex:
        print('sheet happend')
        #return 0

# we need this function
async def index_doc(index, doc_type, doc, doc_id):
    #print(index, doc_type, doc, doc_id)
    await asyncio.sleep(0.0001)
    res = es.index(index, doc_type, doc, doc_id)
    return res

# we need this function
async def fetch(session, url):
    async with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()

def slack(msg):
    payload = json.dumps({"text": msg})
    SLACK = "https://hooks.slack.com/services/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"
    r = rq.post(
        SLACK,
        data=payload,
        headers={'Content-Type': 'application/json'}
    )

# we need this function
async def load_ticker(ticker, ticker_url, loop):
    #print(f'ticker {ticker} GET')
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            response = await fetch(session, ticker_url)
        #print(f'ticker {ticker} JSON')
        ticker_json = json.loads(response)
        ticker_doc = ticker_json[ticker]
        ticker_doc_id = ticker_doc['updated']
        return (ticker_doc, ticker_doc_id)
    except Exception as ex:
        msg = str(ex) + ' ' + str(type(ex))
        print('load_ticker error',msg)
        slack(f"wex ticker error {ticker}: {msg}")
        raise ex

async def load_depth(ticker, depth_url, loop):
    #print(f'depth {ticker} GET')
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            response = await fetch(session, depth_url)
        depth_json = json.loads(response)
        depth_doc = depth_json[ticker]
        return depth_doc
    except Exception as ex:
        msg = str(ex) + ' ' + str(type(ex))
        print('load_depth error',msg)
        slack(f"wex depth error {ticker}: {msg}")
        raise ex

def handler(loop, context):
    print('handler loop',loop)
    print('handler context',context)
    slack(f"handler loop:{loop}")
    slack(f"handler context:{context}")
def main():
    count=0
    while True:
        try:
            tasks = asyncio.gather(*[await_get_and_store(u[0], u[1], u[2]) for u in urls])
            results = loop.run_until_complete(tasks)
            print(f'{count}')
            count += 1
        except Exception as ex:
            msg = str(ex) + ' ' + str(type(ex))
            print('load_depth error', type(ex),msg)
            slack(f"wex ticker error {ticker}: {ex}")

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
#loop.set_debug(1)
loop.set_exception_handler(handler)
main()
loop.run_forever()
loop.close()