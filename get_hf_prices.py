import requests
import json
import time
import psycopg2
import pandas as pd
import threading
from queue import Queue
import datetime

# define global variables
global nyse
global nas
global amx
global extra_tickers

# read in lists of tickers, and legacy values from extra tickers
nyse = pd.read_csv('NYSE.csv')
nas = pd.read_csv('NASDAQ.csv')
amx = pd.read_csv('AMX.csv')
all_db = pd.merge(nas, nyse, how="outer")
all_db = pd.merge(all_db, amx, how='outer')
all_db_set = set(all_db['Symbol'])


def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


@coroutine
def addToTable(option_type=""):
    queue = Queue()
    conn = psycopg2.connect(host="options-prices.cetjnpk7rvcs.us-east-1.rds.amazonaws.com", database="options_prices",
                            user="Stephen", password="password69")
    cur = conn.cursor()

    def add_data():
        print('Threading started')
        while True:
            item = queue.get()
            if item is GeneratorExit:
                #conn.commit()
                cur.close()
                conn.close()
                return
            elif item == [0, 0, 0]:
                conn.commit()
                print("committed")
            elif item is None:
                continue
            else:
                info = {'ask': None, 'bid': None, 'change': None, 'contractsize': None, 'contractsymbol': None,
                        'currency': None, 'expiration': None, 'impliedvolatility': None, 'inthemoney': None,
                        'lastprice': None,
                        'lasttradedate': None, 'openinterest': None, 'percentchange': None, 'strike': None,
                        'volume': None}
                result = item[0]
                stock = item[1]
                price_date = item[2]
                info.update(result)
                if stock in all_db_set:
                    info["priceDate"] = price_date
                    info["underlyingSymbol"] = stock
                    info["type"] = option_type
                    info["currency"] = "USD"
                    info["industry"] = ''.join(
                        str(all_db[all_db['Symbol'] == stock]['industry'].values[0]).split())
                    info["sector"] = ''.join(str(all_db[all_db['Symbol'] == stock]['Sector'].values[0]).split())

                    cur.execute("""INSERT INTO prices (pricedate, underlyingsymbol, ask, bid, change, contractsize, contractsymbol,
                             currency, expiration, impliedvolatility, inthemoney, lastprice, lasttradedate, openinterest, percentchange,
                             strike, volume, optiontype, industry, sector) VALUES (%(priceDate)s, %(underlyingSymbol)s, %(ask)s, %(bid)s, %(change)s, %(contractSize)s,
                             %(contractSymbol)s, %(currency)s, %(expiration)s, %(impliedVolatility)s, %(inTheMoney)s, %(lastPrice)s,
                             %(lastTradeDate)s, %(openInterest)s, %(percentChange)s, %(strike)s, %(volume)s, %(type)s, %(industry)s, %(sector)s);""",
                                info)
                else:
                    info = info
                    info["priceDate"] = price_date
                    info["underlyingSymbol"] = stock
                    info["type"] = option_type
                    info["currency"] = "USD"

                    cur.execute("""INSERT INTO prices (pricedate, underlyingsymbol, ask, bid, change, contractsize, contractsymbol,
                                     currency, expiration, impliedvolatility, inthemoney, lastprice, lasttradedate, openinterest, percentchange,
                                     strike, volume, optiontype) VALUES (%(priceDate)s, %(underlyingSymbol)s, %(ask)s, %(bid)s, %(change)s, %(contractSize)s,
                                     %(contractSymbol)s, %(currency)s, %(expiration)s, %(impliedVolatility)s, %(inTheMoney)s, %(lastPrice)s,
                                     %(lastTradeDate)s, %(openInterest)s, %(percentChange)s, %(strike)s, %(volume)s, %(type)s);""",
                                info)
                print("finished adding option %s" % stock)
    threading.Thread(target=add_data).start()
    try:
        while True:
            result, stock, pricedate = (yield)
            queue.put([result, stock, pricedate])
    except GeneratorExit:
        queue.put(GeneratorExit)


@coroutine
def add_to_quote_table():
    conn = psycopg2.connect(host="options-prices.cetjnpk7rvcs.us-east-1.rds.amazonaws.com", database="options_prices",
                            user="Stephen", password="password69")
    cur = conn.cursor()
    queue = Queue()

    def add_data():
        print('Threading started')
        while True:
            item = queue.get()
            if item is GeneratorExit:
                #conn.commit()
                cur.close()
                conn.close()
                return
            elif item == [0, 0, 0]:
                conn.commit()
                print("committed")
            elif item is None:
                continue
            else:
                info = {"ask": None, "askSize": None, "averageDailyVolume10Day": None, "averageDailyVolume3Month": None,
                        "bid": None,
                        "bidSize": None, "bookValue": None, "currency": None, "dividendDate": None,
                        "earningsTimestamp": None,
                        "earningsTimestampEnd": None, "earningsTimestampStart": None, "epsForward": None,
                        "epsTrailingTwelveMonths": None,
                        "esgPopulated": None, "exchange": None, "exchangeDataDelayedBy": None,
                        "exchangeTimezoneName": None,
                        "exchangeTimezoneShortName": None, "fiftyDayAverage": None, "fiftyDayAverageChange": None,
                        "fiftyDayAverageChangePercent": None, "fiftyTwoWeekHigh": None, "fiftyTwoWeekHighChange": None,
                        "fiftyTwoWeekHighChangePercent": None, "fiftyTwoWeekLow": None, "fiftyTwoWeekLowChange": None,
                        "fiftyTwoWeekLowChangePercent": None, "fiftyTwoWeekRange": None, "financialCurrency": None,
                        "forwardPE": None,
                        "fullExchangeName": None, "gmtOffSetMilliseconds": None, "language": None, "longName": None,
                        "market": None,
                        "marketCap": None, "marketState": None, "messageBoardId": None, "postMarketChange": None,
                        "postMarketChangePercent": None, "postMarketPrice": None, "postMarketTime": None,
                        "priceHint": None,
                        "priceToBook": None, "quoteSourceName": None, "quoteType": None, "region": None,
                        "regularMarketChange": None,
                        "regularMarketChangePercent": None, "regularMarketDayHigh": None, "regularMarketDayLow": None,
                        "regularMarketDayRange": None, "regularMarketOpen": None, "regularMarketPreviousClose": None,
                        "regularMarketPrice": None, "regularMarketTime": None, "regularMarketVolume": None,
                        "sharesOutstanding": None,
                        "shortName": None, "sourceInterval": None, "symbol": None, "tradeable": None,
                        "trailingAnnualDividendRate": None,
                        "trailingAnnualDividendYield": None, "trailingPE": None, "twoHundredDayAverage": None,
                        "twoHundredDayAverageChange": None, "twoHundredDayAverageChangePercent": None, "sector": None,
                        "industry": None}
                res = item[0]
                stock = item[1]
                price_date = item[2]

                info.update(res)
                info["pricedate"] = price_date

                if stock in all_db_set:
                    info["industry"] = ''.join(str(all_db[all_db['Symbol'] == stock]['industry'].values[0]).split())
                    info["sector"] = ''.join(str(all_db[all_db['Symbol'] == stock]['Sector'].values[0]).split())

                cur.execute("""INSERT INTO qoutes (ask, asksize, averagedailyvolume10day, averagedailyvolume3month, bid,
                         bidsize, bookvalue, currency, dividenddate, earningstimestamp, earningstimestampend, 
                         earningstimestampstart, epsforward, epstrailing12months, esgpopulated, exchange, exchangedatadelayedby,
                         exchangetimezonename, exchangetimezoneshortname, fiftydayaverage, fiftydayaveragechange, 
                         fiftydayaveragechangepercent, fiftytwoweekhigh, fiftytwoweekhighchange, fiftytwoweekhighchangepercent,
                         fiftytwoweeklow, fiftytwoweeklowchange, fiftytwoweeklowchangepercent, fiftytwoweekrange, 
                         financialcurrency, forwardpe, fullexchangename, gmtoffsetmilliseconds, language, longname, 
                         market, marketcap, marketstate, messageboardid, postmarketchange, postmarketchangepercent, 
                         postmarketprice, postmarkettime, pricehint, pricetobook, quotesourcename, quotetype, region,
                         regularmarketchange, regularmarketchangepercent, regularmarketdayhigh, regularmarketdaylow, 
                         regularmarketdayrange, regularmarketopen, regularmarketpreviousclose, regularmarketprice, 
                         regularmarkettime, regularmarketvolume, sharesoutstanding, shortname, sourceinterval, symbol, 
                         tradeable, trailingannualdividendrate, trailingannualdividendyield, trailingpe, twohundreddayaverage, 
                         twohundreddayaveragechange, twohundreddayaveragechangepercent, pricedate, sector, industry) VALUES (%(ask)s, %(askSize)s, 
                         %(averageDailyVolume10Day)s, %(averageDailyVolume3Month)s, %(bid)s, %(bidSize)s, %(bookValue)s, %(currency)s,
                         %(dividendDate)s, %(earningsTimestamp)s, %(earningsTimestampEnd)s, %(earningsTimestampStart)s, %(epsForward)s, 
                         %(epsTrailingTwelveMonths)s, %(esgPopulated)s, %(exchange)s, %(exchangeDataDelayedBy)s, %(exchangeTimezoneName)s,
                         %(exchangeTimezoneShortName)s, %(fiftyDayAverage)s, %(fiftyDayAverageChange)s, %(fiftyDayAverageChangePercent)s, 
                         %(fiftyTwoWeekHigh)s, %(fiftyTwoWeekHighChange)s, %(fiftyTwoWeekHighChangePercent)s, %(fiftyTwoWeekLow)s, 
                         %(fiftyTwoWeekLowChange)s, %(fiftyTwoWeekLowChangePercent)s, %(fiftyTwoWeekRange)s, %(financialCurrency)s, 
                         %(forwardPE)s, %(fullExchangeName)s, %(gmtOffSetMilliseconds)s, %(language)s, %(longName)s, %(market)s, %(marketCap)s, 
                         %(marketState)s, %(messageBoardId)s, %(postMarketChange)s, %(postMarketChangePercent)s, %(postMarketPrice)s, 
                         %(postMarketTime)s, %(priceHint)s, %(priceToBook)s, %(quoteSourceName)s, %(quoteType)s, %(region)s, %(regularMarketChange)s, 
                         %(regularMarketChangePercent)s, %(regularMarketDayHigh)s, %(regularMarketDayLow)s, %(regularMarketDayRange)s, 
                         %(regularMarketOpen)s, %(regularMarketPreviousClose)s, %(regularMarketPrice)s, %(regularMarketTime)s, 
                         %(regularMarketVolume)s, %(sharesOutstanding)s, %(shortName)s, %(sourceInterval)s, %(symbol)s, %(tradeable)s, 
                         %(trailingAnnualDividendRate)s, %(trailingAnnualDividendYield)s, %(trailingPE)s, %(twoHundredDayAverage)s, 
                         %(twoHundredDayAverageChange)s, %(twoHundredDayAverageChangePercent)s, %(pricedate)s, %(sector)s, %(industry)s);""",
                            info)
                print("finished adding stock %s" % stock)

    threading.Thread(target=add_data).start()
    try:
        while True:
            result, stock, pricedate = (yield)
            queue.put([result, stock, pricedate])
    except GeneratorExit:
        queue.put(GeneratorExit)


top100 = ['AAPL', 'GE', 'ACB', 'F', 'CRON', 'MSFT', 'FB', 'AMD', 'FIT', 'GPRO', 'CGC', 'AMZN', 'SNAP', 'NFLX', 'BABA',
          'TWTR', 'NVDA', 'BAC', 'SQ', 'TSLA', 'DIS', 'MU', 'CHK', 'PLUG', 'ZNGA', 'T', 'SBUX', 'NVAX', 'NIO', 'XXII',
          'NBEV', 'GRPN', 'SIRI', 'INTC', 'ATVI', 'RAD', 'IQ', 'NKE', 'VOO', 'BRK.B', 'S', 'JD', 'NTDOY', 'V', 'TLRY',
          'GOOGL', 'SPY', 'KO', 'MJ', 'PYPL', 'ROKU', 'AUY', 'CSCO', 'WMT', 'CRM', 'TCEHY', 'DBX', 'JCP', 'GM', 'APHA',
          'SHOP', 'VTI', 'VZ', 'PFE', 'CARA', 'GLUU', 'COST', 'CRBP', 'GOOG', 'WFT', 'SPOT', 'INSY', 'LUV', 'TRXC', 'UAA',
          'CRSP', 'BA', 'AMAT', 'JPM', 'VSLR', 'NOK', 'ENPH', 'SNE', 'SFIX', 'AKS', 'TGT', 'P', 'BILI', 'BOTZ', 'JNJ',
          'TEVA', 'DNR', 'QQQ', 'AMRN', 'SPWR', 'ADBE', 'TWLO', 'CY', 'QCOM', 'IVV']

price_dict = {}
failed = []

url = 'https://query1.finance.yahoo.com/v7/finance/options/'
today = str(int(time.time()))
# i added db to the sp500 just because i want the data


stock_Q = Queue()
put_Q = Queue()
call_Q = Queue()
add_stock = add_to_quote_table()
add_call = addToTable(option_type="call")
add_put = addToTable(option_type="put")


while datetime.datetime.now().hour < 16:
    if datetime.datetime.now().minute % 15 == 0:
        for i, stock in enumerate(top100):
            print(stock)
            print(i/len(top100)*100)
            try:
                result = requests.get(url+stock+'?')
                result = result.json()
                dates = result['optionChain']['result'][0]['expirationDates']
                add_stock.send([result['optionChain']['result'][0]['quote'], stock, str(int(time.time()))])
                for d in dates:
                    result = requests.get(url + stock + '?&date=' + str(d))
                    result = result.json()
                    for call in result["optionChain"]["result"][0]["options"][0]["calls"]:
                        add_call.send([call, stock, str(int(time.time()))])
                    for put in result["optionChain"]["result"][0]["options"][0]["puts"]:
                        add_put.send([put, stock, str(int(time.time()))])
            except TypeError:
                failed.append(stock)
                print("Didn't find dates for " + stock)
            except IndexError:
                failed.append(stock)
                print('index error for '+stock)
            except json.decoder.JSONDecodeError:
                failed.append(stock)
                print('json error')
        print('got all data')
        print(datetime.datetime.now().minute)
        print(call_Q.qsize())
        while not call_Q.empty():
            print(call_Q)
            print(put_Q)
            time.sleep(30)
        add_call.send([0,0,0])
        add_put.send([0,0,0])
        add_stock.send([0,0,0])
    else:
        print("sleep")
        time.sleep(60)

add_stock.close()
add_call.close()
add_put.close()
print(failed)
print(len(failed))


