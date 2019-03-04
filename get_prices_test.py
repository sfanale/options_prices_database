import requests
import json
import time
import datetime
import psycopg2.extras
import pandas as pd
import pytz


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
extra_tickers = ['ACE', 'AET', 'GAS', 'BCR', 'BXLT', 'BRCM', 'CA', 'CVC', 'CAM', 'CBG', 'CCE', 'CSC', 'DPS', 'DD', 'EMC', 'ESRX', 'GGP', 'HAR', 'HCN', 'HSP', 'HCBK', 'JOY', 'GMCR', 'KRFT', 'LVLT', 'LUK', 'LLTC', 'KORS', 'POM', 'PCL', 'PX', 'PCP', 'PCLN', 'COL', 'RLD', 'SNDK', 'SCG', 'SNI', 'STJ', 'SPLS', 'HOT', 'TE', 'TWC', 'TYC', 'XL', 'SPX', 'IVV', 'IJR', 'IYE', 'IYF', 'IYJ', 'IYK', 'IYM', 'IYZ', 'IYW']

# combine into one set for looping
all_tickers = set(nyse['Symbol'])
all_tickers = all_tickers.union(set(nas['Symbol']))
all_tickers = all_tickers.union(set(amx['Symbol']))
all_tickers = all_tickers.union(set(extra_tickers))


def build_info(input_info, stock, price_date, option_type=""):
    info = {'ask': None, 'bid': None, 'change': None, 'contractsize': None, 'contractsymbol': None,
            'currency': None, 'expiration': None, 'impliedvolatility': None, 'inthemoney': None,
            'lastprice': None,
            'lasttradedate': None, 'openinterest': None, 'percentchange': None, 'strike': None,
            'volume': None, "industry": None, "sector": None}
    info.update(input_info)

    info["priceDate"] = price_date
    info["underlyingSymbol"] = stock
    info["type"] = option_type
    info["currency"] = "USD"
    info["pricetype"] = 'intraday'
    if stock in all_db_set:
        info["industry"] = ''.join(
            str(all_db[all_db['Symbol'] == stock]['industry'].values[0]).split())
        info["sector"] = ''.join(str(all_db[all_db['Symbol'] == stock]['Sector'].values[0]).split())
    return info


headers = {'Connection': 'close'}


def test_if_open(url, ticker):
    last_trade_date = 0
    today = str(int(time.time()))
    try:
        res = requests.get(url + ticker + '?', timeout=5, headers=headers)
        res = res.json()
        last_trade = res['optionChain']['result'][0]['quote']['regularMarketTime']
        last_trade_date = datetime.datetime.fromtimestamp(float(last_trade)).date()
        today = datetime.datetime.fromtimestamp(time.time()).date()
    except requests.exceptions.ReadTimeout:
        print('timeout')
    except requests.exceptions.ConnectionError:
        print('\n\nConnection Error\n\n')
    return last_trade_date == today





top100 = ['AAPL', 'GE', 'ACB', 'F', 'CRON', 'MSFT', 'FB', 'AMD', 'FIT', 'GPRO', 'CGC', 'AMZN', 'SNAP', 'NFLX', 'BABA',
          'TWTR', 'NVDA', 'BAC', 'SQ', 'TSLA', 'DIS', 'MU', 'CHK', 'PLUG', 'ZNGA', 'T', 'SBUX', 'NVAX', 'NIO', 'XXII',
          'NBEV', 'GRPN', 'SIRI', 'INTC', 'ATVI', 'RAD', 'IQ', 'NKE', 'VOO', 'BRK.B', 'S', 'JD', 'NTDOY', 'V', 'TLRY',
          'GOOGL', 'SPY', 'KO', 'MJ', 'PYPL', 'ROKU', 'AUY', 'CSCO', 'WMT', 'CRM', 'TCEHY', 'DBX', 'JCP', 'GM', 'APHA',
          'SHOP', 'VTI', 'VZ', 'PFE', 'CARA', 'GLUU', 'COST', 'CRBP', 'GOOG', 'WFT', 'SPOT', 'INSY', 'LUV', 'TRXC', 'UAA',
          'CRSP', 'BA', 'AMAT', 'JPM', 'VSLR', 'NOK', 'ENPH', 'SNE', 'SFIX', 'AKS', 'TGT', 'P', 'BILI', 'BOTZ', 'JNJ',
          'TEVA', 'DNR', 'QQQ', 'AMRN', 'SPWR', 'ADBE', 'TWLO', 'CY', 'QCOM', 'IVV']



url = 'https://query1.finance.yahoo.com/v7/finance/options/'


stock_template = "(%(ask)s, %(askSize)s, %(averageDailyVolume10Day)s, %(averageDailyVolume3Month)s, %(bid)s, " \
                 "%(bidSize)s, %(bookValue)s, %(currency)s,%(dividendDate)s, %(earningsTimestamp)s," \
                 " %(earningsTimestampEnd)s, %(earningsTimestampStart)s, %(epsForward)s,%(epsTrailingTwelveMonths)s, " \
                 "%(esgPopulated)s, %(exchange)s, %(exchangeDataDelayedBy)s, %(exchangeTimezoneName)s," \
                 " %(exchangeTimezoneShortName)s, %(fiftyDayAverage)s, %(fiftyDayAverageChange)s, " \
                 "%(fiftyDayAverageChangePercent)s, %(fiftyTwoWeekHigh)s, %(fiftyTwoWeekHighChange)s, " \
                 "%(fiftyTwoWeekHighChangePercent)s, %(fiftyTwoWeekLow)s, %(fiftyTwoWeekLowChange)s, " \
                 "%(fiftyTwoWeekLowChangePercent)s, %(fiftyTwoWeekRange)s, %(financialCurrency)s,  %(forwardPE)s, " \
                 "%(fullExchangeName)s, %(gmtOffSetMilliseconds)s, %(language)s, %(longName)s, %(market)s, " \
                 "%(marketCap)s,%(marketState)s,%(messageBoardId)s,%(postMarketChange)s,%(postMarketChangePercent)s," \
                 " %(postMarketPrice)s, %(postMarketTime)s, %(priceHint)s, %(priceToBook)s, %(quoteSourceName)s," \
                 " %(quoteType)s, %(region)s, %(regularMarketChange)s, %(regularMarketChangePercent)s, " \
                 "%(regularMarketDayHigh)s, %(regularMarketDayLow)s, %(regularMarketDayRange)s,%(regularMarketOpen)s," \
                 " %(regularMarketPreviousClose)s, %(regularMarketPrice)s, %(regularMarketTime)s," \
                 " %(regularMarketVolume)s, %(sharesOutstanding)s, %(shortName)s, %(sourceInterval)s, %(symbol)s, " \
                 "%(tradeable)s, %(trailingAnnualDividendRate)s, %(trailingAnnualDividendYield)s, %(trailingPE)s, " \
                 "%(twoHundredDayAverage)s, %(twoHundredDayAverageChange)s, %(twoHundredDayAverageChangePercent)s, " \
                 "%(pricedate)s, %(sector)s, %(industry)s, %(pricetype)s)"

stock_sql = "INSERT INTO qoutes (ask, asksize, averagedailyvolume10day, averagedailyvolume3month, bid, bidsize, " \
            "bookvalue, currency, dividenddate, earningstimestamp, earningstimestampend, earningstimestampstart, " \
            "epsforward, epstrailing12months, esgpopulated, exchange, exchangedatadelayedby,exchangetimezonename, " \
            "exchangetimezoneshortname, fiftydayaverage, fiftydayaveragechange, fiftydayaveragechangepercent, " \
            "fiftytwoweekhigh, fiftytwoweekhighchange, fiftytwoweekhighchangepercent,fiftytwoweeklow, " \
            "fiftytwoweeklowchange, fiftytwoweeklowchangepercent, fiftytwoweekrange, financialcurrency, forwardpe, " \
            "fullexchangename, gmtoffsetmilliseconds, language, longname, market, marketcap, marketstate," \
            " messageboardid, postmarketchange, postmarketchangepercent, postmarketprice, postmarkettime, pricehint," \
            " pricetobook, quotesourcename, quotetype, region,regularmarketchange, regularmarketchangepercent, " \
            "regularmarketdayhigh, regularmarketdaylow, regularmarketdayrange, regularmarketopen, " \
            "regularmarketpreviousclose, regularmarketprice, regularmarkettime, regularmarketvolume, " \
            "sharesoutstanding, shortname, sourceinterval, symbol, tradeable, trailingannualdividendrate, " \
            "trailingannualdividendyield, trailingpe, twohundreddayaverage, twohundreddayaveragechange," \
            " twohundreddayaveragechangepercent, pricedate, sector, industry, pricetype) VALUES %s"

option_template = "(%(priceDate)s, %(underlyingSymbol)s, %(ask)s, %(bid)s, %(change)s, %(contractSize)s," \
                  "%(contractSymbol)s, %(currency)s, %(expiration)s, %(impliedVolatility)s, %(inTheMoney)s, " \
                  "%(lastPrice)s,%(lastTradeDate)s, %(openInterest)s, %(percentChange)s, %(strike)s, %(volume)s," \
                  " %(type)s, %(industry)s, %(sector)s, %(pricetype)s)"

option_sql = "INSERT INTO prices (pricedate, underlyingsymbol, ask, bid, change, contractsize, contractsymbol," \
             "currency, expiration, impliedvolatility, inthemoney, lastprice, lasttradedate, openinterest," \
             " percentchange,strike, volume, optiontype, industry, sector, pricetype) VALUES %s"

tz = pytz.timezone('US/Eastern')

while datetime.datetime.now(tz).hour < 16:
    if datetime.datetime.now(tz).minute % 15 == 0:
        run_code = test_if_open(url, 'AAPL')
        if run_code:
            print("run code")
            conn = psycopg2.connect(host="options-prices.cetjnpk7rvcs.us-east-1.rds.amazonaws.com",
                                    database="options_prices",
                                    user="Stephen", password="password69")
            cur = conn.cursor()
            add_stock = []
            add_call = []
            add_put = []
            failed = []
            for i, stock in enumerate(top100):
                print(stock)
                print(i/len(top100)*100)
                try:
                    today = str(int(time.time()))
                    result = requests.get(url+stock+'?', timeout=5, headers=headers)
                    print(stock)
                    result = result.json()
                    dates = result['optionChain']['result'][0]['expirationDates']
                    res = result['optionChain']['result'][0]['quote']
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
                    info.update(res)
                    if stock in all_db_set:
                        info["industry"] = ''.join(str(all_db[all_db['Symbol'] == stock]['industry'].values[0]).split())
                        info["sector"] = ''.join(str(all_db[all_db['Symbol'] == stock]['Sector'].values[0]).split())
                    info["pricedate"] = today
                    info["pricetype"] = 'intraday'
                    add_stock.append(info)
                    for d in dates:
                        result = requests.get(url + stock + '?&date=' + str(d),  timeout=5, headers=headers)
                        result = result.json()
                        for call in result["optionChain"]["result"][0]["options"][0]["calls"]:
                            call_info = build_info(call, stock, today, option_type="call")
                            add_call.append(call_info)
                        for put in result["optionChain"]["result"][0]["options"][0]["puts"]:
                            put_info = build_info(put, stock, today, option_type="put")
                            add_put.append(put_info)
                   # print("done")
                except TypeError:
                    failed.append(stock)
                    print("Didn't find dates for " + stock)
                except IndexError:
                    failed.append(stock)
                    print('index error for '+stock)
                except json.decoder.JSONDecodeError:
                    failed.append(stock)
                    print('json error')
                except requests.exceptions.ReadTimeout:
                    print('timeout')
                except requests.exceptions.ConnectionError:
                    print('\n\nConnection Error\n\n')
                    failed.append(stock)

            print("failed")
            for stock in failed:
                print(stock)
                try:
                    result = requests.get(url+stock+'?')
                    result = result.json()
                    dates = result['optionChain']['result'][0]['expirationDates']
                    res = result['optionChain']['result'][0]['quote']
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
                    info.update(res)
                    if stock in all_db_set:
                        info["industry"] = ''.join(str(all_db[all_db['Symbol'] == stock]['industry'].values[0]).split())
                        info["sector"] = ''.join(str(all_db[all_db['Symbol'] == stock]['Sector'].values[0]).split())
                    info["pricedate"] = today
                    info["pricetype"] = 'intraday'
                    add_stock.append(info)
                    for d in dates:
                        result = requests.get(url + stock + '?&date=' + str(d),  timeout=5)
                        result = result.json()
                        for call in result["optionChain"]["result"][0]["options"][0]["calls"]:
                            call_info = build_info(call, stock, today, option_type="call")
                            add_call.append(call_info)
                        for put in result["optionChain"]["result"][0]["options"][0]["puts"]:
                            put_info = build_info(put, stock, today, option_type="put")
                            add_put.append(put_info)
                   # print("done")
                except TypeError:
                    print("Didn't find dates for " + stock)
                except IndexError:
                    print('index error for '+stock)
                except json.decoder.JSONDecodeError:
                    print('json error')
                except requests.exceptions.ReadTimeout:
                    print('timeout')
                except requests.exceptions.ConnectionError:
                    print('\n\nConnection Error\n\n')

            psycopg2.extras.execute_values(cur, stock_sql, add_stock, template=stock_template)
            psycopg2.extras.execute_values(cur, option_sql, add_call, template=option_template)
            psycopg2.extras.execute_values(cur, option_sql, add_put, template=option_template)
            print("done "+str(datetime.datetime.now(tz).hour) + ': '+str(datetime.datetime.now(tz).minute))
            conn.commit()
            cur.close()
            conn.close()
            #print(failed)
            #print(len(failed))
        else:
            print("market closed today")
            time.sleep(60)

    else:
        print("sleep")
        time.sleep(60)

