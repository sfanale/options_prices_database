import requests
import json
import time
import datetime
import psycopg2
import pandas as pd
import threading
from queue import Queue

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


list_temp = ['IJR']


def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


@coroutine
def addToTable(price_date, option_type=""):
    queue = Queue()
    conn = psycopg2.connect(host="options-prices.cetjnpk7rvcs.us-east-1.rds.amazonaws.com", database="options_prices",
                            user="Stephen", password="password69")
    cur = conn.cursor()

    def add_data():
        print('Threading started')
        while True:
            item = queue.get()
            if item is GeneratorExit:
                conn.commit()
                cur.close()
                conn.close()
                return
            if item is None:
                continue
            else:
                info = {'ask': None, 'bid': None, 'change': None, 'contractsize': None, 'contractsymbol': None,
                        'currency': None, 'expiration': None, 'impliedvolatility': None, 'inthemoney': None,
                        'lastprice': None,
                        'lasttradedate': None, 'openinterest': None, 'percentchange': None, 'strike': None,
                        'volume': None}
                result = item[0]
                stock = item[1]
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
            result, stock = (yield)
            queue.put([result, stock])
    except GeneratorExit:
        queue.put(GeneratorExit)





@coroutine
def add_to_quote_table(price_date):
    conn = psycopg2.connect(host="options-prices.cetjnpk7rvcs.us-east-1.rds.amazonaws.com", database="options_prices",
                            user="Stephen", password="password69")
    cur = conn.cursor()
    queue = Queue()

    def add_data():
        print('Threading started')
        while True:
            item = queue.get()
            if item is GeneratorExit:
                conn.commit()
                cur.close()
                conn.close()
                return
            if item is None:
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
            result, stock = (yield)
            queue.put([result, stock])
    except GeneratorExit:
        queue.put(GeneratorExit)


def test_if_open(url, ticker):
    res = requests.get(url + ticker + '?')
    res = res.json()
    last_trade = res['optionChain']['result'][0]['quote']['regularMarketTime']
    last_trade_date = datetime.datetime.fromtimestamp(float(last_trade)).date()
    today = datetime.datetime.fromtimestamp(time.time()).date()
    return last_trade_date == today




SP500 = ['DB', 'AAPL', 'ABT', 'ABBV', 'ACN', 'ACE', 'ADBE', 'ADT', 'AAP', 'AES', 'AET', 'AFL',
             'AMG', 'A', 'GAS', 'ARE', 'APD', 'AKAM', 'AA', 'AGN', 'ALXN', 'ALLE', 'ADS', 'ALL', 'ALTR', 'MO', 'AMZN',
             'AEE', 'AAL',
             'AEP', 'AXP', 'AIG', 'AMT', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'APC', 'ADI', 'AON', 'APA', 'AIV', 'AMAT',
             'ADM', 'AIZ', 'T', 'ADSK', 'ADP', 'AN', 'AZO', 'AVGO', 'AVB', 'AVY', 'BHI', 'BLL', 'BAC', 'BK', 'BCR',
             'BXLT', 'BAX', 'BBT', 'BDX', 'BBBY', 'BRK.B', 'BBY', 'BLX', 'HRB', 'BA', 'BWA', 'BXP', 'BSX', 'BMY',
             'BRCM',
             'BF.B','MMM', 'DWDP', 'CHRW', 'CA', 'CVC', 'COG', 'CAM', 'CPB', 'COF', 'CAH', 'HSIC', 'KMX', 'CCL', 'CAT', 'CBG', 'CBS',
             'CELG', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHK', 'CVX', 'CMG', 'CB', 'CI', 'XEC', 'CINF', 'CTAS', 'CSCO',
             'C', 'CTXS', 'CLX', 'CME', 'CMS', 'COH', 'KO', 'CCE', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CSC', 'CAG', 'COP',
             'CNX', 'ED', 'STZ', 'GLW', 'COST', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DLPH',
             'DAL', 'XRAY', 'DVN', 'DO', 'DTV', 'DFS', 'DISCA', 'DISCK', 'DG', 'DLTR', 'D', 'DOV', 'DOW', 'DPS', 'DTE',
             'DD', 'DUK', 'DNB', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMC', 'EMR', 'ENDP', 'ESV',
             'ETR', 'EOG', 'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'ES', 'EXC', 'EXPE', 'EXPD', 'ESRX', 'XOM', 'FFIV',
             'FB', 'FAST', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FISV', 'FLIR', 'FLS', 'FLR', 'FMC', 'FTI', 'F', 'FOSL',
             'BEN', 'FCX', 'FTR', 'GME', 'GPS', 'GRMN', 'GD', 'GE', 'GGP', 'GIS', 'GM', 'GPC', 'GNW', 'GILD', 'GS',
             'GT',
             'GOOGL', 'GOOG', 'GWW', 'HAL', 'HBI', 'HOG', 'HAR', 'HRS', 'HIG', 'HAS', 'HCA', 'HCP', 'HCN', 'HP', 'HES',
             'HPQ', 'HD', 'HON', 'HRL', 'HSP', 'HST', 'HCBK', 'HUM', 'HBAN', 'ITW', 'IR', 'INTC', 'ICE', 'IBM', 'IP',
             'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IRM', 'JEC', 'JBHT', 'JNJ', 'JCI', 'JOY', 'JPM', 'JNPR', 'KSU', 'K',
             'KEY', 'GMCR', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KRFT', 'KR', 'LB', 'LLL', 'LH', 'LRCX', 'LM', 'LEG',
             'LEN', 'LVLT', 'LUK', 'LLY', 'LNC', 'LLTC', 'LMT', 'L', 'LOW', 'LYB', 'MTB', 'MAC', 'M', 'MNK', 'MRO',
             'MPC',
             'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MAT', 'MKC', 'MCD', 'MCK', 'MJN', 'MMV', 'MDT', 'MRK', 'MET', 'KORS',
             'MCHP', 'MU', 'MSFT', 'MHK', 'TAP', 'MDLZ', 'MON', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MUR', 'MYL', 'NDAQ',
             'NOV', 'NAVI', 'NTAP', 'NFLX', 'NWL', 'NFX', 'NEM', 'NWSA', 'NEE', 'NLSN', 'NKE', 'NI', 'NE', 'NBL', 'JWN',
             'NSC', 'NTRS', 'NOC', 'NRG', 'NUE', 'NVDA', 'ORLY', 'OXY', 'OMC', 'OKE', 'ORCL', 'OI', 'PCAR', 'PLL', 'PH',
             'PDCO', 'PAYX', 'PNR', 'PBCT', 'POM', 'PEP', 'PKI', 'PRGO', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PBI',
             'PCL', 'PNC', 'RL', 'PPG', 'PPL', 'PX', 'PCP', 'PCLN', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA',
             'PHM',
             'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RRC', 'RTN', 'O', 'RHT', 'REGN', 'RF', 'RSG', 'RAI', 'RHI', 'ROK',
             'COL', 'ROP', 'ROST', 'RLD', 'R', 'CRM', 'SNDK', 'SCG', 'SLB', 'SNI', 'STX', 'SEE', 'SRE', 'SHW', 'SPG',
             'SWKS', 'SLG', 'SJM', 'SNA', 'SO', 'LUV', 'SWN', 'SE', 'STJ', 'SWK', 'SPLS', 'SBUX', 'HOT', 'STT', 'SRCL',
             'SYK', 'STI', 'SYMC', 'SYY', 'TROW', 'TGT', 'TEL', 'TE', 'TGNA', 'THC', 'TDC', 'TSO', 'TXN', 'TXT', 'HSY',
             'TRV', 'TMO', 'TIF', 'TWX', 'TWC', 'TJX', 'TMK', 'TSS', 'TSCO', 'RIG', 'TRIP', 'FOXA', 'TSN', 'TYC', 'UA',
             'UNP', 'UNH', 'UPS', 'URI', 'UTX', 'UHS', 'UNM', 'URBN', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VZ', 'VRTX',
             'VIAB', 'V', 'VNO', 'VMC', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'ANTM', 'WFC', 'WDC', 'WU', 'WY', 'WHR',
             'WFM',
             'WMB', 'WEC', 'WYN', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XL', 'XYL', 'YHOO', 'YUM', 'ZBH', 'ZION', 'ZTS', 'IVV',
             'IJR', 'IYE', 'IYF', 'IYJ', 'IYK', 'IYM', 'IYZ', 'IYW']

temp = []

price_dict = {}
failed = []

url = 'https://query1.finance.yahoo.com/v7/finance/options/'
today = str(int(time.time()))
# i added db to the sp500 just because i want the data

run_code = test_if_open(url, 'AAPL')


stock_Q = Queue()
put_Q = Queue()
call_Q = Queue()
add_stock = add_to_quote_table(today)
add_call = addToTable(today, option_type="call")
add_put = addToTable(today, option_type="put")

if run_code:
    for i, stock in enumerate(all_tickers):
        print(stock)
        print(i/len(all_tickers)*100)
        try:
            result = requests.get(url+stock+'?')
            result = result.json()
            dates = result['optionChain']['result'][0]['expirationDates']
            add_stock.send([result['optionChain']['result'][0]['quote'], stock])
            for d in dates:
                result = requests.get(url + stock + '?&date=' + str(d))
                result = result.json()
                for call in result["optionChain"]["result"][0]["options"][0]["calls"]:
                    add_call.send([call, stock])
                for put in result["optionChain"]["result"][0]["options"][0]["puts"]:
                    add_put.send([put, stock])
        except TypeError:
            failed.append(stock)
            print("Didn't find dates for " + stock)
        except IndexError:
            failed.append(stock)
            print('index error for '+stock)
        except json.decoder.JSONDecodeError:
            failed.append(stock)
            print('json error')

    print(call_Q.qsize())

    while not call_Q.empty():
        print(call_Q)
        print(put_Q)
        time.sleep(30)

    add_stock.close()
    add_call.close()
    add_put.close()
    print(failed)
    print(len(failed))
else:
    print("market closed today")


