import requests
import numpy as np
import boto3
import json
import time
import psycopg2


def addToTable(cur, result, price_date, stock, option_type=""):
    priceDict = result
    priceDict["priceDate"] = price_date
    priceDict["underlyingSymbol"] = stock
    priceDict["type"] = option_type
    print("price dict")

    cur.execute("""INSERT INTO prices (pricedate, underlyingsymbol, ask, bid, change, contractsize, contractsymbol,
      currency, expiration, impliedvolatility, inthemoney, lastprice, lasttradedate, openinterest, percentchange,
      strike, volume, optiontype) VALUES (%(priceDate)s, %(underlyingSymbol)s, %(ask)s, %(bid)s, %(change)s, %(contractSize)s,
      %(contractSymbol)s, %(currency)s, %(expiration)s, %(impliedVolatility)s, %(inTheMoney)s, %(lastPrice)s,
      %(lastTradeDate)s, %(openInterest)s, %(percentChange)s, %(strike)s, %(volume)s, %(type)s);""", priceDict)


def add_to_quote_table(cur, res, price_date):
    info = {"ask": None, "askSize": None, "averageDailyVolume10Day": None, "averageDailyVolume3Month": None, "bid": None,
        "bidSize": None,"bookValue": None, "currency": None, "dividendDate": None,"earningsTimestamp": None,
        "earningsTimestampEnd": None, "earningsTimestampStart": None,"epsForward": None, "epsTrailingTwelveMonths": None,
        "esgPopulated": None,"exchange": None, "exchangeDataDelayedBy": None, "exchangeTimezoneName": None,
        "exchangeTimezoneShortName": None, "fiftyDayAverage": None, "fiftyDayAverageChange": None,
        "fiftyDayAverageChangePercent": None, "fiftyTwoWeekHigh": None, "fiftyTwoWeekHighChange": None,
        "fiftyTwoWeekHighChangePercent": None, "fiftyTwoWeekLow": None, "fiftyTwoWeekLowChange": None,
        "fiftyTwoWeekLowChangePercent": None, "fiftyTwoWeekRange": None, "financialCurrency": None, "forwardPE": None,
        "fullExchangeName": None, "gmtOffSetMilliseconds": None, "language": None, "longName": None, "market": None,
        "marketCap": None, "marketState": None, "messageBoardId": None, "postMarketChange": None,
        "postMarketChangePercent": None, "postMarketPrice": None, "postMarketTime": None, "priceHint": None,
        "priceToBook": None, "quoteSourceName": None, "quoteType": None, "region": None, "regularMarketChange": None,
        "regularMarketChangePercent": None, "regularMarketDayHigh": None, "regularMarketDayLow": None ,
        "regularMarketDayRange": None, "regularMarketOpen": None, "regularMarketPreviousClose": None,
        "regularMarketPrice": None, "regularMarketTime": None, "regularMarketVolume": None, "sharesOutstanding": None,
        "shortName": None, "sourceInterval": None, "symbol": None, "tradeable": None, "trailingAnnualDividendRate": None,
        "trailingAnnualDividendYield": None, "trailingPE": None, "twoHundredDayAverage": None,
        "twoHundredDayAverageChange": None, "twoHundredDayAverageChangePercent": None}

    info.update(res)
    info["pricedate"] = price_date
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
      twohundreddayaveragechange, twohundreddayaveragechangepercent, pricedate) VALUES (%(ask)s, %(askSize)s, 
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
      %(twoHundredDayAverageChange)s, %(twoHundredDayAverageChangePercent)s, %(pricedate)s);""", info)


SP500 = ['DB','AAPL', 'ABT', 'ABBV', 'ACN', 'ACE', 'ADBE', 'ADT', 'AAP', 'AES', 'AET', 'AFL',
'AMG', 'A', 'GAS', 'ARE', 'APD', 'AKAM', 'AA', 'AGN', 'ALXN', 'ALLE', 'ADS', 'ALL', 'ALTR', 'MO', 'AMZN', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'APC', 'ADI', 'AON', 'APA', 'AIV', 'AMAT', 'ADM', 'AIZ', 'T', 'ADSK', 'ADP', 'AN', 'AZO', 'AVGO', 'AVB', 'AVY', 'BHI', 'BLL', 'BAC', 'BK', 'BCR', 'BXLT', 'BAX', 'BBT', 'BDX', 'BBBY', 'BRK.B', 'BBY', 'BLX', 'HRB', 'BA', 'BWA', 'BXP', 'BSX', 'BMY', 'BRCM', 'BF.B', 'CHRW', 'CA', 'CVC', 'COG', 'CAM', 'CPB', 'COF', 'CAH', 'HSIC', 'KMX', 'CCL', 'CAT', 'CBG', 'CBS', 'CELG', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHK', 'CVX', 'CMG', 'CB', 'CI', 'XEC', 'CINF', 'CTAS', 'CSCO', 'C', 'CTXS', 'CLX', 'CME', 'CMS', 'COH', 'KO', 'CCE', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CSC', 'CAG', 'COP', 'CNX', 'ED', 'STZ', 'GLW', 'COST', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DLPH', 'DAL', 'XRAY', 'DVN', 'DO', 'DTV', 'DFS', 'DISCA', 'DISCK', 'DG', 'DLTR', 'D', 'DOV', 'DOW', 'DPS', 'DTE', 'DD', 'DUK', 'DNB', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMC', 'EMR', 'ENDP', 'ESV', 'ETR', 'EOG', 'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'ES', 'EXC', 'EXPE', 'EXPD', 'ESRX', 'XOM', 'FFIV', 'FB', 'FAST', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FISV', 'FLIR', 'FLS', 'FLR', 'FMC', 'FTI', 'F', 'FOSL', 'BEN', 'FCX', 'FTR', 'GME', 'GPS', 'GRMN', 'GD', 'GE', 'GGP', 'GIS', 'GM', 'GPC', 'GNW', 'GILD', 'GS', 'GT', 'GOOGL', 'GOOG', 'GWW', 'HAL', 'HBI', 'HOG', 'HAR', 'HRS', 'HIG', 'HAS', 'HCA', 'HCP', 'HCN', 'HP', 'HES', 'HPQ', 'HD', 'HON', 'HRL', 'HSP', 'HST', 'HCBK', 'HUM', 'HBAN', 'ITW', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IRM', 'JEC', 'JBHT', 'JNJ', 'JCI', 'JOY', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'GMCR', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KRFT', 'KR', 'LB', 'LLL', 'LH', 'LRCX', 'LM', 'LEG', 'LEN', 'LVLT', 'LUK', 'LLY', 'LNC', 'LLTC', 'LMT', 'L', 'LOW', 'LYB', 'MTB', 'MAC', 'M', 'MNK', 'MRO', 'MPC', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MAT', 'MKC', 'MCD', 'MCK', 'MJN', 'MMV', 'MDT', 'MRK', 'MET', 'KORS', 'MCHP', 'MU', 'MSFT', 'MHK', 'TAP', 'MDLZ', 'MON', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MUR', 'MYL', 'NDAQ', 'NOV', 'NAVI', 'NTAP', 'NFLX', 'NWL', 'NFX', 'NEM', 'NWSA', 'NEE', 'NLSN', 'NKE', 'NI', 'NE', 'NBL', 'JWN', 'NSC', 'NTRS', 'NOC', 'NRG', 'NUE', 'NVDA', 'ORLY', 'OXY', 'OMC', 'OKE', 'ORCL', 'OI', 'PCAR', 'PLL', 'PH', 'PDCO', 'PAYX', 'PNR', 'PBCT', 'POM', 'PEP', 'PKI', 'PRGO', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PBI', 'PCL', 'PNC', 'RL', 'PPG', 'PPL', 'PX', 'PCP', 'PCLN', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RRC', 'RTN', 'O', 'RHT', 'REGN', 'RF', 'RSG', 'RAI', 'RHI', 'ROK', 'COL', 'ROP', 'ROST', 'RLD', 'R', 'CRM', 'SNDK', 'SCG', 'SLB', 'SNI', 'STX', 'SEE', 'SRE', 'SHW', 'SPG', 'SWKS', 'SLG', 'SJM', 'SNA', 'SO', 'LUV', 'SWN', 'SE', 'STJ', 'SWK', 'SPLS', 'SBUX', 'HOT', 'STT', 'SRCL', 'SYK', 'STI', 'SYMC', 'SYY', 'TROW', 'TGT', 'TEL', 'TE', 'TGNA', 'THC', 'TDC', 'TSO', 'TXN', 'TXT', 'HSY', 'TRV', 'TMO', 'TIF', 'TWX', 'TWC', 'TJX', 'TMK', 'TSS', 'TSCO', 'RIG', 'TRIP', 'FOXA', 'TSN', 'TYC', 'UA', 'UNP', 'UNH', 'UPS', 'URI', 'UTX', 'UHS', 'UNM', 'URBN', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VZ', 'VRTX', 'VIAB', 'V', 'VNO', 'VMC', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'ANTM', 'WFC', 'WDC', 'WU', 'WY', 'WHR', 'WFM', 'WMB', 'WEC', 'WYN', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XL', 'XYL', 'YHOO', 'YUM', 'ZBH', 'ZION', 'ZTS']
temp = ["DB", "AAPL"]
price_dict = json.load(open('options_prices.json', 'r'))
#price_dict = {}
failed = []
url = 'https://query1.finance.yahoo.com/v7/finance/options/'
today = str(int(time.time()))
# i added db to the sp500 just because i want the data
conn = psycopg2.connect(host="localhost", database="options_prices", user="postgres", password="mypassword")
cur = conn.cursor()

for stock in SP500:
    print(stock)
    if stock not in price_dict:
        price_dict.update({stock: {}})
    result = requests.get(url+stock+'?')
    result = result.json()
    try:
        dates = result['optionChain']['result'][0]['expirationDates']
        add_to_quote_table(cur, result['optionChain']['result'][0]['quote'], today)
        price_dict[stock].update({today: {}})
        for d in dates:
            result = requests.get(url + stock + '?&date=' + str(d))
            result = result.json()
            for call in result["optionChain"]["result"][0]["options"][0]["calls"]:
                addToTable(cur, call, today, stock, option_type="call")
            for put in result["optionChain"]["result"][0]["options"][0]["puts"]:
                addToTable(cur, put, today, stock, option_type="put")
            price_dict[stock][today].update({d: result})
    except TypeError:
        failed.append(stock)
        print("Didn't find dates for " + stock)
    except IndexError:
        failed.append(stock)
        print('index error for '+stock)

conn.commit()
print(price_dict)
json.dump(price_dict, open('options_prices_temp.json', 'w'), indent=4, sort_keys=True)

# will need to make an authentication file

#s3 = boto3.resource('s3') # tell it what service we are using

# Prints out bucket names
#for bucket in s3.buckets.all():
#    print(bucket.name)

# assuming "my-bucket already exists, this uploads a new file
#data = json.load(open('options_prices_temp.json', 'r'))
#s3.Bucket('real-fun-estate-data').put_object(Key='options_prices.json', Body=data)



