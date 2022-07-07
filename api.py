import csv
import json

import requests

stocks = ["AAPL", "ABBV"]

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
for stock in stocks:
    querystring = {"ticker_symbol": stock, "years": "5", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "6de699b1d8mshd8bd55d7354877cp1e4523jsn27d1f547437f",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    filename = "Data/"+stock
    columns = ["Open", "High", "Low", "Close", "Adj_close", "Volume", "Date", "Stock_Name"]
    rows = []
    res = json.loads(response.text)
    for i in res["historical prices"]:
        templist = []
        templist = list(i.values())
        templist.append(stock)
        rows.append(templist)

    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(columns)
        csvwriter.writerows(rows)
