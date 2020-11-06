from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import pandas as pd

import db


base = "https://www.tradegate.de/"
url = "https://www.tradegate.de/orderbuch.php?isin=DE000A1EWWW0"


class CrawledCompanies():
    def __init__(self, company_name, bid, ask, pcs):
        self.company_name = company_name
        self.bid = bid
        self.ask = ask
        self.pcs = pcs

    def __str__(self):
        return self.company_name + " " + self.bid + " " + self.ask + " " + self.pcs


def market_logic():
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html)

    market_list = dict()
    for index, element in enumerate(soup.select('#aktien ul.sidebar a')):
        if index >= 6:
            break
        name = element.text.split('-')[0].replace('®', '')
        market_list[name] = urljoin(base, element.attrs['href'])

    db_market_list = set(key['market_name'] for key in db.get_markets())
    difference = {k: market_list[k] for k in set(market_list) - db_market_list}

    if len(difference) > 0:
        db.add_markets(difference)


def company_logic():
    pass


def get_figures(market_list):
    company_dict = dict()

    for market in market_list.values():
        r = requests.get(market)
        html = r.text
        soup = BeautifulSoup(html)

        for i, elements in enumerate(soup.select("#kursliste_daten tr"), start=1):
            company_name = elements.select_one(f"#name_{i}").text
            bid = elements.select_one(f"#bid_{i}").text
            ask = elements.select_one(f"#ask_{i}").text
            pcs = elements.select_one(f"#sum_{i}").text
            crawled = CrawledCompanies(company_name, bid, ask, pcs)

            values = dict(bid=bid, ask=ask, pcs=pcs)
            company_dict[company_name] = values

    return company_dict


def main():
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html, "html.parser")









if __name__ == "__main__":
    main()
