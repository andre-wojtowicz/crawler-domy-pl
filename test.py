import multiprocessing
import requests
from bs4 import BeautifulSoup

OFFERS_PER_PAGE = 75

url = "https://domy.pl/mieszkania-sprzedaz-mazowieckie+warszawa+mokotow-pl?ps%5Badvanced_search%5D=1&ps%5Bsort_order%5D=rank&ps%5Bsort_asc%5D=0&ps%5Blocation%5D%5Btype%5D=1&limit=75&ps%5Btransaction%5D=1&ps%5Btype%5D=1&ps%5Blocation%5D%5Btext_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w&ps%5Blocation%5D%5Btext_tmp_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w"
url += "&limit={0}".format(OFFERS_PER_PAGE)



checkip_url = "http://checkip.dyn.com"


def fun(i, q):

    print("page {0}".format(i))

    r = requests.get(url + "&page=" + str(i))
    soup = BeautifulSoup(r.content, features="html.parser")
    q.put(soup.text)

if __name__ == '__main__':

    m = multiprocessing.Manager()
    pages_soups = m.Queue()
    urls_offers = []

    r = requests.get(checkip_url)
    soup = BeautifulSoup(r.content, features="html.parser")
    print(soup.find("body").text)

    r = requests.get(url)
    soup = BeautifulSoup(r.content, features="html.parser")

    pages = 0
    offers_found = 0
    obj_offers_found = soup.find("h2", {'class': 'offersFound'})

    if obj_offers_found != None:

        offers_found = int(obj_offers_found.find("b").text)
        obj_paginator = soup.find_all("div", {'class': 'paginator'})

        pages = 1 if obj_paginator == None else int(obj_paginator[1].find_all("a")[-2].text)

        pages_soups.put(soup.text)

    print("Found {0} offers".format(offers_found))



    if pages > 1:

        pool = multiprocessing.Pool()

        for i in range(2, pages+1):
            pool.apply_async(fun, (i, pages_soups))
        pool.close()
        pool.join()


    #        pages_soups.append(soup)

    # for i in range(len(pages_soups)):
    #     print(i+1)
    #     soup = pages_soups[i]
    #     for obj in soup.find_all("a", {'class': 'property_link'}):
    #         urls_offers.append(obj["href"])

    print(pages_soups.qsize())
