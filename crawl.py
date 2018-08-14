import datetime
import multiprocessing
import re
import requests

from bs4 import BeautifulSoup
import progressbar

# ______________________________________________________________________________________________________________________

OUTPUT_DIR = "output"
OUTPUT_FILE_PFX = str(datetime.datetime.now())[:19].replace(":", "").replace(" ", "_")
OFFERS_PER_PAGE = 75  # 25, 50, 75
CHECKIP_URL = "http://checkip.dyn.com"


# ______________________________________________________________________________________________________________________

class Spider:
    scan_type = None
    root_url = None
    offers_found = 0
    offers_warn = False
    offers_real = 0
    offers_downloaded = 0
    pages = 0
    m_manager = None
    first_page = None
    lo_pages = None
    lo_offers_links = None
    lo_offers_html = None
    lo_offers_objs = None
    pb = None
    pb_val = 0

    @classmethod
    def scan_root_url(cls):

        r = requests.get(cls.root_url)
        soup = BeautifulSoup(r.content, features="html.parser")

        cls.scan_type = soup.find("span", {'class': 'mi_defaultValue'}).text.strip()

        obj_offers_found = soup.find("h2", {'class': 'offersFound'})

        if obj_offers_found is not None:

            cls.offers_found = int(obj_offers_found.find("b").text)
            obj_paginator = soup.find_all("div", {'class': 'paginator'})

            cls.pages = 1 if obj_paginator == [] else int(obj_paginator[1].find_all("a")[-2].text)

            if cls.pages * OFFERS_PER_PAGE < cls.offers_found:
                cls.offers_warn = True
                cls.offers_real = cls.pages * OFFERS_PER_PAGE
            else:
                cls.offers_real = cls.offers_found

            cls.first_page = r.content

    @classmethod
    def process_first_page(cls):

        cls.lo_pages.put(cls.first_page)

    @classmethod
    def collect_remaining_pages(cls):

        pool = multiprocessing.Pool()

        cls.lo_pages.put(cls.first_page)

        cls.pb = progressbar.ProgressBar(max_value=cls.pages - 1)
        cls.pb_val = 0

        for i in range(2, cls.pages + 1):
            pool.apply_async(mp_collect_remaining_pages, (i, cls.root_url, cls.lo_pages), callback=cls.__pb_update)

        pool.close()
        pool.join()

        cls.pb.finish()

    @classmethod
    def parse_pages(cls):

        pool = multiprocessing.Pool()

        p_size = cls.lo_pages.qsize()

        cls.pb = progressbar.ProgressBar(max_value=p_size)
        cls.pb_val = 0

        for i in range(p_size):
            pool.apply_async(mp_parse_pages, (i + 1, cls.lo_pages, cls.lo_offers_links), callback=cls.__pb_update)

        pool.close()
        pool.join()

        cls.pb.finish()

    @classmethod
    def collect_offers(cls):

        pool = multiprocessing.Pool()

        cls.pb = progressbar.ProgressBar(max_value=cls.lo_offers_links.qsize())
        cls.pb_val = 0

        for i in range(cls.lo_offers_links.qsize()):
            pool.apply_async(mp_collect_offers, (i + 1, cls.lo_offers_links, cls.lo_offers_html),
                             callback=cls.__pb_update)

        pool.close()
        pool.join()

        cls.pb.finish()

    @classmethod
    def parse_offers(cls):

        pool = multiprocessing.Pool()

        cls.pb = progressbar.ProgressBar(max_value=cls.lo_offers_html.qsize())
        cls.pb_val = 0

        for i in range(cls.lo_offers_html.qsize()):
            pool.apply_async(mp_parse_offers, (i + 1, cls.lo_offers_html, cls.lo_offers_objs),
                             callback=cls.__pb_update)

        pool.close()
        pool.join()

        cls.pb.finish()

    @classmethod
    def __pb_update(cls, result):

        cls.pb_val += 1
        cls.pb.update(cls.pb_val)


class Offer:
    city = None
    district = None
    street = None
    area = None
    rooms = None
    living_area = None
    price = None
    year_of_construction = None
    floor = None
    type_of_building = None
    lift = None
    state_of_property = None
    parking_space = None
    gps_x = None
    gps_y = None
    url = None
    photo_prefix = None

    def __init__(self, url):
        self.url = url


# ______________________________________________________________________________________________________________________

def mp_collect_remaining_pages(i, root_url, q):
    r = requests.get(root_url + "&page=" + str(i))
    q.put(r.content)


def mp_parse_pages(i, q_pages, q_offers_links):
    r_content = q_pages.get()

    soup = BeautifulSoup(r_content, features="html.parser")

    objs = soup.find_all("a", {'class': 'property_link'})

    for bs_obj in objs:
        q_offers_links.put(bs_obj["href"])


def mp_collect_offers(i, q_offers_links, q_offers):
    link = q_offers_links.get()
    r = requests.get(link)
    q_offers.put((link, r.content))


def mp_parse_offers(i, q_offers_html, q_offers_objs):
    link, r_content = q_offers_html.get()

    offer = Offer(link)

    soup = BeautifulSoup(r_content, features="html.parser")

    objs = soup.find_all("a", {'class': 'property_link'})

    q_offers_objs.put(offer)


# ______________________________________________________________________________________________________________________

if __name__ == '__main__':

    # Spider.root_url = input("Adres startowy: ")

    Spider.root_url = "https://domy.pl/mieszkania-wynajem-mazowieckie+warszawa+mokotow-pl?ps%5Badvanced_search%5D=0&ps%5Bsort_order%5D=rank&ps%5Blocation%5D%5Btype%5D=1&ps%5Btransaction%5D=2&ps%5Btype%5D=1&ps%5Blocation%5D%5Btext_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w&ps%5Blocation%5D%5Btext_tmp_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w&ps[living_area_from]=250"

    if not Spider.root_url.startswith("https://domy.pl/"):
        print("Niepoprawny adres startowy; powinien zaczynac sie od https://domy.pl/")
        exit(1)

    if "&limit" not in Spider.root_url:
        Spider.root_url += "&limit={0}".format(OFFERS_PER_PAGE)

    # print host IP

    r = requests.get(CHECKIP_URL)
    soup = BeautifulSoup(r.content, features="html.parser")

    print("Host IP: {0}".format(re.search("\d+\.\d+\.\d+\.\d+", soup.text).group(0)))

    # configure multiprocessing

    Spider.m_manager = multiprocessing.Manager()
    Spider.lo_pages = Spider.m_manager.Queue()
    Spider.lo_offers_links = Spider.m_manager.Queue()
    Spider.lo_offers_html = Spider.m_manager.Queue()
    Spider.lo_offers_objs = Spider.m_manager.Queue()

    # scan root url

    Spider.scan_root_url()

    print(f"Typ ogłoszeń: {Spider.scan_type}")
    print(f"Liczba ogłoszeń: {Spider.offers_real}")

    if Spider.offers_real == 0:
        exit(0)

    if Spider.offers_warn:
        print(f'* W tej lokalizacji znajduje się więcej ogłoszeń ({Spider.offers_found}); ')
        print('* aby je obejrzeć, zawęź kryteria wyszukiwania')

    if Spider.pages > 1:
        print("Pobieranie pozostalych stron:")
        Spider.collect_remaining_pages()
    else:
        Spider.process_first_page()
    print("Pobrane strony: {0}".format(Spider.lo_pages.qsize()))

    print("Parsowanie stron:")
    Spider.parse_pages()
    print("Znalezione linki: {0}".format(Spider.lo_offers_links.qsize()))

    if Spider.lo_offers_links.qsize() == 0:
        exit(-3)

    print("Pobieranie ogłoszeń:")
    Spider.collect_offers()
    print("Pobrane ogłoszenia: {0}".format(Spider.lo_offers_html.qsize()))

    print("Parsowanie ogłoszeń:")
    Spider.parse_offers()
    print("Znalezione ogłoszenia: {0}".format(Spider.lo_offers_objs.qsize()))
