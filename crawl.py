import datetime
import multiprocessing
import os
import re
import requests

from bs4 import BeautifulSoup
import progressbar

# ______________________________________________________________________________________________________________________

OUTPUT_DIR = "output"
IMG_DIR = "img"
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
    csv_file_path = None

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
    def save_csv(cls):

        try:
            os.mkdir(OUTPUT_DIR)
        except FileExistsError:
            pass

        stype = "inne"
        if Spider.scan_type == "Mieszkania do wynajęcia":
            stype = "mieszkania-wynajem"
        elif Spider.scan_type == "Mieszkania na sprzedaż":
            stype = "mieszkania-sprzedaz"
        elif Spider.scan_type == "Mieszkania":
            stype = "mieszkania"

        cls.csv_file_path = f"{OUTPUT_DIR}/{OUTPUT_FILE_PFX}_{stype}.csv"

        csv_header = '"Lp";"Adm1";"Adm2";"Ulica";"Rynek pierwotny";"Cena";"Powierzchnia użytkowa";"Powierzchnia mieszkalna";"Liczba pokoi";"Rok budowy";"Piętro";"Typ budynku";"Winda";"Miejsca parkingowe";"Stan budynku";"Stan nieruchomości";"GPSX";"GPSY";"Url";"Zdjęcie"'

        with open(cls.csv_file_path, mode="w", encoding="utf8") as f:

            f.write(csv_header + '\n')

            i = 1
            while cls.lo_offers_objs.qsize() > 0:
                o = cls.lo_offers_objs.get()
                f.write(o.csv_line(i) + '\n')
                i += 1

    @classmethod
    def __pb_update(cls, result):

        cls.pb_val += 1
        cls.pb.update(cls.pb_val)


class Offer:
    adm_1 = None
    adm_2 = None
    street = None
    primary_market = None
    area = None
    rooms = None
    living_area = None
    price = None
    year_of_construction = None
    floor = None
    type_of_building = None
    lift = None
    state_of_building = None
    state_of_property = None
    parking_space = None
    gps_x = None
    gps_y = None
    url = None
    photo_prefix = None

    def __init__(self, url):
        self.url = url

    def csv_line(self, i):

        line = '"' + str(i) + '";'
        line += '"' + (self.adm_1 if self.adm_1 is not None else '') + '";'
        line += '"' + (self.adm_2 if self.adm_2 is not None else '') + '";'
        line += '"' + (self.street if self.street is not None else '') + '";'
        line += '"' + (self.primary_market if self.primary_market is not None else '') + '";'
        line += '"' + (self.price if self.price is not None else '') + '";'
        line += '"' + (self.area if self.area is not None else '') + '";'
        line += '"' + (self.living_area if self.living_area is not None else '') + '";'
        line += '"' + (self.rooms if self.rooms is not None else '') + '";'
        line += '"' + (self.year_of_construction if self.year_of_construction is not None else '') + '";'
        line += '"' + (self.floor if self.floor is not None else '') + '";'
        line += '"' + (self.type_of_building if self.type_of_building is not None else '') + '";'
        line += '"' + (self.lift if self.lift is not None else '') + '";'
        line += '"' + (self.parking_space if self.parking_space is not None else '') + '";'
        line += '"' + (self.state_of_building if self.state_of_building is not None else '') + '";'
        line += '"' + (self.state_of_property if self.state_of_property is not None else '') + '";'
        line += '"' + (self.gps_x if self.gps_x is not None else '') + '";'
        line += '"' + (self.gps_y if self.gps_y is not None else '') + '";'
        line += '"' + (self.url if self.url is not None else '') + '";'
        line += '"' + (self.photo_prefix if self.photo_prefix is not None else '') + '"'

        return line

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

    #h_location = soup.find("h1").find("strong").text.split(",")
    #
    #offer.adm_1 = h_location[0].strip() if len(h_location) > 0 else None
    #offer.adm_2 = h_location[1].strip() if len(h_location) > 1 else None
    #offer.street = h_location[-1].strip() if len(h_location) >= 3 else None

    h_location = soup.find_all("span", {'typeof': 'v:Breadcrumb'})

    offer.adm_1 = h_location[4].find("a").text
    offer.adm_2 = h_location[5].find("a").text

    h_street = re.search('"street":.*?,', soup.prettify())

    if h_street is None:
        offer.street = None
    else:
        h_street_str = re.search('"street":.*?,', soup.prettify()).group(0)

        if "null" in h_street_str:
            offer.street = None
        else:
            offer.street = h_street_str.split('"')[3].encode().decode("unicode_escape").strip()

    h_params = soup.find_all("div", {"class": "paramsItem"})

    for v in h_params:
        if "Cena:" in v.text:
            offer.price = v.find("strong").text.replace(u'\xa0', u' ')
        elif "Rynek pierwotny:" in v.text:
            offer.primary_market = v.find("strong").text
        elif "Powierzchnia użytkowa:" in v.text:
            offer.area = v.find("strong").text
        elif "Powierzchnia mieszkalna:" in v.text:
            offer.living_area = v.find("strong").text
        elif "Liczba pokoi:" in v.text:
            offer.rooms = v.find("strong").text
        elif "Rok budowy:" in v.text:
            offer.year_of_construction = v.find("strong").text
        elif "Piętro:" in v.text:
            offer.floor = v.find("strong").text
        elif "Typ budynku:" in v.text:
            offer.type_of_building = v.find("strong").text
        elif "Winda:" in v.text:
            offer.lift = v.find("strong").text
        elif "Miejsca parkingowe:" in v.text:
            offer.parking_space = v.find("strong").text
        elif "Stan budynku:" in v.text:
            offer.state_of_building = v.find("strong").text
        elif "Stan nieruchomości:" in v.text:
            offer.state_of_property = v.find("strong").text

    google_maps = soup.find("div", {"class" : "GoogleMap"})

    offer.gps_x = google_maps.get("data-lat") if google_maps is not None else None
    offer.gps_y = google_maps.get("data-lng") if google_maps is not None else None

    offer.photo_prefix = link.split("/")[-1]

    q_offers_objs.put(offer)


# ______________________________________________________________________________________________________________________

if __name__ == '__main__':

    # Spider.root_url = input("Adres startowy: ")

    Spider.root_url = "https://domy.pl/inwestycje/szukaj?ps%5Btype%5D=998&ps%5Badvanced_search%5D=1&ps%5Blocation%5D%5Btype%5D=1&ps%5Bsub_type%5D=1&ps%5Blocation%5D%5Btext_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w&ps%5Blocation%5D%5Btext_tmp_queue%5D%5B%5D=mazowieckie+Warszawa+Mokot%C3%B3w&ps%5Bliving_area_from%5D=80&ps%5Bcompletion_date_to_month%5D=12&ps%5Bcompletion_date_to_year%5D=2022"

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
    print("Opracowane ogłoszenia: {0}".format(Spider.lo_offers_objs.qsize()))

    print("Zapisywanie ogłoszeń:")
    Spider.save_csv()
    print(Spider.csv_file_path)
