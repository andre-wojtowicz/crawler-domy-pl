import argparse
import datetime
import multiprocessing
import os
import re
import requests
import signal

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
    cores = None
    scan_type = None
    root_url = None
    offers_found = 0
    offers_warn = False
    offers_real = 0
    offers_downloaded = 0
    pages = 0
    first_page = None
    m_manager = None
    m_keyboard_event = None
    lo_pages = []
    lo_offers_links = []
    lo_offers_html = []
    lo_offers_objs = []
    pb = None
    pb_val = 0
    csv_file_name = None
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

        cls.lo_pages.append(cls.first_page)

    @classmethod
    def collect_remaining_pages(cls):

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        pool = multiprocessing.Pool(cls.cores)

        cls.lo_pages.append(cls.first_page)

        cls.pb = progressbar.ProgressBar(max_value=cls.pages - 1)
        cls.pb_val = 0

        aa_results = []

        for i in range(2, cls.pages + 1):
            aa_results.append(pool.apply_async(mp_collect_remaining_pages, (i, cls.root_url, cls.m_keyboard_event),
                                               callback=cls.__pb_update))

        pool.close()
        pool.join()

        if cls.m_keyboard_event.is_set():
            exit(-9)

        cls.pb.finish()

        for result in aa_results:
            cls.lo_pages.append(result.get())

        signal.signal(signal.SIGINT, original_sigint_handler)

    @classmethod
    def parse_pages(cls):

        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_pages))
        cls.pb_val = 0

        aa_results = []

        for page in cls.lo_pages:
            aa_results.append(pool.apply_async(mp_parse_pages, (page, ), callback=cls.__pb_update))

        pool.close()
        pool.join()

        cls.pb.finish()

        for result in aa_results:
            for link in result.get():
                cls.lo_offers_links.append(link)

    @classmethod
    def collect_offers(cls):

        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_offers_links))
        cls.pb_val = 0

        aa_results = []

        for link in cls.lo_offers_links:
            aa_results.append(pool.apply_async(mp_collect_offers, (link, ), callback=cls.__pb_update))

        pool.close()
        pool.join()

        cls.pb.finish()

        for result in aa_results:
            cls.lo_offers_html.append(result.get())

    @classmethod
    def parse_offers(cls):

        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_offers_html))
        cls.pb_val = 0

        aa_results = []

        for html in cls.lo_offers_html:
            aa_results.append(pool.apply_async(mp_parse_offers, (html, ), callback=cls.__pb_update))

        pool.close()
        pool.join()

        cls.pb.finish()

        for result in aa_results:
            cls.lo_offers_objs.append(result.get())

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

        fname = f"{OUTPUT_FILE_PFX}_{stype}.csv" if cls.csv_file_name is None else cls.csv_file_name

        cls.csv_file_path = f"{OUTPUT_DIR}/{fname}"

        csv_header = '"Lp";"Adm1";"Adm2";"Ulica";"Rynek pierwotny";"Cena";"Powierzchnia użytkowa";"Powierzchnia mieszkalna";"Liczba pokoi";"Rok budowy";"Piętro";"Typ budynku";"Winda";"Miejsca parkingowe";"Stan budynku";"Stan nieruchomości";"GPSX";"GPSY";"Url";"Zdjęcie"'

        with open(cls.csv_file_path, mode="w", encoding="utf8") as f:

            f.write(csv_header + '\n')

            for i, o in enumerate(cls.lo_offers_objs):
                f.write(o.csv_line(i + 1) + '\n')

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


def mp_collect_remaining_pages(i, root_url, m_keyboard_event):

    if not m_keyboard_event.is_set():
        try:
            r = requests.get(root_url + "&page=" + str(i))
            return r.content
        except KeyboardInterrupt:
            m_keyboard_event.set()

    return None

def mp_parse_pages(r_content):
    soup = BeautifulSoup(r_content, features="html.parser")

    objs = soup.find_all("a", {'class': 'property_link'})

    return [bs_obj["href"] for bs_obj in objs]


def mp_collect_offers(link):
    r = requests.get(link)
    return (link, r.content)


def mp_parse_offers(v):
    link, r_content = v

    offer = Offer(link)

    soup = BeautifulSoup(r_content, features="html.parser")

    h_location = soup.find_all("span", {'typeof': 'v:Breadcrumb'})

    offer.adm_1 = h_location[4].find("a").text
    offer.adm_2 = h_location[5].find("a").text

    h_street = re.search('"street":.*?,', soup.prettify())

    if h_street is None:
        offer.street = None
    else:
        h_street_str = h_street.group(0)

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

    return offer


# ______________________________________________________________________________________________________________________

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cores", type=int, default=multiprocessing.cpu_count(),
                        help="number of cores used in multiprocessing routines, "
                             "negative number decreases max available,"
                             "default (vary on hosts): " + str(multiprocessing.cpu_count()))
    parser.add_argument("-p", "--offers-per-page", type=int, default=75,
                        help=f"number of offers per page (25, 50, 75), default: {OFFERS_PER_PAGE}")
    parser.add_argument("-o", "--output-csv", type=str,
                        help=f"name of resulting csv file in \"{OUTPUT_DIR}\" directory ")
    parser.add_argument("-t", "--test", action="store_true", help="run in test mode")

    args = parser.parse_args()

    if args.test:
        Spider.root_url = "https://domy.pl/mieszkania-sprzedaz-warszawa+mokotow-pl?ps%5Badvanced_search%5D=0&ps%5Bsort_order%5D=rank&ps%5Blocation%5D%5Btype%5D=1&ps%5Btransaction%5D=1&ps%5Btype%5D=1&ps%5Blocation%5D%5Btext_queue%5D%5B%5D=Warszawa+Mokot%C3%B3w&ps%5Blocation%5D%5Btext_tmp_queue%5D%5B%5D=Warszawa+Mokot%C3%B3w"
    else:
        Spider.root_url = input("Adres startowy: ")

    if not Spider.root_url.startswith("https://domy.pl/"):
        print("Niepoprawny adres startowy; powinien zaczynac sie od https://domy.pl/")
        exit(1)

    if args.cores == 0:
        Spider.cores = multiprocessing.cpu_count()
    elif args.cores < 0:
        Spider.cores = max(args.cores + multiprocessing.cpu_count(), 1)
    else:
        Spider.cores = args.cores

    print(f"Procesy: {Spider.cores}")

    if "&limit" not in Spider.root_url:
        Spider.root_url += f"&limit={args.offers_per_page}"
    else:
        Spider.root_url = re.sub(r"&limit=(\d+)", f"&limit={args.offers_per_page}", Spider.root_url)

    print(f"Limit ogłoszeń na stronę: {args.offers_per_page}")

    if args.output_csv is not None:
        Spider.csv_file_name = args.output_csv

    # configure multiprocessing

    Spider.m_manager = multiprocessing.Manager()
    Spider.m_keyboard_event = Spider.m_manager.Event()

    # print host IP

    r = requests.get(CHECKIP_URL)
    soup = BeautifulSoup(r.content, features="html.parser")

    print("Host IP: {0}".format(re.search(r"\d+\.\d+\.\d+\.\d+", soup.text).group(0)))

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
    print("Pobrane strony: {0}".format(len(Spider.lo_pages)))

    print("Parsowanie stron:")
    Spider.parse_pages()
    print("Znalezione linki: {0}".format(len(Spider.lo_offers_links)))

    if len(Spider.lo_offers_links) == 0:
        exit(-3)

    print("Pobieranie ogłoszeń:")
    Spider.collect_offers()
    print("Pobrane ogłoszenia: {0}".format(len(Spider.lo_offers_html)))

    print("Parsowanie ogłoszeń:")
    Spider.parse_offers()
    print("Opracowane ogłoszenia: {0}".format(len(Spider.lo_offers_objs)))

    print("Zapisywanie ogłoszeń:")
    Spider.save_csv()
    print(Spider.csv_file_path)
