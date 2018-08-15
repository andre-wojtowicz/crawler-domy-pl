import argparse
import datetime
import multiprocessing
import os
import re
import requests
import signal
from sys import exit

from bs4 import BeautifulSoup
import progressbar

# ______________________________________________________________________________________________________________________

OUTPUT_DIR = "output"
OUTPUT_FILE_PFX = str(datetime.datetime.now())[:19].replace(":", "").replace(" ", "_")
OFFERS_PER_PAGE = 75  # 25, 50, 75
MAX_IMAGES_DOWNLOAD = 2
CHECKIP_URL = "http://checkip.dyn.com"
TEST_URL_FILE = "test-url.txt"

# ______________________________________________________________________________________________________________________

class Spider:
    cores = None
    scan_type = None
    max_images_download = 0
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
    img_dir_path = None
    num_downloaded_photos = 0

    @classmethod
    def scan_root_url(cls):

        r = requests.get(cls.root_url)
        soup = BeautifulSoup(r.content, features="html.parser")

        sst = soup.find("span", {'class': 'mi_defaultValue'})
        if sst is not None:
            cls.scan_type = sst.text.strip()

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

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_pages))
        cls.pb_val = 0

        aa_results = []

        for page in cls.lo_pages:
            aa_results.append(pool.apply_async(mp_parse_pages, (page, cls.m_keyboard_event), callback=cls.__pb_update))

        pool.close()
        pool.join()

        if cls.m_keyboard_event.is_set():
            exit(-9)

        cls.pb.finish()

        for result in aa_results:
            for link in result.get():
                cls.lo_offers_links.append(link)

        signal.signal(signal.SIGINT, original_sigint_handler)

    @classmethod
    def collect_offers(cls):

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_offers_links))
        cls.pb_val = 0

        aa_results = []

        for link in cls.lo_offers_links:
            aa_results.append(pool.apply_async(mp_collect_offers, (link, cls.m_keyboard_event), callback=cls.__pb_update))

        pool.close()
        pool.join()

        if cls.m_keyboard_event.is_set():
            exit(-9)

        cls.pb.finish()

        for result in aa_results:
            cls.lo_offers_html.append(result.get())

        signal.signal(signal.SIGINT, original_sigint_handler)

    @classmethod
    def parse_offers(cls):

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_offers_html))
        cls.pb_val = 0

        aa_results = []

        for html in cls.lo_offers_html:
            aa_results.append(pool.apply_async(mp_parse_offers, (html, cls.m_keyboard_event), callback=cls.__pb_update))

        pool.close()
        pool.join()

        if cls.m_keyboard_event.is_set():
            exit(-9)

        cls.pb.finish()

        for result in aa_results:
            cls.lo_offers_objs.append(result.get())

        signal.signal(signal.SIGINT, original_sigint_handler)

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

        cls.csv_file_name = f"{OUTPUT_FILE_PFX}_{stype}.csv" if cls.csv_file_name is None else cls.csv_file_name

        cls.csv_file_path = f"{OUTPUT_DIR}/{cls.csv_file_name}"

        csv_header = '"Lp";"Adm1";"Adm2";"Ulica";"Rynek pierwotny";"Cena";"Powierzchnia użytkowa";"Powierzchnia mieszkalna";"Liczba pokoi";"Rok budowy";"Piętro";"Typ budynku";"Winda";"Miejsca parkingowe";"Stan budynku";"Stan nieruchomości";"GPSX";"GPSY";"Url";"Zdjęcie"'

        with open(cls.csv_file_path, mode="w", encoding="utf8") as f:

            f.write(csv_header + '\n')

            for i, o in enumerate(cls.lo_offers_objs):
                f.write(o.csv_line(i + 1) + '\n')

    @classmethod
    def download_photos(cls):

        cls.img_dir_path = OUTPUT_DIR + "/" + cls.csv_file_name.split(".")[0]

        try:
            os.mkdir(cls.img_dir_path)
        except FileExistsError:
            pass

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool = multiprocessing.Pool(cls.cores)

        cls.pb = progressbar.ProgressBar(max_value=len(cls.lo_offers_objs))
        cls.pb_val = 0

        aa_results = []

        for offer in cls.lo_offers_objs:
            aa_results.append(pool.apply_async(mp_download_photos, (offer, cls.img_dir_path, cls.max_images_download,
                                                                    cls.m_keyboard_event), callback=cls.__pb_update))

        pool.close()
        pool.join()

        if cls.m_keyboard_event.is_set():
            exit(-9)

        cls.pb.finish()

        cls.num_downloaded_photos = sum([i.get() for i in aa_results])

        signal.signal(signal.SIGINT, original_sigint_handler)

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
    photo_urls = None

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

def mp_parse_pages(r_content, m_keyboard_event):

    if not m_keyboard_event.is_set():
        try:
            soup = BeautifulSoup(r_content, features="html.parser")

            objs = soup.find_all("a", {'class': 'property_link'})

            return [bs_obj["href"] for bs_obj in objs]
        except KeyboardInterrupt:
            m_keyboard_event.set()

    return None

def mp_collect_offers(link, m_keyboard_event):

    if not m_keyboard_event.is_set():
        try:
            r = requests.get(link)
            return (link, r.content)
        except KeyboardInterrupt:
            m_keyboard_event.set()

    return None

def mp_parse_offers(v, m_keyboard_event):
    if not m_keyboard_event.is_set():
        try:
            link, r_content = v

            offer = Offer(link)

            soup = BeautifulSoup(r_content, features="html.parser")

            h_location = soup.find_all("span", {'typeof': 'v:Breadcrumb'})

            if h_location != []:
                if len(h_location) > 4:
                    sobj = h_location[4].find("a")
                    if sobj is not None:
                        offer.adm_1 = sobj.text
                if len(h_location) > 5:
                    sobj = h_location[5].find("a")
                    if sobj is not None:
                        offer.adm_2 = sobj.text

            h_street = re.search('"street":.*?,', soup.prettify())

            if h_street is None:
                offer.street = None
            else:
                h_street_str = h_street.group(0)

                if "null" in h_street_str:
                    offer.street = None
                else:
                    try:
                        offer.street = h_street_str.split('"')[3].encode().decode("unicode_escape").strip()
                    except UnicodeDecodeError:
                        offer.street = None

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

            offer.photo_urls = []

            img_i = 0
            while True:
                img_li = soup.find("li", {"class": f"image{img_i}"})
                if img_li is None:
                    break
                img_mp = img_li.find("img")
                if img_mp is None:
                    break
                img_src = img_mp.get("src")
                img_src_new = re.sub(r"/(\d+)/(\d+)/\d/thumbnail.jpg", "/640/480/2/thumbnail.jpg", img_src)
                offer.photo_urls.append(img_src_new)
                img_i += 1

            return offer

        except KeyboardInterrupt:
            m_keyboard_event.set()

    return None

def mp_download_photos(offer, img_dir_path, max_images_download, m_keyboard_event):

    if not m_keyboard_event.is_set():
        try:
            di = 0

            for link in offer.photo_urls:
                if di >= max_images_download:
                    break

                r = requests.get(link)
                di += 1

                with open(img_dir_path + "/" + offer.photo_prefix + "_" + str(di) + "." + link.split(".")[-1], 'wb') as f:
                    for chunk in r:
                        f.write(chunk)

            return di
        except KeyboardInterrupt:
            m_keyboard_event.set()

    return 0

# ______________________________________________________________________________________________________________________

if __name__ == '__main__':

    multiprocessing.freeze_support()

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--cores", type=int, default=multiprocessing.cpu_count(),
                            help="number of cores used in multiprocessing routines, "
                                 "negative number decreases max available,"
                                 "default (vary on hosts): " + str(multiprocessing.cpu_count()))
        parser.add_argument("-i", "--max-images-download", type=int, default=MAX_IMAGES_DOWNLOAD,
                            help=f"maximum number of images to download, default: {MAX_IMAGES_DOWNLOAD}")
        parser.add_argument("-p", "--offers-per-page", type=int, default=75,
                            help=f"number of offers per page (25, 50, 75), default: {OFFERS_PER_PAGE}")
        parser.add_argument("-o", "--output-csv", type=str,
                            help=f"name of resulting csv file in \"{OUTPUT_DIR}\" directory ")
        parser.add_argument("-t", "--test", action="store_true", help=f"run in test mode and reads url from {TEST_URL_FILE}")

        args = parser.parse_args()

        if args.test and os.path.isfile(TEST_URL_FILE):
            with open(TEST_URL_FILE, 'r') as f:
                Spider.root_url = f.read()
        else:
            Spider.root_url = input("Adres startowy: ")

        if not Spider.root_url.startswith("https://domy.pl/"):
            print("Niepoprawny adres startowy; powinien zaczynać się od https://domy.pl/")
            exit(1)

        if args.offers_per_page not in (25, 50, 75):
            print(f"Niepoprawny limit ofert na stronę: {args.offers_per_page} (dopuszczalne wartości: 25, 50, 75)")
            exit(-1)

        if "&limit" not in Spider.root_url:
            Spider.root_url += f"&limit={args.offers_per_page}"
        else:
            Spider.root_url = re.sub(r"&limit=(\d+)", f"&limit={args.offers_per_page}", Spider.root_url)

        print(f"Limit ogłoszeń na stronę: {args.offers_per_page}")

        Spider.max_images_download = args.max_images_download if args.max_images_download >= 0 else MAX_IMAGES_DOWNLOAD

        print(f"Limit pobieranych zdjęć z ogłoszenia: {Spider.max_images_download}")

        if args.output_csv is not None:
            Spider.csv_file_name = args.output_csv

        # configure multiprocessing

        if args.cores == 0:
            Spider.cores = multiprocessing.cpu_count()
        elif args.cores < 0:
            Spider.cores = max(args.cores + multiprocessing.cpu_count(), 1)
        else:
            Spider.cores = args.cores

        print(f"Procesy: {Spider.cores}")

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
            print("Pobieranie pozostałych stron:")
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

        print("Zapisywanie ogłoszeń...")
        Spider.save_csv()
        print(f"Plik: {Spider.csv_file_path}")

        if Spider.max_images_download > 0:
            print("Pobieranie zdjęć z ogłoszeń:")
            Spider.download_photos()
            print(f"Katalog: {Spider.img_dir_path}")
            print(f"Pobrane zdjęcia: {Spider.num_downloaded_photos}")

    except KeyboardInterrupt:
        pass