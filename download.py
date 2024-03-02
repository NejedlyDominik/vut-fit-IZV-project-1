import os
import io
import re
import csv
import gzip
import errno
import pickle
import zipfile
import requests
import numpy as np
from bs4 import BeautifulSoup


class DataDownloader:
    """
    Download and parse data on accidents in the Czech Republic.

    Attributes:
        url             - the address, from which the data are downloaded
        folder          - location, where the data are stored
        cache_filename  - name of file, where the processed data are stored
        cache_data      - memory, where the already processed data are stored
        headers         - all headers for individual columns in records

    Methods:
        download_data()                 - download data from the specify url and stored them into a specified folder
        parse_region_data(self, region) - parse data of particular region and get records in specific format
        get_list(self, regions=None)    - get records of specified regions
    """

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        """
        Construct all attributes of data downloader object.

        Arguments:
            url             - the address, from which the data are downloaded (default "https://ehw.fit.vutbr.cz/izv/")
            folder          - location, where the data are stored (defaults "data")
            cache_filename  - name of file, where the processed data are stored (defaults "data_{}.pkl.gz")
        """
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.cache_data = {
            'PHA': None, 'STC': None, 'JHC': None, 'PLK': None, 'ULK': None, 'HKK': None, 'JHM': None, 
            'MSK': None, 'OLK': None, 'ZLK': None, 'VYS': None, 'PAK': None, 'LBK': None, 'KVK': None
        }

        self.headers = [
            'region', 'hour', 'minute', 'id', 'road_type', 'road_number', 'date', 'weekday', 'accident_type', 'moving_vehicles_collision_type', 'solid_collision_type', 
            'accident_character', 'accident_fault', 'alcohol_drugs', 'accident_cause', 'killed', 'seriously_injured', 'slightly_injured', 'total_damage', 'surface_type', 
            'surface_condition', 'road_condition', 'wind_conditions', 'visibility', 'viewing_conditions', 'road_division', 'accident_location_on_the_road', 'trafic_management', 
            'driving_priority', 'place_specifics', 'directional_conditions', 'vehicles', 'place', 'cross_road_type', 'vehicle_type', 'brand', 'production_year', 'vehicle_characteristics', 
            'skid', 'vehicle_after_accident', 'material_leaks', 'rescued_people', 'vehicle_direction', 'vehicle_damage', 'driver_category', 'driver_status', 'external_driver_affects', 
            'a', 'b', 'X_coordinate', 'Y_coordinate', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't', 'accident_location'
        ]

    def download_data(self):
        """
        Download data from the specify url and stored them into a specified folder.
        In the folder data are divided into subfolders, each of which represents particular region
        and contains files named by years with records related to this year and region.
        """
        cookies = {
            '_ranaCid': '1774595917.1518884646',
            '_ga': 'GA1.2.1985459551.1518884646',
            '_ranaUid': '309747631849b24de3ffba602100cf5215588f8584b3c03dade77a3bcd666436',
            '_vwo_uuid': 'DC53CDCDDC43B8D88860FF406ACE85C6C',
            '__gads': 'ID=bd6130ffd363785a:T=1555264924:S=ALNI_MYti4-yV6mEfjEFPrRD8v8CD1W8Fw',
            's_pers': '^%^20v8^%^3D1555341518223^%^7C1649949518223^%^3B^%^20v8_s^%^3DLess^%^2520than^%^25201^%^2520day^%^7C1555343318223^%^3B^%^20c19^%^3Dsd^%^253Aproduct^%^253Ajournal^%^253Aarticle^%^7C1555343318235^%^3B^%^20v68^%^3D1555341519305^%^7C1555343318256^%^3B',
            'AMCV_4D6368F454EC41940A4C98A6^%^40AdobeOrg': '1406116232^%^7CMCIDTS^%^7C18001^%^7CMCMID^%^7C83933250191558551383974912806639520463^%^7CMCAAMLH-1555946318^%^7C6^%^7CMCAAMB-1555946318^%^7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y^%^7CMCOPTOUT-1555348615s^%^7CNONE^%^7CMCAID^%^7CNONE^%^7CMCCIDH^%^7C-236890141^%^7CvVersion^%^7C2.5.0',
            'permutive-session': '^%^7B^%^22session_id^%^22^%^3A^%^22ac3c9433-0b78-44bd-9d18-22f87a256fe0^%^22^%^2C^%^22last_updated^%^22^%^3A^%^222020-10-06T08^%^3A54^%^3A50.197Z^%^22^%^7D',
            'permutive-id': 'd33acb13-8f80-4fe1-9d09-0ef23dadbe22',
            '_gcl_au': '1.1.1507695930.1601981333',
            'AMCV_1B6E34B85282A0AC0A490D44^%^40AdobeOrg': '-1303530583^%^7CMCIDTS^%^7C18561^%^7CMCMID^%^7C87472037697157496684194788568814964425^%^7CMCAAMLH-1604230481^%^7C6^%^7CMCAAMB-1604230481^%^7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y^%^7CMCOPTOUT-1603632881s^%^7CNONE^%^7CMCAID^%^7CNONE^%^7CvVersion^%^7C3.3.0',
            'optimizelyEndUserId': 'oeu1603625768748r0.43681580977518464',
            'amplitude_id_9f6c0bb8b82021496164c672a7dc98d6_edmvutbr.cz': 'eyJkZXZpY2VJZCI6IjUwZTc4Y2YxLTMyN2EtNDBhNy1iZTRjLTI2ZDk2NDUxZjM1ZlIiLCJ1c2VySWQiOm51bGwsIm9wdE91dCI6ZmFsc2UsInNlc3Npb25JZCI6MTYwMzYyNTc3MDA1MCwibGFzdEV2ZW50VGltZSI6MTYwMzYyNTc4ODM4NiwiZXZlbnRJZCI6MCwiaWRlbnRpZnlJZCI6MzUsInNlcXVlbmNlTnVtYmVyIjozNX0=',
            'amplitude_id_408774472b1245a7df5814f20e7484d0vutbr.cz': 'eyJkZXZpY2VJZCI6ImFjNGJiNDEzLWRkNzItNDIxZi1hNGQzLTNkYjM0MDg5YTk3ZiIsInVzZXJJZCI6bnVsbCwib3B0T3V0IjpmYWxzZSwic2Vzc2lvbklkIjoxNjAzNjI1NzcxMjgzLCJsYXN0RXZlbnRUaW1lIjoxNjAzNjI1NzkwNDE4LCJldmVudElkIjoxNiwiaWRlbnRpZnlJZCI6MTA5LCJzZXF1ZW5jZU51bWJlciI6MTI1fQ==',
        }

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Language': 'cs-CZ,cs;q=0.9',
        }

        response = requests.get(self.url, headers=headers, cookies=cookies)

        # parse html response and find all rows of table, where addresses to data files are stored
        data = BeautifulSoup(response.text, 'html.parser')
        data = data.table
        data = data.find_all('tr')

        newest_urls = {}
        
        # get the latest record for every year and store it into dictionary, where year is the key to address
        for row in data:
            row = row.find_all('td')
            
            if(len(row) == 3):
                year = row[0].text

            urltag = row[-1].find('a', href=re.compile('.zip$'))

            if(urltag != None):
                newest_urls[year] = urltag['href']

        # create a folder for storing data, if it not exists
        try:
            os.makedirs(self.folder)
        except OSError as e:
            if(e.errno != errno.EEXIST):
                raise

        # download data for every year
        for year in newest_urls:
            response = requests.get(os.path.join(self.url, newest_urls[year]), headers=headers, cookies=cookies)

            # open downloaded zipfile
            with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zf:
                # get names of all files in zip
                filenames = zf.namelist()
                
                # get region, to which file is related
                for filename in filenames:
                    region = self.__get_region_filename(filename)

                    # skip the file, if it is not related to any region
                    if(region == None):
                        continue

                    region_folder = os.path.join(self.folder, region)

                    # create folder for the region
                    try:
                        os.makedirs(region_folder)
                    except OSError as e:
                        if(e.errno != errno.EEXIST):
                            raise
                    
                    # load data from the file related to region and store them in the file named by year in the region folder
                    file_content = zf.read(filename)                
                    newfile = open(os.path.join(region_folder, f'{year}.csv'), 'wb')
                    newfile.write(file_content)
                    newfile.close()

    @staticmethod
    def __get_region_filename(filename):
        """
        Convert file name to shortcut of region, to which it belongs. 

        Arguments:
            filename - name of file with records for particular region

        Return value:
            str - the region shortcut
        """
        regions = {
            '00' : 'PHA', '01' : 'STC', '02' : 'JHC', '03' : 'PLK', '04' : 'ULK', '05' : 'HKK', '06' : 'JHM',
            '07' : 'MSK', '14' : 'OLK', '15' : 'ZLK', '16' : 'VYS', '17' : 'PAK', '18' : 'LBK', '19' : 'KVK'
        }

        # if the filename without suffix is the key in the regions dictionary, region shortcut is returned, otherwise None value is returned
        return regions.setdefault(filename.split('.')[0], None)

    def parse_region_data(self, region):
        """
        Download data for specified region, if they are not downoladed yet, parse them and get records in specific format.

        Arguments:
            region - shortcut of parsed region

        Return value:
            tuple(list[str], list[np.ndarray]) - couple, where the first item is a list of headers of individual data columns and the second are data columns of specified region
        """
        region_path = os.path.join(self.folder, region)
        
        # if region folder does not exists, try to download data
        if(not os.path.isdir(region_path)):
            self.download_data()

        data_files = os.listdir(region_path)

        # if there are no data files in region folder, try to download data
        if(not data_files):
            self.download_data()
            data_files = os.listdir(region_path)

        # data types for columns in data files
        data_types = [
            'U3', 'i1', 'i1', 'i8', 'i1', 'i4', 'M8[D]','i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i2', 'i2', 'i2', 'i2', 'i4', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 
            'i1', 'i1', 'i1', 'i2', 'i1', 'i1', 'i1', 'i1', 'i2', 'i1', 'i1', 'i1', 'i1', 'i1', 'i1', 'i4', 'i1', 'i1', 'i1', 'f8', 'f8', 'f8', 'f8', 'f8', 'f8', 'U50', 'U50', 'i4', 'U30', 'U10', 
            'i4', 'U30', 'U10', 'U10', 'i4', 'i4', 'U30', 'i1'
        ]

        lines = 0

        # count all rows in all data files related to one region
        for data_file in data_files:
            with open(os.path.join(region_path, data_file), encoding='cp1250') as csvfile:
                lines += sum(1 for _ in csvfile)

        arrays = []

        # fill list with numpy arrays
        for data_type in data_types:
            arrays.append(np.empty(lines, dtype=data_type))

        line = 0

        # load and parse every data file related to the region
        for data_file in data_files:
            with open(os.path.join(region_path, data_file), encoding='cp1250') as csvfile:
                reader = csv.reader(csvfile, delimiter=';')

                # check data for every line and stored them into numpy arrays
                for row in reader:
                    arrays[0][line] = region
                    time = row.pop(5)

                    hour = time[:2]
                    minute = time[2:]

                    # invalid value for hours
                    if(hour == '25'):
                        hour = -1

                    # invalid value for minutes
                    if(minute == '60'):
                        minute = -1

                    arrays[1][line] = hour
                    arrays[2][line] = minute

                    for col, item in enumerate(row, start=3):
                        if(item in ['', 'XX']):
                            item = -1
                        else:
                            item = item.replace(',', '.')

                        try:
                            arrays[col][line] = item
                        except:
                            arrays[col][line] = -1
                    
                    line += 1

        return (self.headers, arrays)

    def get_list(self, regions=None):
        """
        Get records of specified regions and create cache files, where the processed data of individual regions are stored, if the region has not been processed yet.

        Arguments:
            regions - list of region shortcuts (defaults None - means that all regions are processed)

        Return value:
            tuple(list[str], list[np.ndarray]) - couple, where the first item is a list of headers individual data columns and the second are data columns specified regions
        """
        all_regions = [i for i in self.cache_data]

        if(regions == None):
            regions = all_regions
        else:
            # get uniq regions and remove unknown shortcuts
            regions = list(set(regions) & set(all_regions))

        for region in regions:
            # if data are not in memory cache, load them from the cache file, if it exists, otherwise parse them and create cache file with processed data 
            if(self.cache_data[region] == None):
                cache_file_path = os.path.join(self.folder, self.cache_filename.format(region))

                if(os.path.isfile(cache_file_path)):
                    with gzip.open(cache_file_path, 'rb') as cache_file:
                        self.cache_data[region] = pickle.load(cache_file)
                else:
                    data = self.parse_region_data(region)

                    with gzip.open(cache_file_path, 'wb') as cache_file:
                        pickle.dump(data, cache_file)

                    self.cache_data[region] = data

        # if only one region is required, there is no need for concatenation
        if(len(regions) == 1):
            return self.cache_data[regions[0]]

        arrays = []
        length = len(self.headers)

        # for every column concatenate data from all required regions
        for i in range(length):
            concat_arrays = []

            for region in regions:
                concat_arrays.append(self.cache_data[region][1][i])

            arrays.append(np.concatenate(concat_arrays))

        return (self.headers, arrays)


if(__name__ == "__main__"):
    # if the script is run as main, get data for 3 regions and print basic informations about them
    data = DataDownloader()
    headers, records = data.get_list(['JHM', 'JHC', 'MSK'])
    record_num = records[0].shape[0]
    regions = np.unique(records[0])

    decode_region = {
            'PHA': 'Hlavni mesto Praha', 'STC': 'Stredocesky', 'JHC': 'Jihocesky', 'PLK': 'Plzensky', 'ULK': 'Ustecky', 'HKK': 'Kralovehradecky', 'JHM': 'Jihomoravsky', 
            'MSK': 'Moravskoslezky', 'OLK': 'Olomoucky', 'ZLK': 'Zlinsky', 'VYS': 'Kraj Vysocina', 'PAK': 'Pardubicky', 'LBK': 'Liberecky', 'KVK': 'Karlovarsky'
        }

    print('')
    print('Columns:')

    for colnum, colname in enumerate(headers, start=1):
        print(f'    {colnum}) {colname}')

    print('')
    print(f'Number of records: {record_num}')
    print('')
    print(f'Regions:')
    
    for region in regions:
        print(f'    {region} - {decode_region[region]}')

    print('')