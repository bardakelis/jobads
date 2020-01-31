#!/usr/bin/env python3
import requests, logging
#import base64
#import io
from io import BytesIO
from bs4 import BeautifulSoup
# packages needed for image to text conversions:
from PIL import Image
import sys
import pyocr
import pyocr.builders
import os
# for removing empty lines from string:
from os import linesep, walk
# selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

#for text cleanup:
import re
#for MongoDB:
import pymongo
import dns # required for connecting with SRV
import datetime

# for querying MongoDB:
from bson.objectid import ObjectId
# for reading credentials from separate file:
import yaml

# for wordcloud:
from wordcloud import (WordCloud, get_single_color_func)
import matplotlib.pyplot as plt
# Import classes from separate file
from keywordcloud import SimpleGroupedColorFunc, GroupedColorFunc


####################### init pyocr tools: ##################################
tools = pyocr.get_available_tools()
if len(tools) == 0:
    print("No OCR tool found")
    sys.exit(1)
# The tools are returned in the recommended order of usage
tool = tools[0]
print("Will use tool '%s'" % (tool.get_name()))
# Ex: Will use tool 'libtesseract'

langs = tool.get_available_languages()
print("Available languages: %s" % ", ".join(langs))
lang = langs[0]
print("Will use lang '%s'" % (lang))
# Ex: Will use lang 'fra'
# Note that languages are NOT sorted in any way. Please refer
# to the system locale settings for the default language
# to use.
##############################################################################

######################### Define logging format: #####################################
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -%(levelname)s - %(message)s')
logging.basicConfig(level=logging.DEBUG, filename='ads.txt', filemode='w', format='%(asctime)s -%(levelname)s - %(message)s')
######################################################################################

######### Check if ad text extracted can be considered as valid ###############
def ad_extraction_ok(ad_text):
    # If ad text is longer than "min_ad_length", assume it is OK:
    min_ad_length = 100
    if len(ad_text) > min_ad_length:
        return True
    else:
        return False
##############################################################################
# MongoDB config:
#
# Get username/password from external file:
conf = yaml.load(open('credentials.yml'))
username = conf['user']['username']
password = conf['user']['password']
client = pymongo.MongoClient("mongodb+srv://"+username+":"+password+"@cluster0-znit8.mongodb.net/test?retryWrites=true&w=majority")
# Using DB "mydb"
db = client.bigdb
# Using collection "job_ads"
ads = db.job_ads
################# Check if job ad already in collection ##############################
def already_in_db(obj_id):
    found = db.job_ads.find_one({'_id': obj_id})
    if found is None:
        # Not in DB
        return False
    else:
        # Is in DB
        return True
######################################################################################
########################### Selenium browser  ########################################
def selenium_browser(url):
    options = Options()
    options.headless = True
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36')
    browser = webdriver.Chrome("/usr/local/bin/chromedriver", chrome_options=options)
  
    try:
        browser.set_page_load_timeout(30)
        browser.get(url)
    except TimeoutException as ex:
        logging.error("A TimeOut Exception has been thrown: " + str(ex))
        browser.quit()

    # Wait for iframe with id=JobAdFrame to load and switch to it:
    try:
        WebDriverWait(browser, 10).until(EC.frame_to_be_available_and_switch_to_it("JobAdFrame"))
    except TimeoutException:
        logging.warning("Selenium did not find a matching iFrame JobAdFrame. Will continue further...")
    
    page_html = browser.page_source
    # Stop web driver and cleanup:
    browser.quit()

    soup = BeautifulSoup(page_html, 'html.parser')
    ########################################################
    # Cleanup output:
    # remove <script> tags from results
    js_junk = soup.find_all('script')
    for match in js_junk:
        match.decompose()
    # remove <style> tags from results
    css_junk = soup.find_all('style')
    for match in css_junk:
        match.decompose()
    job_ad_frame_page = soup.find('body')
    job_ad_text = job_ad_frame_page.get_text()
    return job_ad_text
########################### End of selenium browser function ##################
########################### Sort a dictionary items by value in descending order: ######################
def sort_dictionary_by_values_desc(unsorted_dict):
    #
    #sorted_keyword_stats = {}
        # Sort dictionary from top keywords to lowest number:
        #for k in sorted(keyword_stats, key=keyword_stats.get, reverse=True):
        #    sorted_keyword_stats[k] = keyword_stats[k]
    #return sorted_keyword_stats
    #
    sorted_dict = {}
    # Sort dictionary from top keywords to lowest number:
    for k in sorted(unsorted_dict, key=unsorted_dict.get, reverse=True):
        sorted_dict[k] = unsorted_dict[k]
    return sorted_dict
########################### Sorting completed ##########################################################
########################### Count technology keywords from DB: ##################
def count_keywords_from_db(file_with_keywords):
    with open(file_with_keywords) as file:
        # Create emtpy dictionary to store stats:
        keyword_stats = {}
        categories = file.readlines()
        for keyword in categories:
            # if keyword is not an empty line 
            if keyword.strip():
                # Compose keyword and wrap around with quotes for exact match in MongoDB
                technology = '"""'+ keyword.rstrip()+'"""'
                # Send a query to MongoDB:
                matched_count = ads.find({"$text": {"$search": technology }}).count()
                keyword_stats[keyword.rstrip()] = matched_count
        sorted_keywords_dict = sort_dictionary_by_values_desc(keyword_stats)
        #sorted_keyword_stats = {}
        # Sort dictionary from top keywords to lowest number:
        #for k in sorted(keyword_stats, key=keyword_stats.get, reverse=True):
        #    sorted_keyword_stats[k] = keyword_stats[k]
    #return sorted_keyword_stats
    return sorted_keywords_dict

###################### End of count technology keywords from DB ##################

########### Define main crawler function - all crawling happens here: #################
def job_ads_crawler(url_to_crawl):

    res = requests.get(url_to_crawl, headers=user_agent)
    ads_inserted_total = 0
    
    whole_page = BeautifulSoup(res.text, 'html.parser')
    offer = whole_page.select('div.offer_primary')
    count_of_offers_in_page = len(offer)
    # looping through the list of jobs shown in a current page (subsequent pages need further code):
    for x in range (count_of_offers_in_page):
        print(x+1,'/',count_of_offers_in_page)
        brief_offer = BeautifulSoup(str(offer[x]),'html.parser')
        # fetching position name
        job_ad_position_name = brief_offer.find('a').text   
        # fetching company name
        company_name = brief_offer.find(itemprop='name').get_text()    
        job_location = brief_offer.find(itemprop='jobLocation').get_text()   
        # fetching salary range string, which needs further parsing to extract numbers
        salary_range = brief_offer.find('span').text    
        # Extract minimum salary if string "Nuo" exists:
        if salary_range.find('Nuo ') != -1:
            salary_from = salary_range.split("Nuo ",1)[1].split(' ')[0]
        else:
            salary_from = ''
        # Extract maximum salary if string "iki" exists
        if salary_range.find('iki ') != -1:
            salary_to = salary_range.split("iki ",1)[1].split(' ')[0]
        else:
            salary_to = ''
        # Extract currency used if string "atlygis" exists (to avoid case when no salary info provided at all)
        if salary_range.find('atlygis') != '':
            salary_currency = salary_range.split(" ")[-1]
        else:
            salary_currency = 'N/A'
        # Check if pay is monthly/hourly etc:
        if 'Mėnesinis' in salary_range:
            pay_interval = 'monthly'
        elif 'Valandinis' in salary_range:
            pay_interval = 'hourly'
        else: 
            pay_interval = 'unknown'
        # Check if pay is bruto or netto:
        if 'bruto' in salary_range:
            salary_amount_type = 'gross'
        elif 'neto' in salary_range:
            salary_amount_type = 'net'
        else:
            salary_amount_type = 'unknown'
        # fetch date when ad was posted, it is inside attribute's "content" value span itemprop="datePosted"
        # e.g. <span itemprop="datePosted" content="2019-09-23">Prieš 19 val.</span>
        date_posted = brief_offer.find('span', {'itemprop':'datePosted'})['content']
        # Search for li element inside ul containing text "Prašymus siųskite iki"
        for item in brief_offer.find('ul', class_='cvo_module_offer_meta offer_dates').find_all('li'):
            if "Prašymus siųskite iki" in item.text: 
                # Extract timestamp from string such as "Prasymus siuskite iki 2019.11.30" and then replace dots with dashes to match job post date format:
                valid_till = item.text.split()[-1].replace('.','-')
            else:
                valid_till = ''
        # fetching href uri location for the full job ad
        job_ad_href = brief_offer.find('a').get('href')
        # constructing a valid url for later retrieval of its contents
        job_ad_url = 'https:'+job_ad_href
        # Check if this ad is already in DB, if so, skip extracting data from it and move to the next one:
        if already_in_db(job_ad_url):
            logging.warning('This ad already in the DB. Will be skipped, URL: %s', job_ad_url)
            print('Ad already in DB, skipping...')
            continue

        logging.info('--------------------------------------------------------------')
        logging.debug('Position: %s', job_ad_position_name)
        logging.debug('Company name: %s', company_name)
        logging.debug('Job location: %s', job_location)
        logging.debug('Salary from: %s', salary_from)
        logging.debug('Salary to: %s', salary_to)
        logging.debug('Salary currency: %s', salary_currency)
        logging.debug('Pay interval: %s', pay_interval)
        logging.debug('Salary amount type: %s', salary_amount_type)
        logging.debug('Job URL: %s', job_ad_url)
        logging.debug('Job post date: %s', date_posted)
        logging.debug('Offer valid till: %s', valid_till)    

        # Crawler is pretending to be Chrome browser on Windows:
        
        # Crawler is pretending to be Chrome browser on Windows:
        job_ad_page_content = requests.get(job_ad_url, headers=user_agent)
    
        ##################### Start reading the ad page and extract its contents if it is in page-main-content div, i.e.
        ##################### plain text, "non-js", "non-iframe", "non-image" ad #############
        # parse detailed job ad text
        job_ad_html = BeautifulSoup(job_ad_page_content.text, 'html.parser')
        # Assuming that a standard cvonline.lt page formatting is used with page-main-content div (otherwise detailed ad text won't be available for extraction)
        job_ad_details = job_ad_html.select('div#page-main-content') 
        extracted_job_ad_text = BeautifulSoup(str(job_ad_details), 'html.parser').get_text()
        extractor = 'BS4:div#page-main-content'
        ########## End of plain text, "non-js", "non-iframe", "non-image" ad extraction ######
        # At this point we have extracted text from the ad image, unless there was an embedded image or iframe.
        # Since we are not sure if we got all we needed, we will check for embedded job ad images with id=JobAdImage and extract text from them if they exist:
        # If we find iFrame with id "JobAdFrame",then we extract ad text from it as iframe must be in the page for a reason:
        #
        # ************** AD AS IFRAME *******************************************************
        # Check if iframe with ID JobAdFrame exists in the page:
        job_ad_frame_tag = job_ad_html.find('iframe', {'id':'JobAdFrame'})
        # If iframe exists, a url address needs to be obtained from it:
        if job_ad_frame_tag is not None:
            # combine domain name with url path to get full URL:
            job_ad_frame_link = root_url + job_ad_frame_tag['src']
            # retrieve the image contents from the link:
            job_ad_frame = requests.get(job_ad_frame_link)
            #print('Job ad frame contains:',str(job_ad_frame))
            job_ad_from_frame = BeautifulSoup(job_ad_frame.text, 'html.parser')
            # remove <script> tags from results
            js_junk = job_ad_from_frame.find_all('script')
            for match in js_junk:
                match.decompose()
            # remove <style> tags from results
            css_junk = job_ad_from_frame.find_all('style')
            for match in css_junk:
                match.decompose()
            job_ad_frame_page = job_ad_from_frame.find('body')
            # do some checks to avoid errors when extracted_job_ad_text is NoneType, i.e. job ad empty as this one: 
            # https://www.cvonline.lt/darbo-skelbimas/alisa-management-laboratory-uab/java-programuotojas-a-f4068182.html
            #Exception has occurred: AttributeError
            #'NoneType' object has no attribute 'get_text'
            try:
                extracted_job_ad_text = job_ad_frame_page.get_text()
            except AttributeError:
                logging.error('This ad is empty, sorry!')
                extracted_job_ad_text = 'Sorry - empty!'
            
            extractor = 'BS4:iFrame'
        # ************** END OF AD AS IFRAME ************************************************
                        
        # Check if we have enough content to assume we retrieved a full ad, if not, fall back to Selenium which can deal with iFrame and JS:
        if ad_extraction_ok(extracted_job_ad_text) is False:
            logging.warning("Extracted text is too short: %s bytes. Engaging Selenium...", len(extracted_job_ad_text))           
            print('Selenium to look at URL: ', job_ad_url)

            extracted_job_ad_text = selenium_browser(job_ad_url)
            extractor = 'Selenium'

        # At this point we have extracted text from an URL embedded into iframe also from  if it existed also if there was any text-based ad.

        # If there was no iframe, we will check for embedded job ad images with id=JobAdImage 
        # and extract text from them if they exist by leveraging OCR:

        # ************** AD AS AN IMAGE *******************************************************
        # Check if there is any image with ID=JobAdImage which means that job ad is embedded as a picture.
        job_ad_image_tag = job_ad_html.find('img', {'id':'JobAdImage'})
        # If job ad image exists, it has to be retrieved to do OCR:
        if job_ad_image_tag is not None:
            # combine domain name with url path to get full URL:
            job_ad_img_link = root_url + job_ad_image_tag['src']
            # retrieve the image contents from the link:
            job_ad_image = requests.get(job_ad_img_link).content
            # save retrieved image bytes into a RAM buffer:
            image_in_buffer = BytesIO(job_ad_image)
            # Identifying what OCR language to use depending on the text string found in the page:
            if  'Job ad without a frame' in extracted_job_ad_text:
                lang = 'eng'
            elif 'Darbo skelbimas be rėmelio' in extracted_job_ad_text:
                lang = 'lit'
            # Use pyocr library that facilitates communication with tesseract library and convert image to text:
            # https://gitlab.gnome.org/World/OpenPaperwork/pyocr
            # selecing appropriate language for OCR by looking at expected text string in 2 langages (LT and EN):
            
            extracted_job_ad_text = tool.image_to_string(
                Image.open(image_in_buffer),
                lang=lang,
                builder=pyocr.builders.TextBuilder()
            )
            #extracted_job_ad_text = 'Extracted by OCR, language: '+lang+'\n'+extracted_job_ad_text
            extractor = f'BS4:OCR({str(lang)})'
        # ************** END OF AD AS AN IMAGE SECTION *********************************************

        # Printing results obtained from page crawling by direct content crawl, iframe link or embedded image:
        extracted_job_ad_text = linesep.join([s for s in extracted_job_ad_text.splitlines() if s])
        ad_with_spaces_removed = re.sub(' +', ' ', extracted_job_ad_text)
        extracted_job_ad_text = ad_with_spaces_removed

        if ad_extraction_ok(extracted_job_ad_text) is False:
            logging.warn('URL: %s | Ad length too short (%d bytes) | Extractor is: %s', job_ad_url, len(extracted_job_ad_text), extractor)
        else:
            logging.info('URL: %s | Ad length OK (%d bytes) | Extractor is: %s', job_ad_url, len(extracted_job_ad_text), extractor)
                    
        logging.debug('Job ad text: %s', repr(extracted_job_ad_text))

        #################################### Writing extracted data to database: ###################
        if salary_from != '':
            salary_from = int(float(salary_from))
        if salary_to != '':
            salary_to = int(float(salary_to))

        collected_info = {"_id": job_ad_url,
            "job_ad_url": job_ad_url,
            "position": job_ad_position_name,
            "company_name": company_name,
            "job_location": job_location,
            "salary_range": salary_range,
            "salary_from": salary_from,
            "salary_to": salary_to,
            "salary_currency": salary_currency,
            "pay_interval": pay_interval,
            "salary_amount_type": salary_amount_type,
            "job_post_date": datetime.datetime.strptime(date_posted, '%Y-%m-%d'),
            "offer_valid_till": datetime.datetime.strptime(valid_till, '%Y-%m-%d'),
            "ad_text": extracted_job_ad_text,
            "extracted_by": extractor,
            "inserted_at": datetime.datetime.utcnow()
            }
        ads = db.job_ads
        try:
            result = ads.insert_one(collected_info)
            if result.acknowledged is True:
                ads_inserted_total += 1
                logging.debug('Ad added into MongoDB. ID: %s', job_ad_url)
            else:
                logging.error('Failed to add an ad into MongoDB! ID: %s', job_ad_url)
        except pymongo.errors.DuplicateKeyError:
            print('This ad already in DB, skipping: ', job_ad_url)    
            logging.warning('This ad already in the DB, URL: %s', job_ad_url)
        #################################### End of writing to database ###############################

    # Check if there are any further ads in the next page, or it is just a single page of results: 
    next_page_tag = whole_page.find('li', class_='page_next')
    # If there is no tag with class page_next (NoneType returned), this means that result fits on a single page:
    if next_page_tag is None:
        more_pages = 0
    else:
        next_page_text = next_page_tag.text
        # If we see a button with text "Toliau*" (next), then it's a multi-page output and crawler needs to get to the next page:
        if 'Toliau' in next_page_text:
            print('Seeing more pages, will continue crawling on the next one...')
            logging.debug('Seeing more pages, will continue crawling on the next one...')
            # Set indicator to 1 if there's yet another page with results (a "Next" button):
            more_pages = 1
        else:
            # Set zero if there's no "Next" button on the page:
            more_pages = 0
    # prepare a tupe to be returned from the function:
    feedback = (more_pages, count_of_offers_in_page, ads_inserted_total)
    return feedback
########################### End of main crawler function ############################

########################### Sanitize non-nested dictionary-keys for MongoDB ############################
# Function replaces "." characters found in keys with double undescore"__" so that MongoDB/PyMongo does not complain
def dots_to_underscore_in_keys(dict):
    for key in dict:
            if "." in key:
                new_key = key.replace('.','__')
                dict[new_key]=dict[key]
                del dict[key]

########################### End of sanitize non-nested dictionary-keys for MongoDB #####################
########################### Convert nested BSON from MongoDB to nested dict: ############################
# Function removes _id as an item and replaces __ to . in key names 
# because the opposite was done upon inserting data into MongoDB.
def nested_bson_2_nested_dict(bson_from_mongo):
    # remove _id from bson because it is no longer needed in dictionary
    #print('bson from mongo:--------------------------------------')
    #print(bson_from_mongo)
    del bson_from_mongo['_id']
    # convert "__" back to "." (as MongoDB id not like dots in key names hence dots were replaced with double undersconre when writing to DB:
    for tech_grp, nested_dict in bson_from_mongo.items():   
        # now go through nested dict values and replace double underscore with a dot as originally was intended (MongoDB restriction to store keys containing dots): 
        for key in nested_dict:
            if "__" in key:
                new_key = key.replace('__','.')
                nested_dict[new_key] = nested_dict[key]
                del nested_dict[key]
        # sort nested dictionary by count so that biggest count gets higher position in the dict:
        nested_dict = sort_dictionary_by_values_desc(nested_dict)
        
        bson_from_mongo[tech_grp] = nested_dict
    return bson_from_mongo
########################### End of convert nested BSON from MongoDB to nested dict#######################
########################### Produce a keyword cloud ##########################################################
def produce_keyword_cloud(keyword_dict):
    #dict = {'Linux': 109, 'Docker': 106, 'Windows': 66, 'AWS': 62, 'Kubernetes': 54, 'iOS': 48, 'Android': 43, 'Azure': 42, 'Terraform': 14, 'S3': 9, 'Google Cloud': 9, 'Microsoft Azure': 8, 'EC2': 7, 'Amazon Web Services': 6, 'MacOS': 5, 'Raspberry Pi': 4, 'Google Cloud Platform': 2, 'CloudFormation': 1, 'Slack': 1, 'WordPress': 1, 'Heroku': 1, 'IBM Cloud': 1, 'Oracle': 65, 'MySQL': 54, 'PostgreSQL': 31, 'Redis': 22, 'Elasticsearch': 21, 'MongoDB': 20, 'Microsoft SQL Server': 10, 'Cassandra': 9, 'MariaDB': 6, 'Firebase': 4, 'Java': 148, 'JavaScript': 128, 'PHP': 104, 'Python': 95, 'CSS': 74, 'HTML': 71, 'C#': 65, 'Go': 43, 'C++': 31, 'Bash': 24, 'PowerShell': 21, 'TypeScript': 21, 'Scala': 18, 'Ruby': 16, 'Swift': 13, 'Kotlin': 9, 'VBA': 7, 'Shell': 6, 'Objective-C': 3, 'Assembly': 2, 'Rust': 2, 'Clojure': 1, 'Spring': 61, 'Angular': 37, 'Laravel': 32, 'jQuery': 23, 'ASP.NET': 12, 'React.js': 10, 'Vue.js': 10, 'Drupal': 9, 'Express': 8, 'Django': 2, '.NET': 76, 'Ansible': 26, 'Node.js': 19, 'Hadoop': 18, 'Puppet': 17, 'Chef': 17, 'React Native': 16, '.NET Core': 10, 'Cordova': 1, 'Xamarin': 1}
    # Since the text is small collocations are turned off and text is lower-cased
    wc = WordCloud(min_font_size=14, max_font_size=100, background_color='white',width=800, height=400, mode='RGB').generate_from_frequencies(keyword_dict)

    platforms = {'Linux': 111, 'Docker': 107, 'Windows': 68, 'AWS': 62, 'Kubernetes': 54, 'iOS': 49, 'Android': 43, 'Azure': 42, 'Terraform': 14, 'S3': 9, 'Google Cloud': 9, 'Microsoft Azure': 8, 'EC2': 7, 'Amazon Web Services': 6, 'MacOS': 5, 'Raspberry Pi': 4, 'Google Cloud Platform': 2, 'CloudFormation': 1, 'Slack': 1, 'WordPress': 1, 'Heroku': 1, 'IBM Cloud': 1}
    databases = {'Oracle': 65, 'MySQL': 57, 'PostgreSQL': 31, 'Redis': 22, 'Elasticsearch': 21, 'MongoDB': 20, 'Microsoft SQL Server': 11, 'Cassandra': 9, 'MariaDB': 6, 'Firebase': 4}
    languages = {'Java': 152, 'JavaScript': 131, 'PHP': 106, 'Python': 98, 'CSS': 75, 'HTML': 72, 'C#': 65, 'Go': 43, 'C++': 33, 'Bash': 24, 'PowerShell': 21, 'TypeScript': 21, 'Scala': 20, 'Ruby': 16, 'Swift': 14, 'Kotlin': 9, 'VBA': 7, 'Shell': 6, 'Objective-C': 3, 'Assembly': 2, 'Rust': 2, 'Clojure': 1}
    frameworks = {'Spring': 63, 'Angular': 37, 'Laravel': 33, 'jQuery': 24, 'React.js': 12, 'ASP.NET': 12, 'Vue.js': 10, 'Drupal': 10, 'Express': 8, 'Django': 2}
    other = {'.NET': 76, 'Ansible': 26, 'Node.js': 19, 'Hadoop': 19, 'Puppet': 17, 'Chef': 17, 'React Native': 16, '.NET Core': 10, 'Cordova': 1, 'Xamarin': 1}

    color2words = {
        'red': list(platforms.keys()),
        'blue': list(databases.keys()),
        'green': list(languages.keys()),
        'orange': list(frameworks.keys()),
        'black': list(other.keys())
    }
    #print(color2words)
    # Words that are not in any of the color_to_words values
    # will be colored with a grey single color function
    default_color = 'grey'
    # Create a color function with single tone
    # grouped_color_func = SimpleGroupedColorFunc(color_to_words, default_color)
    grouped_color_func = SimpleGroupedColorFunc(color2words, default_color)

    # Apply our color function
    wc.recolor(color_func=grouped_color_func)

    # Plot
    plt.figure()
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.show()
########################### End fo keyword cloud production ##################################################
    
######################### Main code goes here: #################################
root_url = 'https://www.cvonline.lt'
# Crawler is pretending to be Chrome browser on Windows:
user_agent = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}

# Options in the site:
    
# timespan:
# All time: ""
# 1 day: "1d"
# 3 days: "3d"
# 7 days: "7d"
# 14 days: "14d"
# 28 days: "28d"
timespan = '1d'

# job_area
# IT: "informacines-technologijos"
job_area = 'informacines-technologijos'

# region:
# Vilnius: vilniaus
#region = 'vilniaus'


# "crawling_ongoing" variable set to 1 to indicate that crawler is looping through pages 
# If multiple pages of job ads are returned, this value is set to 1 and only if 
# last page of multiple pages is returned (or there was a single page in total)
# it is set to 0 to exit crawling loop:
crawling_ongoing = 1
#crawling_ongoing = 0
# page_no is page number to request 1st and subsequent pages of job ads in the web site
page_no = 0
# initializing total ad counter:
ads_in_current_page = 0
ads_total = 0
ads_inserted = 0
while crawling_ongoing == 1:
 #   url = f"{root_url}/darbo-skelbimai/{timespan}/{job_area}/{region}?page={page_no}"
    url = f"{root_url}/darbo-skelbimai/{timespan}/{job_area}?page={page_no}"
    feedback_from_crawler = job_ads_crawler(url)
    # If 1, crawling will go to the next page of results:
    crawling_ongoing = feedback_from_crawler[0]
    # Number of ads processed in previously crawled page:
    ads_in_current_page = feedback_from_crawler[1]
    ads_total += ads_in_current_page
    ads_inserted += feedback_from_crawler[2]
    page_no += 1
#
#
logging.info('Number of ad pages: %d, number of ads: %s', page_no, str(ads_total))
logging.info('Number of ads inserted: %s', str(ads_inserted))

# Return something from DB:
#something = ads.find({ $and: [{ad_text: /Docker/}, {ad_text: /Kubernetes/}, {ad_text: /AWS/}, {ad_text: /Linux/}]}, {position:1, salary_from:1, salary_to:1} )
#the_query = {'ad_text': {'$regex' : 'MongoDB', '$options' : 'i'}}
#the_query = {'$and': [ {'ad_text': {'$regex' : 'MongoDB', '$options' : 'i'}}, {'ad_text': {'$regex' : 'Docker', '$options' : 'i'}}, {'ad_text': {'$regex' : 'linux', '$options' : 'i'}} ] }
#the_query = {'$and': [ {'ad_text': {'$regex' : 'MongoDB', '$options' : 'i'}}, {'ad_text': {'$regex' : 'Docker', '$options' : 'i'}}, {'ad_text': {'$regex' : 'linux', '$options' : 'i'}} ] }
#the_projection = {'position':1, 'salary_from':1, 'salary_to':1}
#print('Query:', the_query)
#something = ads.find(the_query+','+the_projection) 
#something = ads.find({'$and': [ {'ad_text': {'$regex' : 'MongoDB', '$options' : 'i'}}, {'ad_text': {'$regex' : 'Docker', '$options' : 'i'}}, {'ad_text': {'$regex' : 'linux', '$options' : 'i'}} ] }, {'position':1, 'salary_from':1, 'salary_to':1, '_id':0})


# Print dictionary of keywords and number of matched documents, take keyword group name from file name:
basepath = 'categories/'
# this is the nested dictionary where we will be storing tech keyword matches for all technology types (OS, DBs, languages etc.):
container_with_stats = {}
for entry in os.listdir(basepath):
    file_with_path = os.path.join(basepath, entry)
    if os.path.isfile(file_with_path):
        # get technology group name by reading file name and replacing underscore with spaces, so that e.g. "Web_technologies" will be converted into "Web technologies"
        technology = entry.replace('_', ' ')
        # Calculate keyword stats based on keywords listed in particular file.
        # Function searches MongoDB for matching keywords and counts number of documents matched.
        top_tech = count_keywords_from_db(file_with_path)
        
        # replace dot (.) if found in a technology name with and double underscore (__) to satisfy MongoDB requirement not to create key names containing a dot:
        #for key in top_tech:
        #    if "." in key:
        #       print('Value with dot: ', key)
        #        new_key = key.replace('.','__')
        #        top_tech[new_key]=top_tech[key]
        #        del top_tech[key]
        #        print('old key: ', key, 'new key: ', new_key)

        # dot replaced
        dots_to_underscore_in_keys(top_tech)
        #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        #print(top_tech)
        # create a nested dictionary containing all technology groups with nested technology keyword counts to represent most popular keywords:
        container_with_stats[technology] = top_tech

# get todays date:
todays_timestamp = datetime.datetime.today().strftime('%Y-%m-%d')
# add a _id with a value of today's date in format 2020-01-20 to be ready for insertion into MongoDB:
container_with_stats['_id'] = todays_timestamp
# define db and collection as tech_stats:
tech_stats = db.top_tech_stats_daily
# write keyword statistics results back to MongoDB so that we don't have to store it in a dictionary but have a persistent storage instead:
try:
    insert_result = tech_stats.insert_one(container_with_stats)
    if insert_result.acknowledged is True:
        logging.info('Keyword search results have been inserted into DB successfully!')
    else:
        logging.error('Keyword search results insertion failed!')
except pymongo.errors.DuplicateKeyError:
    print('Top technologies for date {} already in db!'.format(todays_timestamp))    
    logging.warning('Top technologies for date %s already in db!', todays_timestamp)
    replace_result = tech_stats.replace_one({'_id': todays_timestamp}, container_with_stats)
    if replace_result.matched_count > 0 :
        logging.info('Existing keyword search results have been updated in DB successfully!')
    else:
        logging.error('Failed to update keyword search results in DB!')


taken_from_db = tech_stats.find_one({'_id': todays_timestamp})

#print("That's what was found in the DB for today: ", taken_from_db)

dictionarized_keyword_stats = nested_bson_2_nested_dict(taken_from_db)

# list dictionary contents:
for key in dictionarized_keyword_stats:
    print('Key :', key)
    print('value: ', dictionarized_keyword_stats[key])
    produce_keyword_cloud(dictionarized_keyword_stats[key])


############################################################################################
# Now we are going to produce some keyword clouds here:
#produce_keyword_cloud(dictionarized_keyword_stats[key])


######################### Main code end #################################