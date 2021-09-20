#!/usr/bin/env python3
from git import Repo
import git
import shutil
import datetime
from datetime import timedelta
import logging
import os
#for text cleanup:
import re
import sys
# for yaml parsing
# need at least versio 5.1 to support no-reordering of dictionary items
# pip3 install PyYAML
import yaml
#import base64
#import io
from io import BytesIO
# for removing empty lines from string:
from os import linesep, walk

import dns  # required for connecting with SRV
import matplotlib.pyplot as plt
import numpy as np
#for MongoDB:
import pymongo
import pyocr
import pyocr.builders
import requests
# for reading credentials from separate file:
import yaml
from bs4 import BeautifulSoup
# for orderedDict:
import collections
# for querying MongoDB:
from bson.objectid import ObjectId
# below 2 needed for watermarked text on image:
# packages needed for image to text conversions:
from PIL import Image, ImageDraw, ImageFont, ImageOps
# selenium
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

# for wordcloud:
from wordcloud import WordCloud, get_single_color_func

# also need "pip install pillow" so that matplotlib can save files as jpg (https://stackoverflow.com/questions/8827016/matplotlib-savefig-in-jpeg-format)
# Import classes from separate file
from keywordcloud import GroupedColorFunc, SimpleGroupedColorFunc
# for parsing json in html:
import json

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
logging.basicConfig(level=logging.DEBUG, filename='logs/application.log', filemode='a', format='%(asctime)s -%(levelname)s - %(message)s')
######################################################################################

######### Check if ad text extracted can be considered as valid ###############
def ad_extraction_ok(ad_text):
    # If ad text is longer than "min_ad_length", assume it is OK:
    min_ad_length = 250
    if len(ad_text) > min_ad_length:
        return True
    else:
        return False
##############################################################################
# MongoDB config:
#
# Get username/password from external file:
conf = yaml.load(open('credentials.yml'), Loader=yaml.FullLoader)
username = conf['user']['username']
password = conf['user']['password']
client = pymongo.MongoClient("mongodb+srv://"+username+":"+password+"@cluster0-znit8.mongodb.net/test?retryWrites=true&w=majority")
# Using DB "mydb"
db = client.bigdb
# Using collection "job_ads"
ads = db.job_ads_test
################# Check if job ad already in collection ##############################
def already_in_db(obj_id):
    found = db.job_ads_test.find_one({'_id': obj_id})
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
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36')
    options.add_argument('--no-sandbox')
    # workaround for Selenium error "unknown error: session deleted because of page crash"
    options.add_argument('--disable-dev-shm-usage')
    browser = webdriver.Chrome("./webdriver/chromedriver", options=options)
  
    try:
        browser.set_page_load_timeout(30)
        browser.get(url)
    except TimeoutException as ex:
        logging.error("A TimeOut Exception has been thrown: " + str(ex))
        browser.quit()

        # Wait for iframe with class=vacancy-content__url to load and switch to it:
    try: # If no timeout and iframe loads:
        WebDriverWait(browser, 10).until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR,".vacancy-content__url")))
    except TimeoutException: # when timeout and selenium did not get anything, log warning and just get contents of the URL requested.
        logging.warning("Selenium did not find a matching iFrame with class vacancy-content__url. Will ignore iFrame and just go to requested URL")
    except Exception as e:
        logging.warning("Webdriver encountered an exception %s", e)
    
    try:
        page_html = browser.page_source
    except Exception as e:
        logging.warning("Webdriver encountered an exception while trying to get page html code %s", e)
        page_html =''
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
    if job_ad_frame_page is not None: # if we have retrieved any html, proceed with extraction to avoid NoneType excpetion in the next step.
        job_ad_text = job_ad_frame_page.get_text(strip=True, separator=' ')
    else:
        job_ad_text = ''
    # Stop web driver and cleanup:
    browser.quit()
    return job_ad_text
########################### End of selenium browser function ##################

########### Define main crawler function - all crawling happens here: #################
def job_ads_crawler(url_to_crawl):
    try:
        res = requests.get(url_to_crawl, headers=user_agent, timeout=request_timeout)
    except requests.exceptions.RequestException as err:
        logging.error('Error: %s', err)
        print('Error: ', err)
        feedback = (0, 0, 0)
        return feedback

    ads_inserted_total = 0
    
    whole_page = BeautifulSoup(res.text, 'html.parser')


    # This code will find script tag with id=__NEXT_DATA__ which in fact contains application/json content type in the html 
    # That json content contains a lot of valuable parameters like position name, salary, offer validity periods etc. This is very helpful as instead
    # of having to extract those number from plain HTML we just can use that json as a dictionary, all we need is to match "id" value in that dictionary
    #  with the same value found in job ad URL.
    
    json_from_webpage = whole_page.find('script', {"id": "__NEXT_DATA__"})
    stringified_json = json.loads(json_from_webpage.string)
    ad_details = stringified_json['props']['initialReduxState']['search']['vacancies']
    print('-------------------ad_details from json:--------------')
    print(ad_details)
    print('-------------------------------------------------')

    offers = whole_page.findAll("li", class_="vacancies-list__item")
    #print(offer)
    #print('*************************************************')
    #print(offer.get_text)

    # Looping through the brief job offers on the page:
    for offer in offers:
        count_of_offers_in_page = len(offers)
        brief_offer = BeautifulSoup(str(offer),'html.parser')
        # fetching href uri location for the full job ad
        job_ad_href = brief_offer.find('a').get('href')
        # constructing a valid url for later retrieval of its contents
        job_ad_url = root_url+job_ad_href
        # Get rid of a span tag with class "hide-mobile" as it contains some junk we want to remove, i.e. double dash "--"
        brief_offer.find('span', class_="hide-mobile").decompose()
        job_location = brief_offer.find('div', class_="vacancy-item__info-main" ).find('span', class_="vacancy-item__locations").text    
        try:
            salary_range = brief_offer.find('div', class_="vacancy-item__info" ).find('span', class_="vacancy-item__salary-label").text
        except:
            salary_range = ''

        # job_ad_href looks like:
        # /lt/vacancy/508920/cv-online-recruitment-lithuania/programuotojas-a-dynamics-ax-dynamics-365-f-ir-o
        # ID is the third field, i.e. 508920 in above example
        job_ad_id = job_ad_href.split("/")[3]
        # A couple of default assumptions here:
        salary_currency = "EUR"
        salary_amount_type = 'gross'
        pay_interval = 'monthly'

        # Retrieve ad details from crawled json code by ID:
        # If no match by ID, then return False:
        ad_as_dict = next((item for item in ad_details if item["id"] == int(job_ad_id)), False)
        
        print('-----------------------ad_as_dict is:-------------------------------------')
        print(ad_as_dict)
        # check that ad_as_dict did not return "False" and important fields are not of None type (positionContent may be empty sometimes though):
        if ad_as_dict is not False and \
           ad_as_dict['positionTitle'] is not None and \
           ad_as_dict['employerName'] is not None and \
           ad_as_dict['publishDate'] is not None and \
           ad_as_dict['expirationDate'] is not None:

            print('ad_as_dict is True!')
            try: # try assigning all values from the JSON, if something fails, fallback to HTML crawling:
                job_ad_id = ad_as_dict['id']
                job_ad_position_name = ad_as_dict['positionTitle']
                company_name = ad_as_dict['employerName']
                salary_from = ad_as_dict['salaryFrom']
                salary_to = ad_as_dict['salaryTo']
                ad_hourly_salary = ad_as_dict['hourlySalary']
                if 'False' in str(ad_hourly_salary):
                    pay_interval = 'monthly' 
                else:
                    pay_interval = 'hourly'
                date_posted = ad_as_dict['publishDate']
                valid_till = ad_as_dict['expirationDate']
                if ad_as_dict['positionContent'] is None:
                    ad_position_content = ''
                else:
                    ad_position_content = ad_as_dict['positionContent']
            except Exception:
                pass  
        else:
            print('no ad_as_dict found, going to collect info from the html')
            # Do our best to extract at least some ad details from html:
            # fetching position name 
            job_ad_position_name = brief_offer.find('span', class_="vacancy-item__title").text  
            print(f'Job ad position name: {job_ad_position_name}')
            # fetching company name 
            company_name = brief_offer.find('div', class_="vacancy-item__info-main" ).find('a').text    
            print(f'company: {company_name}')

            # fetching salary range string, which needs further parsing to extract numbers. Some ads contain no salary, hence "try":
            try:
                salary_range = brief_offer.find('div', class_="vacancy-item__info" ).find('span', class_="vacancy-item__salary-label").text
            except:
                salary_range = ''

            salary_split = salary_range.split(' ')
            
            # if min and max salary is available in html, expected like "€ 2000 – 3000", which makes 4 items:
            if len(salary_split) == 4:
                salary_from = salary_split[1]
                salary_to = salary_split[3]
            # if only one salary is available in html, expected like "€ 2563", which makes 2 items:
            elif len(salary_split) == 2:
                salary_from = salary_split[1]
                salary_to = ''
            else:
                salary_from = salary_to = ''

            # not fetching post date and validity date because dates are not nicely parsable, hence assuming it was from today/now:
            date_posted = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            # Trying to get ad validity date from HTML code:
            try:
                ad_validity = brief_offer.find('div', class_="vacancy-item__info-secondary").select_one('span:contains("Baigiasi")').text.split("Baigiasi: ")[1]
                valid_till = str(datetime.datetime.strptime(ad_validity, '%Y-%m-%d'))

            except:
                # putting a dummy date if extraction did not succeed:
                valid_till = '1970-01-01T12:00:00.000+00:00'

            # ad text not available in brief job description's html, hence empty:
            ad_position_content = ''

        print(f'Position title: {job_ad_position_name}')    
        print(f'Company name: {company_name}')
        print(f'Job location: {job_location}')
        print(f'Salary from: {salary_from}')
        print(f'Salary to: {salary_to}')
        print(f'Salary currency: {salary_currency}')
        print(f'Pay interval: {pay_interval}')
        print(f'Salary amount type: {salary_amount_type}')
        print(f'Job ad URL: {job_ad_url}')
        print(f'Job post date: {date_posted}')
        print(f'Offer valid till: {valid_till}')
        print(f'ID: {job_ad_id}')    
        print(f'Skelbimo tekstas: {ad_position_content}')   

        print('***********************************')

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

        # Check if this ad is already in DB, if so, skip extracting data from it and move to the next one:
        if already_in_db(job_ad_url):
            logging.warning('This ad already in the DB. Will be skipped, URL: %s', job_ad_url)
            print('Ad already in DB, skipping...')
            continue

        if ad_extraction_ok(ad_position_content) is True:
            logging.info("Job description lengtht: %s bytes. Assuming that it is good enough...", len(ad_position_content))   
            print("Assuming we have a good job description text from JSON. Job description length:", len(ad_position_content))
            extracted_job_ad_text = ad_position_content
            extractor = 'JSON_DATA'      
        else:  
            print('Clicking on link to read the ad content:', job_ad_url)
        
            # Crawler is pretending to be Chrome browser on Windows:
            try:
                job_ad_page_content = requests.get(job_ad_url, headers=user_agent, timeout=request_timeout)
            
                ##################### Start reading the ad page and extract its contents as ##########
                ##################### plain text, "non-js", "non-iframe", "non-image" ad #############
                # parse detailed job ad text
                job_ad_html = BeautifulSoup(job_ad_page_content.text, 'html.parser')
                # Assuming that a standard cvonline.lt page formatting is used with vacancy-details__section divs (may be more than one in the page, hence using findAll)       
                job_ad_details = job_ad_html.findAll('div', class_="vacancy-details__section")
                # we will concatenate sections of job_ad_details findAll results into "combined_sections" string in case there are more than 1 div with this class:
                # If more than 1 div with class "vancancy-details__section" is found:
                if len(job_ad_details) > 1:
                    combined_sections = ''
                    for section in job_ad_details:
                        combined_sections +=str(section)
                    job_ad_details = combined_sections
                extracted_job_ad_text = BeautifulSoup(str(job_ad_details), 'html.parser').get_text(strip=True, separator=' ')
                extractor = 'BS4:div.vacancy-details__section'
                ########## End of plain text ad extraction ######
            except requests.exceptions.RequestException as err:
                logging.error('Error: %s', err)
                print('Error: ', err)

            # If extracted_job_ad_text is long enough, we're happy, otherwise we'll look for iframe:

            if ad_extraction_ok(extracted_job_ad_text) is True:
                logging.info("Job description lengtht: %s bytes. Assuming that it is good enough...", len(ad_position_content))   
                print("Assuming we have a good job description text from vacancy-details__section. Job description length:", len(extracted_job_ad_text))
            else: # so we are not happy with extracted text length, assuming it contains no valid ad, and will proceed to look for iFrame:
                print(f'Data extracted by BS4:div.vacancy-details__section dont seem to be valid, num of bytes: {len(ad_position_content)}')
            
                # If we find iFrame with title="urlDetails" or "class = vacancy-content__url",then we extract ad text from it as iframe must be in the page for a reason:
                #
                # ************** AD AS IFRAME *******************************************************
                # Check if iframe with class = vacancy-content__url exists in the page:
                # company_name = brief_offer.find('div', class_="vacancy-item__info-main" ).find('a').text  
                job_ad_frame_tag = job_ad_html.find('iframe', class_="vacancy-content__url")
                # If iframe exists, a url address needs to be obtained from it:
                if job_ad_frame_tag is not None:
                    # combine domain name with url path to get full URL:
                    job_ad_frame_link = job_ad_frame_tag['src']
                    print(f'Found external link in iframe as {job_ad_frame_link}')
   
                    # retrieve the image contents from the link:
                    try:
                        job_ad_frame = requests.get(job_ad_frame_link, timeout=request_timeout)
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
                        #Exception has occurred: AttributeError
                        #'NoneType' object has no attribute 'get_text'
                        try:
                            extracted_job_ad_text = job_ad_frame_page.get_text(strip=True, separator=' ')
                        except AttributeError:
                            logging.error('This ad is empty, sorry!')
                            extracted_job_ad_text = 'Sorry - empty!'

                        extractor = 'BS4:iFrame'
                    except requests.exceptions.RequestException as err:
                        logging.debug('Error: %s', err)
                        print('****************Error: ', err)

                    # Check if we have enough content to assume we retrieved a full ad, if not, fall back to Selenium which can deal with iFrame and JS:
                    if ad_extraction_ok(extracted_job_ad_text) is False:
                        logging.warning("Extracted text is too short: %s bytes. Engaging Selenium...", len(extracted_job_ad_text))           
                        print(f'Extracted text is too short {len(extracted_job_ad_text)}. Selenium to look at URL {job_ad_url}')

                        extracted_job_ad_text = selenium_browser(job_ad_url)
                        extractor = 'Selenium4iFrame'
                        
                # ************** END OF AD AS IFRAME ************************************************

            # If extraction via iFrame not succeeded (most likely because it was not found), let's check if there is a <a href="external_url" link as an ad:
            if ad_extraction_ok(extracted_job_ad_text) is False:
                logging.warning("Extracted text is too short: %s bytes, will try to check if there is a simple a href link...", len(extracted_job_ad_text))     

                # At this point we have extracted text from an URL embedded into iframe also from  if it existed also if there was any text-based ad.
                # Now we will check whether ad was implemented as a simple "a href" link with the position name that is supposed to open a new window and load third-party site:

                # ************** AD AS a href LINK OPENING IN A NEW WINDOW *******************************************************
                # Check if there is a div with class "react-tabs__tab-panel--selected" which contains a href similar to:
                # <a href="https://apply.workable.com/euromonitor/j/90197754B1/" rel="noopener noreferrer" target="_blank" class="jsx-1778450779">SENIOR SOFTWARE ENGINEER</a>

                job_ad_href_link_tag = job_ad_html.find('div',class_='react-tabs__tab-panel--selected').find('a',href=True, target='_blank' )
                if job_ad_href_link_tag is not None:
                    external_url_to_crawl = job_ad_href_link_tag['href']
                    print('External URL in href:')
                    print(external_url_to_crawl)
                    extracted_job_ad_text = selenium_browser(external_url_to_crawl)
                    extractor = 'Selenium4href'
                # ************** END OF AD AS a href LINK OPENING IN A NEW WINDOW ************************************************
                
            if ad_extraction_ok(extracted_job_ad_text) is False:
                logging.warning("Extracted text is too short: %s bytes, will try to check if there is an image-based ad...", len(extracted_job_ad_text))     

                # ************** AD AS AN IMAGE *******************************************************
                # We will scan image only if there was no vacancy-details__class div in the page, otherwise image may be just an illustration 
                # with no valid ad text next to text-based job ad.
                text_based_ad = job_ad_html.find('div', class_="vacancy-details__section")
                if text_based_ad is None: # seems there is no ad text in the page, so we will check if there is any image to scan:
                    try: # using try for cases when method falls back to this due to connection errors to site, so image tag method is not valid:
                        job_ad_image_tag = job_ad_html.find('div',class_='react-tabs__tab-panel--selected').find('div',class_='vacancy-details__image' ).find('img')
                    except Exception:
                        job_ad_image_tag = None
                    # If job ad image exists, it has to be retrieved to do OCR:
                    if job_ad_image_tag is not None:
                        # combine domain name with url path to get full URL:
                        job_ad_img_link = root_url + job_ad_image_tag['src']
                        # retrieve the image contents from the link:
                        try:
                            job_ad_image = requests.get(job_ad_img_link, timeout=request_timeout).content
                            # save retrieved image bytes into a RAM buffer:
                            image_in_buffer = BytesIO(job_ad_image)
                            # Identifying what OCR language to use depending on the text string found in the page:
                            if 'Darbo skelbimas be rėmelio' in extracted_job_ad_text:
                                lang = 'lit'
                            elif  'Job ad without a frame' in extracted_job_ad_text:
                                lang = 'eng'
                    # If there is another language, still treat it as english (I saw ads in Russian, in this case string
                            # will look like 'Объявление без рамки', but we won't bother extracting kirilica:
                            else:
                                #lang = 'eng'
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
                        except requests.exceptions.RequestException as err:
                            logging.debug('Error: %s', err)
                            print('Error: ', err)
                # ************** END OF AD AS AN IMAGE SECTION *********************************************

        # Printing results obtained from page crawling by direct content crawl, iframe link or embedded image:
        extracted_job_ad_text = linesep.join([s for s in extracted_job_ad_text.splitlines() if s])
        ad_with_spaces_removed = re.sub(' +', ' ', extracted_job_ad_text)
        extracted_job_ad_text = ad_with_spaces_removed

        if ad_extraction_ok(extracted_job_ad_text) is False:
            logging.warning('URL: %s | Ad length too short (%d bytes) | Extractor is: %s', job_ad_url, len(extracted_job_ad_text), extractor)
        else:
            logging.info('URL: %s | Ad length OK (%d bytes) | Extractor is: %s', job_ad_url, len(extracted_job_ad_text), extractor)
                    
        logging.debug('Job ad text: %s', repr(extracted_job_ad_text))

        #################################### Writing extracted data to database: ###################
        if salary_from != '' and salary_from is not None:
            salary_from = int(float(salary_from))
        else:
            salary_from = ''
        if salary_to != '' and salary_to is not None:
            salary_to = int(float(salary_to))
        else:
            salary_to = ''

       # job_post_date = datetime.datetime.strptime(date_posted, '%Y-%m-%d')
        job_post_date = datetime.datetime.fromisoformat(date_posted)
        offer_valid_till = datetime.datetime.fromisoformat(valid_till)

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
            "job_post_date": job_post_date,
            "offer_valid_till": offer_valid_till,
            "ad_text": extracted_job_ad_text,
            "extracted_by": extractor,
            "inserted_at": datetime.datetime.utcnow()
            }
        
        # If ad text is sufficiently long, we will write it to the database, otherwise will skip it for retrying later as this could be a result of connection error
        if ad_extraction_ok(extracted_job_ad_text) is True:
            
            ads = db.job_ads_test
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
        else:
            # If extracted text was not long enough, we will just ignore the whole ad, perhaps we will be able to crawl it next time if that was due to connection errors.
            logging.warning('This ad has got too short text upon crawling, hence will be ignored and not put into DB for now: %s', job_ad_url)
            print(f'This ad is too short and will not be inserted into DB for now: {job_ad_url}')

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
# using list(dict) instead of "key" here because we want to copy original key list and iterate through it instead of through changing keys as they are created and deleted in the loop
# which can cause key skipping and thus proper dot replacement sometimes:
    for i in list(dict):
            if "." in i:
                new_key = i.replace('.','__')
                dict[new_key]=dict[i]
                del dict[i]
########################### End of sanitize non-nested dictionary-keys for MongoDB #####################
########################### Convert nested BSON from MongoDB to nested dict: ############################
# Function removes _id as an item and replaces __ to . in key names 
# because the opposite was done upon inserting data into MongoDB.

   
######################### Main code goes here: #################################
root_url = 'https://www.cvonline.lt'
# Crawler is pretending to be Chrome browser on Windows:
user_agent = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}

# Options in the site:

# How many ads to crawl:
ad_limit = 500
# what is offset of ads (i.e. how many ads to skip from top)
ad_offset = 0
# set urllib3 request timeout in seconds:
request_timeout = 30

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
    url = f'https://www.cvonline.lt/lt/search?limit={ad_limit}&offset=0&categories%5B0%5D=INFORMATION_TECHNOLOGY'
    feedback_from_crawler = job_ads_crawler(url)
    # If 1, crawling will go to the next page of results:
    crawling_ongoing = feedback_from_crawler[0]
    # Number of ads processed in previously crawled page:
    ads_in_current_page = feedback_from_crawler[1]
    ads_total += ads_in_current_page
    ads_inserted += feedback_from_crawler[2]
    page_no += 1
logging.info('Number of ad pages: %d, number of ads: %s', page_no, str(ads_total))
logging.info('Number of ads inserted: %s', str(ads_inserted))


######################### Main code end #################################
