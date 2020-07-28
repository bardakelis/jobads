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
# for wordcloud:
from wordcloud import WordCloud, get_single_color_func

# also need "pip install pillow" so that matplotlib can save files as jpg (https://stackoverflow.com/questions/8827016/matplotlib-savefig-in-jpeg-format)
# Import classes from separate file
from keywordcloud import GroupedColorFunc, SimpleGroupedColorFunc

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
    min_ad_length = 100
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
    options.add_argument('--no-sandbox')
    browser = webdriver.Chrome("./webdriver/chromedriver", options=options)
  
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
########################### Sort nested dictionary items by value in descending order: ######################
def sort_dictionary_by_values_desc(unsorted_dict):
    #sorted_dict = collections.OrderedDict()
    sorted_dict = {}
    # Sort dictionary from top keywords to lowest number:
    # https://dzone.com/articles/python-201-how-sort-dictionary
    for k in sorted(unsorted_dict.keys(), key=lambda y: (unsorted_dict[y]['adsWithKwd']), reverse=True):
        sorted_dict[k] = unsorted_dict[k]
    return sorted_dict
########################### Sorting completed ##########################################################
########################### Count technology keywords from DB: ##################
def count_keywords_from_db(file_with_keywords):
    # Fetch records not older than 90 days, i.e. approx. 3 months:
    ref_date = datetime.datetime.today() - datetime.timedelta(days=90)
    # Obtain total number of ads in the database, by only looking at date when an ad was posted:
    total_ads_per_period = ads.count_documents({"job_post_date":{"$gt": ref_date} })
    #total_ads_per_period = ads.find({"job_post_date":{"$gt": ref_date} }).estimated_document_count()


    with open(file_with_keywords) as file:
        # Create emtpy dictionary to store stats:
        keyword_stats = {}
        categories = file.readlines()
        # e.g. for 'Linux' in Platforms or for 'C++' in Programming_languages:
        for keyword in categories:
            # if keyword is not an empty line 
            if keyword.strip():
                
                # Strip spaces from the end of the keyword:                
                technology = keyword.rstrip()
                
                # Send a query to MongoDB:
                adsWithKwd = ads.count_documents({"$text": {"$search": f'""\"{technology}\"""' }, "job_post_date":{"$gt": ref_date} })
                # declare temporary storage dictinary for count matches, we will add it to a larger dictionary for each technology individually
                documents_matched = {}
                documents_matched['adsWithKwd'] = adsWithKwd # this is a count of docs with keywords we are looking for
                documents_matched['adsInDBforPeriod'] = total_ads_per_period # this is a count of all docs/ads per the same period, so that it helps calculate percentage if we want later                
                # constucting a query to MongoDB that retrieves all statistics such as salary, keyword count etc:               
                pipeline = [
                    { "$match": 
                        {
                            "$and": 
                            [
                                {"$text" : { "$search": f'""\"{technology}\"""' }  },
                                {"pay_interval" : { "$eq": 'monthly' }},
                                {"salary_amount_type" : { "$eq": 'gross' }},
                                {"job_post_date": {"$gt": ref_date}}
                            ] 
                        }
                    },
                    { 
                        "$group": 
                        {
                            "_id": "null", 
                            "avgSalaryLow": { "$avg": "$salary_from"},
                            "avgSalaryHigh": { "$avg": "$salary_to"}, 
                            "adsWithKwdSalary": {"$sum":1}, 
                            "avgTxtScoreKwdSalary": {"$avg": {"$meta": "textScore"}}
                        },
                    },
                    { 
                        "$project": 
                        { 
                            "_id": 0, 
                            'avgSalaryLow':1,
                            'avgSalaryHigh':1,
                            'adsWithKwdSalary':1,
                            'avgTxtScoreKwdSalary':1
                        }
                    }
                    ]

                
                # lets define a dictionary to hold output from db, later we will merge it with documents_matched dict and produce final keyword_stats dict:
                scores = {}
                # getting a cursor from MongoDB
                cursor = ads.aggregate(pipeline)
                # to get actual data from the cursor we have to iterate thru items in the cursor (one item, that will come out as a dictionary):
                for data in cursor:
                    scores = data
            
                # joining 2 dictionaries into one single one:
                keyword_stats[technology] = {**scores, **documents_matched}

       
        #sorted_keywords_dict = collections.orderedDict()
        sorted_keywords_dict = sort_dictionary_by_values_desc(keyword_stats)
       
    return sorted_keywords_dict

###################### End of count technology keywords from DB ##################

########### Define main crawler function - all crawling happens here: #################
def job_ads_crawler(url_to_crawl):
    try:
        res = requests.get(url_to_crawl, headers=user_agent)
    except requests.exceptions.RequestException as err:
        logging.error('Error: %s', err)
        print('Error: ', err)
        feedback = (0, 0, 0)
        return feedback

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
                try:
                    datetime.datetime.strptime(valid_till, '%Y-%m-%d')
                except ValueError:
                   # raise ValueError("Incorrect valid_till date format, should be YYYY-MM-DD")
                    logging.warning('Incorrect valid-till date format, should be YYYY-MM-DD')
                    valid_till = '2111-11-11'
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
        try:
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
        except requests.exceptions.RequestException as err:
            logging.error('Error: %s', err)
            print('Error: ', err)

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

            try:
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
            except requests.exceptions.RequestException as err:
                logging.debug('Error: %s', err)
                print('Error: ', err)
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
            try:
                job_ad_image = requests.get(job_ad_img_link).content
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
                    lang = 'eng'
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
def nested_bson_2_nested_dict(bson_from_mongo):
    # remove _id from bson because it is no longer needed in dictionary
    #print('bson from mongo:--------------------------------------')
    #print(bson_from_mongo)
    del bson_from_mongo['_id']
    # convert "__" back to "." (as MongoDB id not like dots in key names hence dots were replaced with double undersconre when writing to DB:
    for tech_grp, nested_dict in bson_from_mongo.items():   
        # now go through nested dict values and replace double underscore with a dot as originally was intended (MongoDB restriction to store keys containing dots): 
        # using list(nested_dict) instead of "key" here because we want to copy original key list and iterate through it instead of through changing keys as they are created and deleted in the loop
        # which can cause key skipping and thus proper dot replacement sometimes:

        for i in list(nested_dict):
            if "__" in i:
                new_key = i.replace('__','.')
                nested_dict[new_key] = nested_dict[i]
                del nested_dict[i]
        # sort nested dictionary by count so that biggest count gets higher position in the dict:
        nested_dict = sort_dictionary_by_values_desc(nested_dict)
        bson_from_mongo[tech_grp] = nested_dict
    return bson_from_mongo
########################### End of convert nested BSON from MongoDB to nested dict#######################
################################### Make top list dictionary for usage in a web page ##########################################################################
# 
# Constructing dictionary for use on a web page containing top1, top2, top3 as keys, 
# so that data is better structured by using top1 in a web page instead of e.g. "Java":
# Input something like:
# 'SNMP': {'avgSalaryLow': 2177.5, 'avgSalaryHigh': 2650.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015849811108432, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}, 
# 'Pandas': {'avgSalaryLow': 2200.0, 'avgSalaryHigh': 3300.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5011765292111441, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}, 
# 'Cordova': {'avgSalaryLow': 2064.0, 'avgSalaryHigh': 4375.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015516829792313, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}, 
# 'Xamarin': {'avgSalaryLow': 2314.0, 'avgSalaryHigh': 4070.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015690275413895, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}

# Output something like (tech names don't match above input just because of bad example
# {top1: {'name': 'SNMP','avgSalaryLow': 2177.5, 'avgSalaryHigh': 2650.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015849811108432, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}} 
# {top2: {'name': 'Pandas','avgSalaryLow': 2200.0, 'avgSalaryHigh': 3300.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5011765292111441, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}} 
# {top3: {'name': 'Cordova', 'avgSalaryLow': 2064.0, 'avgSalaryHigh': 4375.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015516829792313, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}} 
# {top4: {'name':'Xamarin', 'avgSalaryLow': 2314.0, 'avgSalaryHigh': 4070.0, 'adsWithKwdSalary': 2, 'avgTxtScoreKwdSalary': 0.5015690275413895, 'adsWithKwd': 2, 'adsInDBforPeriod': 1775}}
def make_top_list_dict(sorted_nested_dict, kwd_group_name, top_size=10):
    kwds_for_web = {}
    kwds_for_web_with_grp_name = {}
    num_pos = 1
    for k, v in sorted_nested_dict.items():
        
        tech_name = {}
        data_for_tech = {}
        tech_name['name'] = k # creating dictionary holding tech name, e.g. {'name':'java'}
        data_for_tech = v # creating dictionary holding actual data , such as {'avgSalaryLow': 2314.0,...}
        merged_dict = dict(**tech_name, **data_for_tech) # merging it to be like {'name':'java', avgSalaryLow': 2314.0,...}
        
        kwds_for_web[f'top{num_pos}'] = {} # creating nested dictionary to hold merged_dict data
        kwds_for_web[f'top{num_pos}'] = merged_dict # this becomes top1, top2...topn as: {top1: {'name':'java', avgSalaryLow': 2314.0,...}}
        num_pos += 1
        if num_pos > top_size:
            break
    
    # This will help group data in a single YAML file as technologies and their stats will be listed 
    # under top1, top2, top"n", so we need somehow distinguish to what section those technologies belong
    # e.g. whether top1 technology belongs to "all_top_keywords" or to a more specific group "Platforms" or "Databases"
    kwds_for_web_with_grp_name[kwd_group_name] = kwds_for_web
    return kwds_for_web_with_grp_name
################################### End of making top list dictionary for usage in a web page ######################################################################

############################ Extract only technology name and count from nested dict ###########################
# CONSTRUCTING DICTIONARIES FOR WORDCLOUD
# constructing dictionary suitable for wordcloud, i.e. "technology:count"
def get_keyword_and_count(nested_dict):
    kwds_with_count = {}
    for k, v in nested_dict.items():
        kwds_with_count[k] = v['adsWithKwd']
    return kwds_with_count
############################ End of extract only technology name and count from nested dict ###################
########################### Produce a keyword cloud ##########################################################
def produce_keyword_cloud(keyword_dict, img_file_to_save, jpg_quality, bigger=False):
    # assign colors to categories:   
    color2words = {
        'magenta': list(buzzwords_kwds.keys()),
        'mediumvioletred': list(databases_kwds.keys()),
        'navy': list(infosec_kwds.keys()),
        'brown': list(networking_kwds.keys()),
        'darkgreen': list(other_frameworks_tools_kwds.keys()),
        'teal': list(platforms_kwds.keys()),
        'dodgerblue': list(programming_scripting_languages_kwds.keys()),
        'limegreen': list(tools_kwds.keys()),
        'firebrick': list(web_frameworks_kwds.keys())
        # more colors here: https://matplotlib.org/2.0.2/_images/named_colors.png
        
    }
    
    default_color = 'grey'
    grouped_color_func = SimpleGroupedColorFunc(color2words, default_color)

    #original was this:
    #wc = WordCloud(font_path='fonts/Inter-Medium.ttf', prefer_horizontal=1,  max_words=300, background_color='white',width=700, height=700, mode='RGB').generate_from_frequencies(keyword_dict)
    
    # for all_kwds we will make wordcloud image of bigger height:
    if bigger == True:
        wc = WordCloud(font_path='fonts/Inter-Medium.ttf', prefer_horizontal=1,  max_words=300, background_color='white',width=700, height=1000, mode='RGB').generate_from_frequencies(keyword_dict)
        plt.figure( figsize=(8,11), facecolor='k')
         # define pixel size of resized image:
        x = 600
        y = 900
    else:
        wc = WordCloud(font_path='fonts/Inter-Medium.ttf', prefer_horizontal=1,  max_words=300, background_color='white',width=700, height=900, mode='RGB').generate_from_frequencies(keyword_dict)
        #wc = WordCloud(font_path='fonts/Inter-Medium.ttf', prefer_horizontal=1,  max_words=300, background_color=None,width=700, height=900, mode='RGBA').generate_from_frequencies(keyword_dict)
        plt.figure( figsize=(8,10), facecolor='k')
       # plt.figure( figsize=(11.11111111,13.88888889), facecolor='k', dpi=72)
        #plt.figure( figsize=(16,20), facecolor='k', dpi=300)
        # define pixel size of resized image:
        x = 600
        y = 800



    # Apply our color function
    wc.recolor(color_func=grouped_color_func)
    

    # Plot
    #plt.figure()
    # resize image plotted: figsize=(8,8)  means 800x800 pixels
    #plt.figure( figsize=(8,8), facecolor='k')

    #plt.figure( figsize=(8,10), facecolor='k')
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    # reducing padding of the image to minimum - more effective use of space:
    plt.tight_layout(pad=2)
    # saving large image:
    plt.savefig(img_file_to_save+'_tmp.png')
    plt.close()

    base_image = Image.open(img_file_to_save+'_tmp.png')
    #base_image.show()
    base_image = base_image.convert('RGB')
    # Performing posterization that reduces number of color bits per RGB channel from 8 down to 5
    # This helps reduce file size. Negative outcome is that pure white color is lost from the background
    base_image_2x = ImageOps.posterize(base_image,5)
    base_image_2x.save(img_file_to_save+'@2x.png', 'png')

    base_image = base_image.resize((x, y), Image.ANTIALIAS)
    # Performing posterization that reduces number of color bits per RGB channel from 8 down to 5
    # This helps reduce file size. Negative outcome is that pure white color is lost from the background
    base_image = ImageOps.posterize(base_image,5)
    base_image.save(img_file_to_save+'.png', 'png')
    
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
timespan = '3d'

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
        #        print('Value with dot: ', key)
        #        new_key = key.replace('.','__')
        #        top_tech[new_key]=top_tech[key]
        #        del top_tech[key]
        #        print('old key: ', key, 'new key: ', new_key)

        # dot replaced
        dots_to_underscore_in_keys(top_tech)
       # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
       # print(top_tech)
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
    #print('Key :', key)
    #print('value: ', dictionarized_keyword_stats[key])
    if key == 'Buzzwords':
        buzzwords_kwds = dictionarized_keyword_stats[key]
    if key == 'Databases':
        databases_kwds = dictionarized_keyword_stats[key]
    if key == 'InfoSec':
        infosec_kwds = dictionarized_keyword_stats[key]
    if key == 'Networking':
        networking_kwds = dictionarized_keyword_stats[key]
    if key == 'Other Frameworks and tools':
        other_frameworks_tools_kwds = dictionarized_keyword_stats[key]
    if key == 'Platforms':
        platforms_kwds = dictionarized_keyword_stats[key]
    if key == 'Programming and Scripting Languages':
        programming_scripting_languages_kwds = dictionarized_keyword_stats[key]
    if key == 'Tools':
        tools_kwds = dictionarized_keyword_stats[key]
    if key == 'Web Frameworks':
        web_frameworks_kwds = dictionarized_keyword_stats[key]

    
    #produce_keyword_cloud(dictionarized_keyword_stats[key])
print('Buzzwords: ', buzzwords_kwds)
print('DBs: ', databases_kwds)
print('InfoSec: ', infosec_kwds)
print('Networking: ', networking_kwds)
print('Other framweworks/tools: ', other_frameworks_tools_kwds)
print('Platforms: ', platforms_kwds)
print('Programming and Scripting Languages: ', programming_scripting_languages_kwds)
print('Tools: ', tools_kwds)
print('Web Frameworks: ', web_frameworks_kwds)


all_kwds = {**databases_kwds, **infosec_kwds, **networking_kwds, **other_frameworks_tools_kwds, **platforms_kwds, 
            **programming_scripting_languages_kwds, **tools_kwds, **web_frameworks_kwds}
# sort keywords by keyword count:
all_kwds = sort_dictionary_by_values_desc(all_kwds)
# to amend:

#get_keyword_and_count(all_kwds)

#dict_for_yaml = make_top_list_dict(all_kwds, 5)
# create dictionary holding today's date:
timestamp = {}
todays_timestamp_with_hours = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
timestamp['date'] = todays_timestamp
timestamp['timestamp'] = todays_timestamp_with_hours

# write YAML file to disk:
# opening for writing, truncating old file if exists:
with open('./output/yaml/toptech.yaml', 'w') as yaml_file:
    yaml.dump(timestamp, yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(all_kwds, 'allTopKwds', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(platforms_kwds, 'Platforms', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(programming_scripting_languages_kwds, 'ProgrammingScriptingLanguages', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(databases_kwds, 'Databases', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(web_frameworks_kwds, 'WebFrameworks', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(other_frameworks_tools_kwds, 'OtherFrameworksTools', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(tools_kwds, 'Tools', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(infosec_kwds, 'InfoSec', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(networking_kwds, 'Networking', 10), yaml_file, default_flow_style=False, sort_keys=False)
    yaml.dump(make_top_list_dict(buzzwords_kwds, 'Buzzwords', 10), yaml_file, default_flow_style=False, sort_keys=False)
    


#print(yaml.dump(dict_for_yaml, default_flow_style=False))
#print('PLATORMS: ')
#print(yaml.dump(make_top_list_dict(platforms_kwds, 20), yaml_file, default_flow_style=False, sort_keys=False))



path_to_kwd_images = './output/keyword_cloud/'

# Generate keyword cloud images for all keyword groups:
# Format: dictionary with keyword:count pairs, path and file name, jpg image quality
produce_keyword_cloud(get_keyword_and_count(all_kwds), path_to_kwd_images+'all_kwds', 95, True)
produce_keyword_cloud(get_keyword_and_count(buzzwords_kwds), path_to_kwd_images+'buzzwords_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(databases_kwds), path_to_kwd_images+'databases_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(infosec_kwds), path_to_kwd_images+'infosec_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(networking_kwds), path_to_kwd_images+'networking_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(other_frameworks_tools_kwds), path_to_kwd_images+'other_frameworks_tools_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(platforms_kwds), path_to_kwd_images+'platforms_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(programming_scripting_languages_kwds), path_to_kwd_images+'programming_scripting_languages_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(tools_kwds), path_to_kwd_images+'tools_kwds', 85)
produce_keyword_cloud(get_keyword_and_count(web_frameworks_kwds), path_to_kwd_images+'web_frameworks_kwds', 85)

############################################################################################
# Now we are going to produce some keyword clouds here:
#produce_keyword_cloud(dictionarized_keyword_stats[key])

############################################################################################
# Let's copy produced images to Hugo's GIT repository. We will first pull it from Github,
# then will add img files (png) and yaml file and then push back into Github. This will 
# ensure that Hugo repo is ready for compiling right away
############################################################################################

# Define hugo GIT directory here:
repo_dir = '/opt/itdarborinka_app/hugo_tmp'

if os.path.isdir(f'{repo_dir}/.git'):
    # .git dir already exists, so just pull HUGO repository locally so that it can be updated with latest img files afterwards:
    repo = Repo(repo_dir)
    origin = repo.remote('origin')
    g = git.cmd.Git(repo_dir)
    pull_result = g.pull()
    print(f'pull result: {pull_result}')
else:
    # .git directory not found, so must be empty then. Let's clone repo:
    Repo.clone_from('git@github.com:bardakelis/hugo.git', repo_dir)

    repo = Repo(repo_dir)
    origin = repo.remote('origin')
    g = git.cmd.Git(repo_dir)

# We'll take files produced by application in this dir:
app_out_dir = '/opt/itdarborinka_app/application/output/keyword_cloud/'
yaml_out_file = '/opt/itdarborinka_app/application/output/yaml/toptech.yaml'
# and copy to hugo images directory to store them there till they are compiled for a new web site version:
hugo_imgs_location = '/opt/itdarborinka_app/hugo_tmp/static/img/keyword_cloud'
hugo_yaml_location = '/opt/itdarborinka_app/hugo_tmp/data/'
# these files are of intered for us:
kwd_cloud_files = [
    'all_kwds@2x.png',
    'all_kwds.png',
    'buzzwords_kwds@2x.png',
    'buzzwords_kwds.png',
    'databases_kwds@2x.png',
    'databases_kwds.png',
    'infosec_kwds@2x.png',
    'infosec_kwds.png',
    'networking_kwds@2x.png',
    'networking_kwds.png',
    'other_frameworks_tools_kwds@2x.png',
    'other_frameworks_tools_kwds.png',
    'platforms_kwds@2x.png',
    'platforms_kwds.png',
    'programming_scripting_languages_kwds@2x.png',
    'programming_scripting_languages_kwds.png',
    'tools_kwds@2x.png',
    'tools_kwds.png',
    'web_frameworks_kwds@2x.png',
    'web_frameworks_kwds.png'
]

# Let's copy img files from app out directory into hugo directory now:
for file_to_copy in kwd_cloud_files:
    kwd_cloud_file_in_output = app_out_dir+file_to_copy
    print(f'file with path: {kwd_cloud_file_in_output}')
    shutil.copy2(kwd_cloud_file_in_output, hugo_imgs_location )
# Let's copy yaml file too:
shutil.copy2(yaml_out_file, hugo_yaml_location )

# prepare list of files that would be git commit-friendly, i.e. with proper file path:
list_of_git_files = []
git_img_path = 'static/img/keyword_cloud/'
git_yaml_file_with_path = 'data/toptech.yaml'

for git_file_to_push in kwd_cloud_files:
    list_of_git_files.append(git_img_path+git_file_to_push)

# We also need to add YAML file toptech.yaml from generated output and place in hugo/data/ directory:
list_of_git_files.append(git_yaml_file_with_path)

print(f'List of GIT files to push: {list_of_git_files}')

timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
commit_message = f'Python commiting to github at {timestamp}'
# Commint updated HUGO repository to GitHub, so that it can be picked up by HUGO compiler already with up-to-date image files

repo.index.add(list_of_git_files)
repo.index.commit(commit_message)
push_result = origin.push()
print(f'Push result: {push_result}')

######################### Main code end #################################
