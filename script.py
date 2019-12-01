#!/usr/bin/env python3
import requests, bs4



def job_ads_crawler(url_to_crawl):

    res = requests.get(url_to_crawl, headers=chrome_ua)
    
    whole_page = bs4.BeautifulSoup(res.text)
    offer = whole_page.select('div.offer_primary')
    count_of_offers_in_page = len(offer)
    # looping through the list of jobs shown in a current page (subsequent pages need further code):
    for x in range (count_of_offers_in_page):
        print(x+1,'/',count_of_offers_in_page)
        brief_offer = bs4.BeautifulSoup(str(offer[x]))
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

        # Printing the stuff out:
        print(' ')
        print('-------Job offer as follows:--------')
        print('Position:', job_ad_position_name)
        print('Company name:', company_name)    
        print('Job location:', job_location)
        print('Salary range:', salary_range)
        print('Salary from:', salary_from)
        print('Salary to:', salary_to)
        print('Salary currency:', salary_currency)
        print('Pay interval:', pay_interval)
        print('Salary amount type:', salary_amount_type)
        print('Job URL:', job_ad_url)
        print('Job post date:', date_posted)
        print('Offer valid till:', valid_till)
        print('-------End of job offer --------')
        print(' ')

        
        # Crawler is pretending to be Chrome browser on Windows:
        job_ad_page_content = requests.get(job_ad_url, headers=chrome_ua)
        # parse detailed job ad text
        job_ad_html = bs4.BeautifulSoup(job_ad_page_content.text, 'html.parser')
        # Assuming that a standard cvonline.lt page formatting is used with page-main-content div (otherwise detailed ad text won't be available for extraction)
        # So ads embedded from other sources won't be fetched
        job_ad_details = str(job_ad_html.select('div#page-main-content'))     
        extracted_job_ad_text = bs4.BeautifulSoup(job_ad_details, 'html.parser').get_text()

        print(extracted_job_ad_text)

    # Check if there are any further ads in the next page, or it is just a single page of results: 
    #next_page_value = whole_page.find('li', class_='page_next').text
    next_page_tag = whole_page.find('li', class_='page_next')
    # If there is no tag with class page_next (NoneType returned), this means that result fits on a single page:
    if next_page_tag is None:
        more_pages = 0
    else:
        next_page_text = next_page_tag.text
        print('Next Page value:', next_page_text)
        # If we see a button with text "Toliau*" (next), then it's a multi-page output and crawler needs to get to the next page:
        if 'Toliau' in next_page_text:
            print('Seeing more pages, will continue crawling on the next one...')
            # Set indicator to 1 if there's yet another page with results (a "Next" button):
            more_pages = 1
        else:
            # Set zero if there's no "Next" button on the page:
            more_pages = 0
    # prepare a tupe to be returned from the function:
    feedback = (more_pages, count_of_offers_in_page)
    return feedback
    

# Crawler is pretending to be Chrome browser on Windows:
chrome_ua = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}

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
region = 'vilniaus'


# crawling_unfinished set to 1 to indicate that crawler is looping through pages 
# If multiple pages of job ads are returned, this value is set to 1 and only if 
# last page of multiple pages is returned (or there was a single page in total)
# it is set to 0 to exit crawling loop:
crawling_ongoing = 1
# page_no is page number to request 1st and subsequent pages of job ads in the web site
page_no = 0
# initializing total ad counter:
ads_in_current_page = 0
ads_total = 0
while crawling_ongoing == 1:
    url = f"https://www.cvonline.lt/darbo-skelbimai/{timespan}/{job_area}/{region}?page={page_no}"
    print('url is:', url)
    feedback_from_crawler = job_ads_crawler(url)
    # If 1, crawling will go to the next page of results:
    crawling_ongoing = feedback_from_crawler[0]
    # Number of ads processed in previously crawled page:
    ads_in_current_page = feedback_from_crawler[1]
    ads_total += ads_in_current_page
    page_no += 1

    
#
print('Number of ad pages:', page_no)
print('Number of ads retrieved:', ads_total)