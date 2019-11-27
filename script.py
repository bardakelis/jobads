#!/usr/bin/env python3
import requests, bs4

exampleFile = open('joblists.html')
exampleSoup = bs4.BeautifulSoup(exampleFile.read())
#elems = exampleSoup.select('#joblist')
offer = exampleSoup.select('div.offer_primary')
count_of_offers = len(offer)
# looping through the list of jobs shown in a current page (subsequent pages need further code):
for x in range(len(offer)):
    #debug info
    print(x+1,'/',len(offer))
    #print(len(offer))
    #end debug info
    
    brief_offer = bs4.BeautifulSoup(str(offer[x]))
    # fetching position name
    job_ad_position_name = brief_offer.find('a').text   

    # fetching company name as company_info[0] -> needs reviewing
    #company_info = brief_offer.select('ul.cvo_module_offer_meta a')  
    company_name = brief_offer.find(itemprop='name').get_text()    
    job_location = brief_offer.find(itemprop='jobLocation').get_text()   

    # fetching salary range string, which needs further parsing to extract numbers
    salary_range = brief_offer.find('span').text    
    # Extract minimum salary if string "Nuo" exists:
    if salary_range.find('Nuo ') != -1:
        salary_from = salary_range.split("Nuo ",1)[1].split(' ')[0]
    else:
        salary_from = -1
    # Extract maximum salary if string "iki" exists
    if salary_range.find('iki ') != -1:
        salary_to = salary_range.split("iki ",1)[1].split(' ')[0]
    else:
        salary_to = -1
    # Extract currency used if string "atlygis" exists (to avoid case when no salary info provided at all)
    if salary_range.find('atlygis') != -1:
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
            #print('What we''ve found:', item.text)
            # Extract timestamp from string such as "Prasymus siuskite iki 2019.11.30" and then replace dots with dashes to match job post date format:
            valid_till = item.text.split()[-1].replace('.','-')
        else:
            valid_till = 'n/a'

    # fetching href uri location for the full job ad
    job_ad_href = brief_offer.find('a').get('href')



    # constructing a valid url for later retrieval of its contents
    job_ad_url = 'https:'+job_ad_href
    
    print(' ')
    print('-------Job offer as follows:--------')
    print('Position:', job_ad_position_name)
    
    print('Company name:', company_name)    
    print('Job location:', job_location)
    #
    # print('Company location:', company_info[1].text)
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
    #date_submitted = brief_offer.find('span').get('content')
    #date_submitted = brief_offer.find_all('span', attrs{'itemprop':'datePosted'})
    

    
    if x == len(offer) - 1:
        #print(offer[x])
        # Fetch job add full description from the full job ad URL:
        #url = job_ad_url
        url = 'https://www.cvonline.lt/darbo-skelbimas/albars-uab/it-administratorius-e-d4057194.html'
        chrome_ua = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
        job_ad_page_content = requests.get(url, headers=chrome_ua)
        #print(job_ad_page_content.text)
        job_ad_html = bs4.BeautifulSoup(job_ad_page_content.text, 'html.parser')
        job_ad_details = job_ad_html.select('div#page-main-content')
        #job_ad_details = job_ad_html.get_text()
        #print(job_ad_details)

        print(type(job_ad_html))
        print(type(job_ad_details))

        nu_dabar_kaip = bs4.BeautifulSoup(job_ad_details, 'html.parser')
        print(type(nu_dabar_kaip))
        job = nu_dabar_kaip.find('h3')
        


        print(job)

        # Fetch job application start and end dates, will require further processing to save values as a date:
        #application_dates = job_ad_html.select('span.application-date')

        




#print((elems[0]))