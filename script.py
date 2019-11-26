#!/usr/bin/env python3
import requests, bs4

exampleFile = open('/home/fogelis/proj/cvish/joblists.html')
exampleSoup = bs4.BeautifulSoup(exampleFile.read())
#elems = exampleSoup.select('#joblist')
offer = exampleSoup.select('div.offer_primary')
count_of_offers = len(offer)
for x in range(len(offer)):
    #debug info
    print(x)
    print(len(offer))
    #end debug info
    
    brief_offer = bs4.BeautifulSoup(str(offer[x]))

    job_ad_position_name = brief_offer.find('a').text   
    company_info = brief_offer.select('ul.cvo_module_offer_meta a')   
    salary_range = brief_offer.find('span').text    
    job_ad_href = brief_offer.find('a').get('href')
    job_ad_url = 'https:'+job_ad_href
    
    print(' ')
    print('-------Job offer as follows:--------')
    print('Position:', job_ad_position_name)
    print('Company name:', company_info[0].text)    
    print('Company location:', company_info[1].text)
    print('Salary range:', salary_range)
    print('Job URL:', job_ad_url)
    

    print('-------End of job offer --------')
    print(' ')
    #date_submitted = brief_offer.find('span').get('content')
    #date_submitted = brief_offer.find_all('span', attrs{'itemprop':'datePosted'})
    

    
    if x == len(offer) - 1:
        #print(offer[x])
        # Fetch job add full description from the full job ad URL:
        url = job_ad_url
        look_like_a_browser = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
        job_ad_page_content = requests.get(url, headers=look_like_a_browser)
        #print(job_ad_page.text)
        job_ad_html = bs4.BeautifulSoup(job_ad_page_content.text, 'html.parser')
        job_ad_details = job_ad_html.select('div#page-main-content')
        print(job_ad_details)
        offer_valid_dates = job_ad_html.select('span.application-date')

        print(offer_valid_dates[0].text)
        print(offer_valid_dates[1].text)
 
    
print(type(job_ad_html))
print(type(job_ad_details))



#print((elems[0]))