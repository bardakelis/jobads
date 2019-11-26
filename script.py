#!/usr/bin/env python3
import bs4, requests

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
    job_ad_url = brief_offer.find('a').get('href')
    
    print(' ')
    print('-------Job offer as follows:--------')
    print('Position:', job_ad_position_name)
    print('Company name:', company_info[0].text)    
    print('Company location:', company_info[1].text)
    print('Salary range:', salary_range)
    print('Job URL: https:'+job_ad_url)

    print('-------End of job offer --------')
    print(' ')
    #date_submitted = brief_offer.find('span').get('content')
    #date_submitted = brief_offer.find_all('span', attrs{'itemprop':'datePosted'})
    
    if x == len(offer) - 1:
        print(offer[x])
    
 
    
#print(type(exampleFile))
#print(type(offer[0]))



#print((elems[0]))