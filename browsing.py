import os
from selenium import webdriver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# for handling embedded iframe contents:
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By



options = Options()
options.headless = True
#options.headless = False
browser = webdriver.Chrome("/usr/local/bin/chromedriver", chrome_options=options)

browser.get("https://www.cvonline.lt/darbo-skelbimas/tesonet/software-development-engineer-in-test-b2c-cyber-security-product-f4062788.html")
#browser.get("https://www.cvonline.lt/jobdata/4062788/leedu-1575380285-/j/36352007B3")
#timeout = 15
# Wait for iframe to load and switch to it as per advice in https://stackoverflow.com/questions/52327098/how-to-wait-iframe-page-load-in-selenium/52327853
wait(browser, 15).until(EC.frame_to_be_available_and_switch_to_it("JobAdFrame"))
#try:
    #elem = browser.text
elem = browser.find_element_by_tag_name("html")
    #print('Element:', elem)
page_html = browser.page_source
#
#print(page_html)

soup = BeautifulSoup(page_html)


##############

# remove <script> tags from results
js_junk = soup.find_all('script')
for match in js_junk:
    match.decompose()
# remove <style> tags from results
css_junk = soup.find_all('style')
for match in css_junk:
    match.decompose()
job_ad_frame_page = soup.find('body')
extracted_job_ad_text = job_ad_frame_page.get_text()


#############


print(extracted_job_ad_text)
#print(soup.get_text("\n")) 
    #print('Found <%s> element with that class name!' % (elem.text))
    #print('Text is: ', elem.text)
#except:
    #print('Was not able to find an element with that name.')