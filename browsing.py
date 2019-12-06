#import os
from os import linesep
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.support.ui import WebDriverWait

options = Options()
options.headless = True
browser = webdriver.Chrome("/usr/local/bin/chromedriver", chrome_options=options)
#tesonet ad:
#browser.get("https://www.cvonline.lt/darbo-skelbimas/tesonet/software-development-engineer-in-test-b2c-cyber-security-product-f4062788.html")
# rimi fish
browser.get("https://www.cvonline.lt/darbo-skelbimas/uab-rimi-lietuva/zuvies-pardavejas-a-f4058410.html")

# Wait for iframe with id=JobAdFrame to load and switch to it:
WebDriverWait(browser, 10).until(EC.frame_to_be_available_and_switch_to_it("JobAdFrame"))
elem = browser.find_element_by_tag_name("html")
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
extracted_job_ad_text = job_ad_frame_page.get_text()
########################################################




# removing empty lines, as from:
# https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
extracted_job_ad_text = linesep.join([s for s in extracted_job_ad_text.splitlines() if s])

print(extracted_job_ad_text)
