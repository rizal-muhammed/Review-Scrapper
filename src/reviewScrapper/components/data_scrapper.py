import requests
import os
import re
import socket
import time
import pandas as pd
from urllib.request import Request, urlopen
from urllib.error import URLError
from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path

from reviewScrapper.utils import create_directories
from reviewScrapper.entity import DataScrapperConfig
from reviewScrapper import logger
from reviewScrapper.utils import get_size

timeout = 5
socket.setdefaulttimeout(timeout)


class DataScrapper:
    def __init__(self, config: DataScrapperConfig):
        self.config = config
        create_directories([config.data_dir, config.meta_data_dir])
    
    def _fetch_page(self, url: str) -> str:
        """
        
        """
        try:
            logger.info(f"""Fetching page at url : 
                        {url}""")
            req = Request(url)
            try:
                response = urlopen(req)
                time.sleep(3)
            except URLError as e:
                if hasattr(e, 'reason'):
                    print('We failed to reach a server.')
                    print('Reason: ', e.reason)
                elif hasattr(e, 'code'):
                    print('The server couldn\'t fulfill the request.')
                    print('Error code: ', e.code)
            else:
                resultant_page = response.read()
                response.close()
                
                return resultant_page
        except Exception as e:
            raise e
    
    def _fetch_search_page(self, search_string: str) -> str:
        """
        
        """
        try:
            logger.info("Fetching product page url...")

            url = self.config.source_URL + "/search?q=" + search_string
            search_page = self._fetch_page(url)
            return search_page
        except Exception as e:
            raise e
    
    def _get_product_page_url(self, search_page: str) -> str:
        """
        
        """
        try:
            product_page_html = BeautifulSoup(search_page, "html.parser")

            # fetch product details section
            product_details = product_page_html.find_all('div', {'class': '_1YokD2 _3Mn1Gg'}, limit=1)

            # fetch url of first product
            first_product = product_details[0].find('div', {'class': '_1AtVbE col-12-12'})
            product_url = first_product.div.div.div.a['href']
            
            # complete url of page for first product 
            product_page_url = self.config.source_URL + str(product_url)

            return product_page_url
        except Exception as e:
            raise e
    
    def _get_product_page(self, search_page: str) -> str:
        """
        
        """
        try:
            product_page_url = self._get_product_page_url(search_page)

            # Fetch product page
            product_page = self._fetch_page(url=product_page_url)

            return product_page
            
        except Exception as e:
            raise e
    
    def _get_reviews_page_url(self, product_page: str) -> str:
        """
        
        """
        try:
            reviews_page_html = BeautifulSoup(product_page, "html.parser")

            # Locate reviews section
            right_sections = reviews_page_html.find_all('div', {'class': '_1YokD2 _3Mn1Gg col-8-12'}, limit=1)
            right_sections_sub = right_sections[0].find_all('div', {'class': '_1YokD2 _3Mn1Gg'}, limit=1)
            sections = right_sections_sub[0].find_all('div', {'class': '_1AtVbE col-12-12'})
            review_section = sections[5]

            # Extract url for reviews
            reviews_link_cls = review_section.find('div', {'class': 'col JOpGWq'})
            reviews_links = reviews_link_cls.find_all('a')
            reviews_url = reviews_links[-1].get('href')

            # # fetch product details section
            # product_details = reviews_page_html.find_all('div', {'class': 'col JOpGWq'}, limit=1)

            # # fetch url of first product
            # reviews_url = product_details[0].find_all('a', limit=1)[0].get('href')
            
            # complete url of page for first product 
            reviews_page_url = self.config.source_URL + str(reviews_url)

            return reviews_page_url
        except Exception as e:
            raise e
    
    def _get_reviews_page(self, product_page:str) -> str:
        """
        
        """
        try:
            reviews_page_url = self._get_reviews_page_url(product_page)

            # Fetch reviews page
            reviews_page = self._fetch_page(url=reviews_page_url)

            return reviews_page
        except Exception as e:
            raise e
    
    def _get_total_no_of_pages_in_reviews_page(self, reviews_page_url: str) -> str:
        """
        
        """
        try:
            reviews_page = self._fetch_page(reviews_page_url)
            reviews_page_html = BeautifulSoup(reviews_page, "html.parser")

            # Locate reviews section
            reviews = reviews_page_html.find_all('div', {'class': '_1AtVbE col-12-12'})
            total_no_of_pages = int(reviews[-1].div.div.span.text.split()[-1].replace(',', ''))

            return total_no_of_pages
        
        except Exception as e:
            raise e
    
    def _scrap_a_review(self, review: str) -> pd.DataFrame:
        """
        
        """
        try:
            column_names = ['Rating', 'Review Head', 'Review']
            df = pd.DataFrame(columns=column_names)

            review_data = []
            try:
                rating = review.div.div.div.div.div.text
            except Exception as e:
                rating = 'No Rating'
            
            try:
                review_head = review.div.div.div.div.find_all('p', {'class': '_2-N8zT'})[0].text
            except Exception as e:
                review_head = 'No Review Heading'
            
            try:
                review = review.div.div.div.find('div', {'class': 't-ZTKy'}).div.text
            except Exception as e:
                review = 'No Review'
            
            review_data.append(rating)
            review_data.append(review_head)
            review_data.append(review)

            df = pd.concat([df, pd.DataFrame([review_data], columns=df.columns)], ignore_index=True)
            
            return df

        except Exception as e:
            raise e
    
    def _extract_reviews_in_a_page(self, reviews_page_url: str) -> pd.DataFrame:
        """
        
        """
        try:
            reviews_page = self._fetch_page(reviews_page_url)
            reviews_page_html = BeautifulSoup(reviews_page, 'html.parser')
            reviews = reviews_page_html.find_all('div', {'class': '_1AtVbE col-12-12'})
            del reviews[-1]
            del reviews[0:4]

            column_names = ['Rating', 'Review Head', 'Review']
            df = pd.DataFrame(columns=column_names)

            for review in reviews:
                review_df = self._scrap_a_review(review)
                df = pd.concat([df, review_df], ignore_index=True)
            
            return df
        
        except Exception as e:
            raise e
    
    def _save_file(self, df: pd.DataFrame):
        """
        
        """
        try:
            logger.info("Saving file...")
            filename = self.config.search_string + "_reviews.csv"
            if os.path.exists(os.path.join(self.config.data_dir, filename)):
                df_old = pd.read_csv(os.path.join(self.config.data_dir, "reviews.csv"))
                df_old = pd.concat([df_old, df], ignore_index=True)
                df_old.to_csv(os.path.join(self.config.data_dir, filename), index=False)
            else:
                df.to_csv(os.path.join(self.config.data_dir, filename), index=False)

            logger.info("File saved successfully.")
        except Exception as e:
            raise e
    
    def _extract_reviews(self, product_page: str) -> pd.DataFrame:
        """
        
        """
        try:
            # Fetch reviews page
            reviews_page_url = self._get_reviews_page_url(product_page)
            total_no_of_pages = self._get_total_no_of_pages_in_reviews_page(reviews_page_url)
            total_no_of_pages = 10
            last_page_read_filepath = os.path.join(self.config.meta_data_dir, "last_page_read.txt")

            if (not os.path.exists(last_page_read_filepath)) or (os.path.getsize(last_page_read_filepath) == 0):
                
                for page_num in range(1, total_no_of_pages+1):
                    if re.search(r'&page=\d+$', reviews_page_url):
                        reviews_page_url = re.sub(r'&page=\d+$', "&page=" + str(page_num) , reviews_page_url)
                    else:
                        reviews_page_url = reviews_page_url + "&page=" + str(page_num)
                    df = self._extract_reviews_in_a_page(reviews_page_url)
                    self._save_file(df)
                    with open(last_page_read_filepath, "w") as f:
                        f.write(str(page_num))
                    if total_no_of_pages // 10 == 0:
                        time.sleep(5)
            else:
                with open(last_page_read_filepath, "r+") as f:
                    last_page_read = int(f.read())
                
                for page_num in range(last_page_read, total_no_of_pages+1):
                    if re.search(r'&page=\d+$', reviews_page_url):
                        reviews_page_url = re.sub(r'&page=\d+$', "&page=" + str(page_num) , reviews_page_url)
                    else:
                        reviews_page_url = reviews_page_url + "&page=" + str(page_num)
                    df = self._extract_reviews_in_a_page(reviews_page_url)
                    self._save_file(df)
                    with open(last_page_read_filepath, "w") as f:
                        f.write(str(page_num)) 
                    if total_no_of_pages // 10 == 0:
                        time.sleep(5)

        except Exception as e:
            raise e
    
    def review_scrapper(self):
        """
        
        """
        try:
            logger.info("Review scrapping...")

            search_string = str(self.config.search_string)
            search_string = search_string.replace(" ", "")  # remove spaces in between.
            logger.info(f"The search string is : {search_string}")

            search_page = self._fetch_search_page(search_string)
            product_page = self._get_product_page(search_page)

            self._extract_reviews(product_page)
        
        except Exception as e:
            raise e
        