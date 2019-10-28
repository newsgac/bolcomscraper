import json
import logging

import pandas
import requests

from bs4 import BeautifulSoup

from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

class BolComScraper:
    # &bltgc=r0jdQdRo-qj-peaDkTJC2Q
    # sorted by 'Best verkocht'. Boeken. Nederlands.
    root_url = 'https://www.bol.com/nl/l/ajax/index.html?filterN=11209%2B8293&n=24410&origin=8&section=books&sort=SEARCH_RANK1'

    pages_dir = Path(__file__).parent / 'pages'
    reviews_dir = Path(__file__).parent / 'reviews'

    def get_page(self, i):
        url = self.root_url + '&page=' + str(i)
        logging.debug('Fetching page ' + url)
        return requests.get(url)

    def save_pages(self, max_pages=100):
        for i in range(1, max_pages):
            with open(self.pages_dir / str(i), 'w') as f:
                f.write(self.get_page(i).text)

    def parse_author(self, item):
        if 'top' in item and len(item['top']) > 0 and 'partySeo' in item['top'][0]:
            return item['top'][0]['partySeo']['name']
        else:
            return None

    def parse_item(self, item):
        price = 0
        try:
            price = float(item['price']['price']['price']) + float(item['price']['price'].get('priceFraction', "0")) / 100
        except KeyError as e:
            logging.warn('Error parsing price: ' + str(item))
            logging.warn(str(e))
        return {
            'ratingScore': item['rating']['score'],
            'reviewCount': item['rating']['reviewCount'],
            'percentage': item['rating']['percentage'],
            'subtitle': item.get('subTitle', ""),
            'price': price,
            'id': item['globalId'],
            'description': item['description'],
            'title': item['title'],
            'author': self.parse_author(item)
        }

    def parse_page(self, pagestr: str) -> pandas.DataFrame:
        page_dict = json.loads(pagestr)
        items = page_dict['itemsContent']['items']
        df = pandas.DataFrame()
        for item in items:
            df = df.append(self.parse_item(item), ignore_index=True)
        return df

    def parse_pages_folder(self) -> pandas.DataFrame:
        df = None

        for file_name in self.pages_dir.glob('*'):
            with open(file_name, 'r') as f:
                logging.info('parsing page' + str(file_name))
                new_df = self.parse_page(f.read())
                df = new_df if df is None else df.append(new_df)

        return df.reset_index(drop=True)

    def save_reviews_for_products(self):
        products = pandas.read_csv('products.csv')
        for idx, product in products.iterrows():
            self.save_reviews(product)

    def get_reviews_page(self, product_id: str):
        review_url = f'https://www.bol.com/nl/rnwy/productPage/reviews?productId={product_id}&offset=0&limit=1000'
        logging.debug('Getting url' + review_url)
        return requests.get(review_url)

    def save_reviews(self, product: pandas.Series):
        reviews_str = self.get_reviews_page(product['id']).text
        if reviews_str and len(reviews_str) > 5:
            with open(self.reviews_dir / str(product['id']), 'w') as f:
                f.write(self.get_reviews_page(product['id']).text)

    def parse_review(self, reviewstr) -> pandas.DataFrame:
        df = pandas.DataFrame()

        soup = BeautifulSoup(reviewstr)
        for review_soup in soup.ul.findAll('li', recursive=False):
            review = {
                'id': review_soup.attrs['id'],
                'title': review_soup.find('strong', {'class': 'review__title'}).text,
                'date': review_soup.find('li', {'data-test': 'review-author-date'}).text,
                'rating': review_soup.find('input', {'name': 'rating-value'}).attrs['value'],
                'body': review_soup.find('p', {'data-test': 'review-body'}).text,
                'feedback_positive': review_soup.find('a', {'class': 'review-feedback__btn--positive'}).text,
                'feedback_negative': review_soup.find('a', {'class': 'review-feedback__btn--negative'}).text
            }
            df = df.append(review, ignore_index=True)

        return df

    def parse_reviews_folder(self) -> pandas.DataFrame:
        df = None

        for file_name in self.reviews_dir.glob('*'):
            try:
                with open(file_name, 'r') as f:
                    logging.info('parsing review' + str(file_name))
                    new_df = self.parse_review(f.read())
                    new_df['product_id'] = file_name.stem
                    df = new_df if df is None else df.append(new_df)
            except Exception as e:
                logger.warn('Error parsing ' + str(file_name))
                logger.warn(str(e))

        return df.reset_index(drop=True)


if __name__ == '__main__':
    scraper = BolComScraper()
    # scraper.save_pages()
    # df = scraper.parse_pages_folder()
    # df.to_csv('products.csv', index=False)
    # scraper.save_reviews_for_products()
    reviews = scraper.parse_reviews_folder()
    reviews.to_csv('reviews.csv', index=False)
