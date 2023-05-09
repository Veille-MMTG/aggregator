import logging
from GoogleNews import GoogleNews
from newspaper import Article, ArticleException
import nltk
import time
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import IndexModel, ASCENDING

import dotenv
import os

dotenv.load_dotenv()

MONGO_URL = os.getenv('MONGO_URL')


def store_articles():
    nltk.download('punkt')

    # Set up logging to file
    logging.basicConfig(filename='news.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(console)

    # Initialize GoogleNews
    googlenews = GoogleNews()

    # Set search query and language
    search_query = 'large language models'
    language = 'en'
    googlenews.set_lang(language)

    # Search for articles
    googlenews.search(search_query)

    # Log number of articles found
    logging.info(f"Number of articles found: {googlenews.total_count()}")

    # Get article URLs
    article_urls = googlenews.get_links()

    # Initialize MongoClient
    client = MongoClient(MONGO_URL)

    # Get the database and collection
    db = client['news_db']
    collection = db['articles']

    # Create a unique index on the 'url' field
    index = IndexModel([("url", ASCENDING)], unique=True)
    collection.create_indexes([index])

    # Scrape articles
    for url in article_urls:
        try:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            
            # Insert article data into MongoDB
            article_data = {
                'title': article.title,
                'text': article.text,
                'summary': article.summary,
                'keywords': article.keywords,
                'url': url
            }
            collection.insert_one(article_data)
            
            time.sleep(1) # Respect the request rate limit imposed by GoogleNews
        except ArticleException as ae:
            logging.error(f"Error downloading or parsing article: {url}")
            logging.error(str(ae))
        except DuplicateKeyError as dke:
            logging.warning(f"Article already exists in the database: {url}")
        except Exception as e:
            logging.error(f"Unknown error while processing article: {url}")
            logging.error(str(e))

    # Add finishing logging messages
    logging.info('Script finished executing.')
    logging.debug('End of script.')


def run():
    store_articles()


if __name__ == '__main__':
    store_articles()