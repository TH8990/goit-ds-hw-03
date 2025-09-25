import requests
import json
from bs4 import BeautifulSoup
from pymongo import MongoClient
import os

# Базова URL-адреса сайту
BASE_URL = 'http://quotes.toscrape.com'
# Змінна оточення для підключення до MongoDB Atlas, з резервом на локальне підключення
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/") 

# Словник для зберігання унікальних авторів
authors_data = {}
# Список для зберігання всіх цитат
quotes_data = []

def parse_author_page(url):
    """
    Парсить сторінку автора і повертає його повну інформацію.
    """
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        fullname = soup.find('h3', class_='author-title').text.strip()
        born_date = soup.find('span', class_='author-born-date').text.strip()
        born_location = soup.find('span', class_='author-born-location').text.strip()
        description = soup.find('div', class_='author-description').text.strip()

        return {
            'fullname': fullname,
            'born_date': born_date,
            'born_location': born_location,
            'description': description
        }
    except Exception as e:
        print(f"Помилка при парсингу сторінки автора {url}: {e}")
        return None

def scrape_quotes():
    """
    Парсить усі сторінки з цитатами, збирає дані про цитати та авторів.
    """
    current_url = BASE_URL
    while current_url:
        print(f"Парсинг сторінки: {current_url}")
        try:
            response = requests.get(current_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            quotes_divs = soup.find_all('div', class_='quote')

            for quote_div in quotes_divs:
                quote_text = quote_div.find('span', class_='text').text
                author_name = quote_div.find('small', class_='author').text
                author_link = BASE_URL + quote_div.find('a')['href']
                tags = [tag.text for tag in quote_div.find_all('a', class_='tag')]

                # Збір цитат
                quotes_data.append({
                    'tags': tags,
                    'author': author_name,
                    'quote': quote_text
                })

                # Збір унікальних авторів
                if author_name not in authors_data:
                    authors_data[author_name] = parse_author_page(author_link)

            # Знайти посилання на наступну сторінку
            next_link = soup.find('li', class_='next')
            if next_link:
                current_url = BASE_URL + next_link.find('a')['href']
            else:
                current_url = None
        except Exception as e:
            print(f"Помилка при скрапінгу сторінки {current_url}: {e}")
            break

def save_to_json():
    """
    Зберігає зібрані дані у файли authors.json та quotes.json.
    """
    try:
        with open('quotes.json', 'w', encoding='utf-8') as f:
            json.dump(quotes_data, f, ensure_ascii=False, indent=2)
        print("Дані про цитати успішно збережено у quotes.json")

        with open('authors.json', 'w', encoding='utf-8') as f:
            json.dump(list(authors_data.values()), f, ensure_ascii=False, indent=2)
        print("Дані про авторів успішно збережено у authors.json")
    except Exception as e:
        print(f"Помилка при збереженні файлів JSON: {e}")

def import_to_mongodb():
    """
    Імпортує дані з JSON-файлів у колекції MongoDB Atlas.
    """
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping') # Перевірка підключення
        db = client['web_scraping_db']
        
        # Імпорт quotes
        quotes_collection = db['quotes']
        quotes_collection.delete_many({})  # Очистити колекцію перед імпортом
        quotes_collection.insert_many(quotes_data)
        print("Дані про цитати успішно імпортовано в MongoDB.")

        # Імпорт authors
        authors_collection = db['authors']
        authors_collection.delete_many({}) # Очистити колекцію перед імпортом
        authors_collection.insert_many(list(authors_data.values()))
        print("Дані про авторів успішно імпортовано в MongoDB.")

        client.close()
    except Exception as e:
        print(f"Помилка при імпорті в MongoDB: {e}")

if __name__ == '__main__':
    scrape_quotes()
    save_to_json()
    
    if MONGO_URI == "mongodb://localhost:27017/":
        print("Використано локальне підключення до MongoDB.")
    else:
        print("Використано підключення до MongoDB Atlas.")

    import_to_mongodb()