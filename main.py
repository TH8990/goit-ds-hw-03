import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "cats_db"
COLLECTION_NAME = "cats"


def get_collection():
    """Створює та повертає об'єкт колекції MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        # Перевірка підключення
        client.admin.command('ping')
        db = client[DB_NAME]
        return db[COLLECTION_NAME]
    except ConnectionFailure as e:
        print(f"Помилка підключення до MongoDB: {e}")
        return None
    except OperationFailure as e:
        print(f"Помилка операції з MongoDB: {e}")
        return None

# --- CRUD операції ---

# Create 
def create_cat(collection, name, age, features):
    """Додає нового кота в колекцію."""
    try:
        if collection.find_one({"name": name}):
            print(f"Кіт з ім'ям '{name}' вже існує.")
            return None
        cat_data = {
            "name": name,
            "age": age,
            "features": features
        }
        result = collection.insert_one(cat_data)
        print(f"Кіт '{name}' успішно доданий. ID: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"Помилка при створенні кота: {e}")
        return None

# Read
def read_all_cats(collection):
    """Виводить всі записи з колекції."""
    print("\n--- Список усіх котів ---")
    try:
        cats = collection.find({})
        if cats:
            for cat in cats:
                print(cat)
        else:
            print("Колекція порожня.")
    except Exception as e:
        print(f"Помилка при читанні всіх записів: {e}")

def read_cat_by_name(collection, name):
    """Виводить інформацію про кота за його ім'ям."""
    print(f"\n--- Пошук кота з ім'ям '{name}' ---")
    try:
        cat = collection.find_one({"name": name})
        if cat:
            print(cat)
        else:
            print(f"Кіт з ім'ям '{name}' не знайдений.")
    except Exception as e:
        print(f"Помилка при пошуку кота: {e}")

# Update
def update_cat_age_by_name(collection, name, new_age):
    """Оновлює вік кота за ім'ям."""
    print(f"\n--- Оновлення віку кота '{name}' на {new_age} ---")
    try:
        result = collection.update_one(
            {"name": name},
            {"$set": {"age": new_age}}
        )
        if result.matched_count:
            print(f"Вік кота '{name}' успішно оновлено.")
        else:
            print(f"Кіт з ім'ям '{name}' не знайдений.")
    except Exception as e:
        print(f"Помилка при оновленні віку: {e}")

def add_cat_feature_by_name(collection, name, new_feature):
    """Додає нову характеристику до списку features кота за ім'ям."""
    print(f"\n--- Додавання характеристики '{new_feature}' коту '{name}' ---")
    try:
        result = collection.update_one(
            {"name": name},
            {"$push": {"features": new_feature}}
        )
        if result.matched_count:
            print(f"Характеристика '{new_feature}' успішно додана.")
        else:
            print(f"Кіт з ім'ям '{name}' не знайдений.")
    except Exception as e:
        print(f"Помилка при додаванні характеристики: {e}")

# Delete
def delete_cat_by_name(collection, name):
    """Видаляє запис з колекції за ім'ям тварини."""
    print(f"\n--- Видалення кота '{name}' ---")
    try:
        result = collection.delete_one({"name": name})
        if result.deleted_count:
            print(f"Кіт '{name}' успішно видалений.")
        else:
            print(f"Кіт з ім'ям '{name}' не знайдений.")
    except Exception as e:
        print(f"Помилка при видаленні кота: {e}")

def delete_all_cats(collection):
    """Видаляє всі записи з колекції."""
    print("\n--- Видалення всіх котів ---")
    try:
        result = collection.delete_many({})
        print(f"Видалено {result.deleted_count} записів.")
    except Exception as e:
        print(f"Помилка при видаленні всіх записів: {e}")

# --- Запуск скрипта ---
if __name__ == "__main__":
    cats_collection = get_collection()
    if cats_collection is not None:
        # Приклад використання функцій
        
        # Додавання тестових даних (якщо колекція порожня)
        if not cats_collection.find_one():
            print("Колекція порожня. Додаю тестові дані.")
            create_cat(cats_collection, "barsik", 3, ["ходить в капці", "дає себе гладити", "рудий"])
            create_cat(cats_collection, "lapa", 5, ["любить спати", "не боїться собак"])
            create_cat(cats_collection, "marta", 2, ["грайлива", "любить молоко"])

        # Завдання: Читання
        read_all_cats(cats_collection)
        read_cat_by_name(cats_collection, "barsik")
        read_cat_by_name(cats_collection, "non_existent_cat")

        # Завдання: Оновлення
        update_cat_age_by_name(cats_collection, "barsik", 4)
        add_cat_feature_by_name(cats_collection, "lapa", "не любить воду")
        read_all_cats(cats_collection) # Перевірка змін

        # Завдання: Видалення
        delete_cat_by_name(cats_collection, "marta")
        read_all_cats(cats_collection) # Перевірка видалення
        
        # Завдання: Видалення всіх
        # delete_all_cats(cats_collection)
        # read_all_cats(cats_collection) # Перевірка після повного видалення