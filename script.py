import requests
import psycopg2
from collections import defaultdict
import time
import logging
from config import TRELLO_API_KEY, TRELLO_TOKEN, MAIN_TRELLO_DESK_ID, DONE_CARDS_LIST_ID, DOING_CARDS_LIST_ID, H_DOING_CARDS_LIST_ID, VENDOR_LIST_ID, TIMUR_ID, HOZHIMUROD_ID, LABELS

# Настройка логирования
logging.basicConfig(
    filename='trello_to_grafana.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Функция получения данных из Trello
def get_trello_data():
    url = f'https://api.trello.com/1/boards/{MAIN_TRELLO_DESK_ID}/cards'
    params = {
        'key': TRELLO_API_KEY,
        'token': TRELLO_TOKEN,
    }
    response = requests.get(url, params=params)
    return response.json()

# Функция классификации карточек
def categorize_cards(cards):
    counts = defaultdict(int)
    label_counts_timur = defaultdict(int)
    label_counts_hozhimurod = defaultdict(int)
    none_cards_per_project = defaultdict(int)
    done_count = 0
    total_cards_count = 0
    timur_count = 0
    hozhimurod_count = 0
    vendor_count = 0
    timur_cards = 0
    hozhimurod_cards = 0
    none_cards = 0

    for card in cards:
        list_id = card['idList']
        members = card['idMembers']
        labels = [label['id'] for label in card['labels']]

        if list_id == DONE_CARDS_LIST_ID:
            done_count += 1
        else:
            total_cards_count += 1

            if list_id == DOING_CARDS_LIST_ID:
                timur_count += 1
            elif list_id == H_DOING_CARDS_LIST_ID:
                hozhimurod_count += 1
            elif list_id == VENDOR_LIST_ID:
                vendor_count += 1

            project_assigned = False
            for project, label_id in LABELS.items():
                if label_id in labels:
                    project_assigned = True
                    counts[project] += 1
                    if TIMUR_ID in members:
                        label_counts_timur[project] += 1
                    if HOZHIMUROD_ID in members:
                        label_counts_hozhimurod[project] += 1

            if TIMUR_ID in members:
                timur_cards += 1
            if HOZHIMUROD_ID in members:
                hozhimurod_cards += 1
            if TIMUR_ID not in members and HOZHIMUROD_ID not in members:
                none_cards += 1
                if project_assigned:
                    for project, label_id in LABELS.items():
                        if label_id in labels:
                            none_cards_per_project[project] += 1

    return (counts, label_counts_timur, label_counts_hozhimurod, 
            done_count, total_cards_count, timur_count, 
            hozhimurod_count, vendor_count, timur_cards, 
            hozhimurod_cards, none_cards, none_cards_per_project)

# Функция сохранения данных в базу данных PostgreSQL
def save_statistics_to_db(counts, label_counts_timur, label_counts_hozhimurod, 
                           done_count, total_cards_count, timur_count, 
                           hozhimurod_count, vendor_count, timur_cards, 
                           hozhimurod_cards, none_cards, none_cards_per_project):
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="PromoAdmin1540@", host="localhost")
        cursor = conn.cursor()

        # Очистка старых данных
        cursor.execute("DELETE FROM trello_statistics")

        for project, count in counts.items():
            cursor.execute(
                """
                INSERT INTO trello_statistics (project, count, done_count, total_cards_count, timur_count, hozhimurod_count, vendor_count, timur_cards, hozhimurod_cards, none_cards, timur_label_count, hozhimurod_label_count, none_cards_per_project)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    project, count, done_count, total_cards_count, timur_count, hozhimurod_count, 
                    vendor_count, timur_cards, hozhimurod_cards, none_cards, 
                    label_counts_timur.get(project, 0),  
                    label_counts_hozhimurod.get(project, 0),  
                    none_cards_per_project.get(project, 0)
                )
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return False

# Основной запуск скрипта
if __name__ == '__main__':
    while True:
        try:
            cards = get_trello_data()
            counts, label_counts_timur, label_counts_hozhimurod, done_count, total_cards_count, timur_count, hozhimurod_count, vendor_count, timur_cards, hozhimurod_cards, none_cards, none_cards_per_project = categorize_cards(cards)
            if save_statistics_to_db(counts, label_counts_timur, label_counts_hozhimurod, done_count, total_cards_count, timur_count, hozhimurod_count, vendor_count, timur_cards, hozhimurod_cards, none_cards, none_cards_per_project):
                logging.info("Data successfully saved to PostgreSQL.")
            else:
                logging.error("Failed to save data to PostgreSQL.")
        except Exception as e:
            logging.error(f"Error occurred: {e}")
        
        time.sleep(60)
