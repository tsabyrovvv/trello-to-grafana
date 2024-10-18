import requests
import psycopg2
from collections import defaultdict
import time
import logging

# Настройка логирования
logging.basicConfig(
    filename='trello_to_grafana.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Trello API ключи и идентификаторы
TRELLO_API_KEY = '0b49763f2558b26be20fcdba3e737ff1'
TRELLO_TOKEN = 'ATTA8bbb07c46425f1254f183778679bbce0ce5cdf3d2bdfd68ec5827c9e25170e678FFC1398'
MAIN_TRELLO_DESK_ID = '645e0f810362a8d4c115dfe1'
DONE_CARDS_LIST_ID = '645e0f810362a8d4c115dfea'
DOING_CARDS_LIST_ID = '645e0fa9647dfebff89df26d'
H_DOING_CARDS_LIST_ID = '666c69f5813bbc54d27d2282'
VENDOR_LIST_ID = '645e0fb96c4832ff53d045d8'

# IDs участников
TIMUR_ID = '645e0f20922d475268f8c7f1'
HOZHIMUROD_ID = '6145f6d528c7d867683128ae'

# Теги и их идентификаторы
LABELS = {
    'SR': '645e0f811ef89e539277b0e4',
    'Yandex': '660ed11113deef4bd6c51eb9',
    'БЗ': '645e0f811ef89e539277b0e1',
    'LE': '645e0f811ef89e539277b0da',
    'В очереди': '660ee7adbdaa125b3cee36ab'
}

LOCAL_LABELS = {
    'SR': '645e0f811ef89e539277b0e4',
    'Yandex': '660ed11113deef4bd6c51eb9',
    'БЗ': '645e0f811ef89e539277b0e1',
    'LE': '645e0f811ef89e539277b0da',
    'В очереди': '660ee7adbdaa125b3cee36ab'
}

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
    none_cards_per_project = defaultdict(int)  # Добавляем счетчик none cards по проектам
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

        print(f"Processing card {card['id']} with labels {labels} and members {members}")

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

            # Проверяем метки 'SR', 'Yandex', 'БЗ'
            project_assigned = False  # Флаг, что карточка относится к проекту
            for project, label_id in LABELS.items():
                if label_id in labels:
                    project_assigned = True  # Карточка относится к проекту
                    counts[project] += 1
                    if TIMUR_ID in members:
                        print(f"Timur is a member of card {card['id']} with project {project}")
                        label_counts_timur[project] += 1
                    if HOZHIMUROD_ID in members:
                        print(f"Hozhimurod is a member of card {card['id']} with project {project}")
                        label_counts_hozhimurod[project] += 1

            # Подсчет карточек по участникам
            if TIMUR_ID in members:
                timur_cards += 1
            if HOZHIMUROD_ID in members:
                hozhimurod_cards += 1
            if TIMUR_ID not in members and HOZHIMUROD_ID not in members:
                none_cards += 1
                # Если карточка не имеет участников, увеличиваем счетчик по проекту
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
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="root", host="localhost")
        cursor = conn.cursor()

        for project, count in counts.items():
            cursor.execute(
                """
                INSERT INTO trello_statistics (project, count, done_count, total_cards_count, timur_count, hozhimurod_count, vendor_count, timur_cards, hozhimurod_cards, none_cards, timur_label_count, hozhimurod_label_count, none_cards_per_project)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (project) DO UPDATE
                SET count = EXCLUDED.count,
                    done_count = EXCLUDED.done_count,
                    total_cards_count = EXCLUDED.total_cards_count,
                    timur_count = EXCLUDED.timur_count,
                    hozhimurod_count = EXCLUDED.hozhimurod_count,
                    vendor_count = EXCLUDED.vendor_count,
                    timur_cards = EXCLUDED.timur_cards,
                    hozhimurod_cards = EXCLUDED.hozhimurod_cards,
                    none_cards = EXCLUDED.none_cards,
                    timur_label_count = CASE WHEN EXCLUDED.timur_label_count IS NULL THEN 0 ELSE EXCLUDED.timur_label_count END,
                    hozhimurod_label_count = CASE WHEN EXCLUDED.hozhimurod_label_count IS NULL THEN 0 ELSE EXCLUDED.hozhimurod_label_count END,
                    none_cards_per_project = EXCLUDED.none_cards_per_project
                """,
                (
                    project, count, done_count, total_cards_count, timur_count, hozhimurod_count, 
                    vendor_count, timur_cards, hozhimurod_cards, none_cards, 
                    label_counts_timur.get(project, 0),  
                    label_counts_hozhimurod.get(project, 0),  
                    none_cards_per_project.get(project, 0)  # Добавляем none cards для каждого проекта
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