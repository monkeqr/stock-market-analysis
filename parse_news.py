#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import time
import logging
from datetime import datetime

# --- Настройка ---
BASE_URL = "https://t.me/s/bbbreaking"
OUTPUT_FILE = "bbbreaking_news.csv"   # формат: date;news
STOP_DATE = datetime(2025, 1, 1)      # парсим до начала 2025 (не включительно)
REQUEST_TIMEOUT = 10
RETRIES = 3
SLEEP_BETWEEN_PAGES = 1.5

# --- Логгер ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parse_news.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def http_get(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TelegramParser/1.0; +https://example.com)"
    }
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            return r
        except Exception as e:
            logger.warning(f"Ошибка запроса [{attempt}/{RETRIES}] {url}: {e}")
            time.sleep(1 + attempt)
    return None


def parse_date(date_str):
    """Пытаемся распарсить дату в datetime, возвращаем naive datetime или None."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        pass
    for fmt in ("%d.%m.%Y", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass
    return None


def load_existing_keys():
    existing = set()
    if not os.path.exists(OUTPUT_FILE):
        logger.info("CSV не найден, будет создан новый.")
        return existing

    try:
        with open(OUTPUT_FILE, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                date = row.get("date", "").strip()
                news = row.get("news", "").strip()
                existing.add((date, news))
        logger.info(f"Загружено {len(existing)} уже сохранённых новостей.")
    except Exception as e:
        logger.error(f"Ошибка чтения {OUTPUT_FILE}: {e}")
    return existing


def extract_id_from_href(href):
    """Ищет конечную цифру в href типа /channel/123456"""
    if not href:
        return None
    m = re.search(r"/(\d+)(?:/?$)", href)
    if m:
        return m.group(1)
    return None


def clean_text(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def parse_channel():
    next_page = BASE_URL
    existing = load_existing_keys()
    added = 0
    processed_pages = 0

    write_header = not os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as fout:
        writer = csv.writer(fout, delimiter=";")
        if write_header:
            writer.writerow(["date", "news"])

        while next_page:
            logger.info(f"Загружаем страницу: {next_page}")
            resp = http_get(next_page)
            processed_pages += 1
            if resp is None:
                logger.error(f"Не удалось загрузить страницу после {RETRIES} попыток: {next_page}")
                break
            if resp.status_code != 200:
                logger.error(f"Страница вернула статус {resp.status_code}: {next_page}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            messages = soup.find_all("div", class_="tgme_widget_message_wrap")
            logger.info(f"Найдено {len(messages)} сообщений на странице.")

            if not messages:
                logger.warning("Сообщения на странице отсутствуют — возможно, Telegram вернул пустую страницу.")
                break

            page_added = 0
            ids_on_page = []

            for msg in messages:
                # Текст
                text_block = msg.find("div", class_="tgme_widget_message_text")
                text = clean_text(text_block.get_text(" ") if text_block else "")

                # Дата
                time_block = msg.find("time")
                if not time_block:
                    continue
                date_str = time_block.get("datetime", time_block.text.strip())
                date = parse_date(date_str)
                if not date:
                    continue
                if date < STOP_DATE:
                    logger.info("Достигнута граница STOP_DATE — парсинг завершён.")
                    next_page = None
                    break

                key = (date.isoformat(), text)
                if key not in existing:
                    writer.writerow([date.isoformat(), text])
                    fout.flush()
                    existing.add(key)
                    added += 1
                    page_added += 1
                    logger.info(f"Добавлена новость: {date.isoformat()} {text[:80]}...")

                # Получаем ID для пагинации
                post_id = msg.get("data-post")
                if post_id:
                    m = re.search(r"/?(\d+)$", post_id)
                    if m:
                        ids_on_page.append(int(m.group(1)))
                else:
                    a = msg.find("a", class_="tgme_widget_message_date")
                    if a and a.has_attr("href"):
                        id_from_href = extract_id_from_href(a["href"])
                        if id_from_href:
                            ids_on_page.append(int(id_from_href))

            logger.info(f"На странице добавлено {page_added} новых записей.")

            # Пагинация по максимальному ID на странице
            if next_page and ids_on_page:
                max_id = max(ids_on_page)
                next_page = f"{BASE_URL}?before={max_id}"
                logger.debug(f"Переход к следующей странице: {next_page}")
                time.sleep(1)
            else:
                if not ids_on_page and next_page:
                    logger.warning("Не найден ID ни одного сообщения — пагинация невозможна, завершаем.")
                next_page = None

    logger.info(f"Парсинг завершён: обработано страниц {processed_pages}, добавлено {added} новых новостей.")


if __name__ == "__main__":
    logger.info("Старт парсинга...")
    try:
        parse_channel()
    except Exception as exc:
        logger.exception(f"Скрипт завершился с ошибкой: {exc}")
    logger.info("Работа программы завершена. Все новости сохранены в CSV.")
