import time
from datetime import datetime
from pathlib import Path

import requests
from database.crud import mass_create_trade
from database.database import SessionLocal
from logging_config import logger
from parsers.parser import Parser
from parsers.scraper import download_file_as_bytes, get_page
from utils.file_utils import XLSExtractor, XLSExtractorError

BASE_URL = "https://spimex.com"
START_PAGE = (
    "https://spimex.com/markets/oil_products/trades/results/?page=page-1"
)
SAVE_DIR = Path("xls_files")
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 2023
SHEET_NAME = "TRADE_SUMMARY"
TABLE_NAME = "Единица измерения: Метрическая тонна"


def main():
    current_page = START_PAGE
    count = 0
    logger.info("Начало парсинга страниц...")
    while current_page:
        try:
            content = get_page(current_page)
            parser = Parser(content, MIN_YEAR, CURRENT_YEAR)
            logger.info(f"Обрабатывается страница: {current_page}")
        except requests.RequestException as e:
            logger.error(f"Ошибка при загрузке страницы {current_page}: {e}")
            break  # Останавливаем выполнение
        link_files = parser.extract_files()
        if not link_files:
            logger.info("Файлы не найдены, завершаем работу.")
            break

        for file_url, date_fie in link_files:
            file_stream = download_file_as_bytes(file_url)
            try:
                xls_extractor = XLSExtractor(file_stream, date_fie)
                data = xls_extractor.get_data()
                with SessionLocal() as session:
                    mass_create_trade(session, data)
                    session.commit()
                    count += 1
                    logger.info(
                        f"Файл {file_url} загружен и сохранен в БД ({count})"
                    )
            except XLSExtractorError as e:
                logger.error(f"Ошибка обработки файла {file_url}: {e}")
                continue

        next_page = parser.get_next_page()
        if not next_page:
            logger.info("Достигнут конец страниц.")
            break
        current_page = BASE_URL + next_page
        time.sleep(2)


if __name__ == "__main__":
    main()
