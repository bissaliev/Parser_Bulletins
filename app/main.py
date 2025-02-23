import time
from datetime import datetime

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
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 2023


def main():
    current_page = START_PAGE
    count_entries = 0
    count_pages = 0
    logger.info("Начало парсинга страниц...")
    try:
        while current_page:
            try:
                logger.info(f"Обрабатывается страница: {current_page}")
                content = get_page(current_page)
                parser = Parser(content, MIN_YEAR, CURRENT_YEAR)
            except requests.RequestException as e:
                logger.error(
                    f"Ошибка при загрузке страницы {current_page}: {e}"
                )
                break  # Останавливаем выполнение
            link_files = parser.extract_files()
            if not link_files:
                logger.info("Файлы не найдены, завершаем работу.")
                break

            logger.info(f"Скачивание файлов со страницы {current_page}")
            for file_url, bidding_date in link_files:
                file_stream = download_file_as_bytes(BASE_URL + file_url)
                try:
                    xls_extractor = XLSExtractor(file_stream, bidding_date)
                    data = xls_extractor.get_data()
                    logger.info(
                        f"Начало загрузки данных торгов {bidding_date} в БД"
                    )
                    with SessionLocal() as session:
                        mass_create_trade(session, data)
                        session.commit()
                        count_entries += 1
                        logger.info(
                            f"Торги {bidding_date} сохранены ({count_entries})"
                        )
                    logger.info(f"Данных торгов {bidding_date} загружены в БД")
                except XLSExtractorError as e:
                    logger.error(f"Ошибка обработки файла {file_url}: {e}")
                    continue
                except Exception as e:
                    logger.error(
                        f"Неизвестная ошибка обработки файла {file_url}: {e}"
                    )
                    continue

            next_page = parser.get_next_page()
            if not next_page:
                logger.info("Следующая страница не найдена")
                break
            current_page = BASE_URL + next_page
            count_pages += 1
            logger.info(f"Обработано {count_pages} страниц")
            time.sleep(2)
    except KeyboardInterrupt:
        logger.warning(
            "Программа прервана пользователем (Ctrl + C). Завершение работы..."
        )
    except Exception as e:
        logger.error(f"Программа прервана {e}", exc_info=True)
    finally:
        logger.info(
            f"Завершение работы. Всего обработано {count_pages} страниц. "
            f"В БД сохранено {count_entries} записей."
        )


if __name__ == "__main__":
    main()
