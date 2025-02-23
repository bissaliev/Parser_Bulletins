import io

import requests


def get_page(url: str) -> str:
    """Получает HTML-код страницы и текст страницы."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def download_file_as_bytes(url: str) -> io.BytesIO:
    """Скачивает файл по ссылке и возвращает его в виде потока байтов."""
    response = requests.get(url)
    response.raise_for_status()

    return io.BytesIO(response.content)
