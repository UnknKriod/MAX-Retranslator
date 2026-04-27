import aiohttp
import io
from abc import ABC, abstractmethod
from typing import Optional, List, Union

class NotificationService(ABC):
    """Абстрактный базовый класс для сервисов уведомлений."""

    @abstractmethod
    async def send(self, chat_id: str, text: str) -> bool:
        """Отправить сообщение получателю с указанным идентификатором."""
        pass


class TelegramNotificationService(NotificationService):
    """Реализация уведомлений через Telegram Bot API с поддержкой SOCKS5 прокси и вложений."""

    # Максимальный размер файла для Telegram Bot API (в байтах)
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB

    def __init__(self, bot_token: str, proxy_url: str = None):
        self.bot_token = bot_token
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.proxy_url = proxy_url
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Создать или вернуть существующую сессию с прокси."""
        if self._session is None:
            if self.proxy_url and self.proxy_url.startswith('socks5'):
                try:
                    from aiohttp_socks import ProxyConnector
                    connector = ProxyConnector.from_url(self.proxy_url)
                    self._session = aiohttp.ClientSession(connector=connector)
                except ImportError:
                    print("❌ Telegram: библиотека aiohttp_socks не установлена. Установите: pip install aiohttp_socks")
                    self._session = aiohttp.ClientSession()
            else:
                self._session = aiohttp.ClientSession()
        return self._session

    async def download_file_as_bytes(self, url: str) -> Optional[bytes]:
        """Скачать файл по URL с правильными заголовками, вернуть bytes."""
        session = await self._get_session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'video/mp4,video/*;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Referer': 'https://max.ru/',
        }
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get('Content-Type', '')
                    if 'video' in content_type or 'octet-stream' in content_type:
                        data = await resp.read()
                        if len(data) <= self.MAX_FILE_SIZE:
                            return data
                        else:
                            print(f"❌ Файл слишком большой: {len(data)} > {self.MAX_FILE_SIZE}")
                            return None
                    else:
                        # Может быть HTML-страница, капча и т.п.
                        text_sample = (await resp.text())[:200]
                        print(f"❌ Неожиданный Content-Type {content_type}, ответ: {text_sample}")
                        return None
                else:
                    print(f"❌ Ошибка скачивания: HTTP {resp.status}")
                    return None
        except Exception as e:
            print(f"❌ Ошибка при скачивании: {e}")
            return None

    async def download_telegram_file(self, file_id: str) -> Optional[bytes]:
        """Скачать файл из Telegram по file_id, вернуть bytes."""
        # Получаем путь к файлу
        get_file_url = f"{self.api_url}/getFile"
        async with await self._get_session() as session:
            async with session.post(get_file_url, json={"file_id": file_id}) as resp:
                if resp.status != 200:
                    print(f"❌ Не удалось получить file_path: {await resp.text()}")
                    return None
                result = await resp.json()
                if not result.get("ok"):
                    print(f"❌ Ошибка getFile: {result}")
                    return None
                file_path = result["result"]["file_path"]

        # Скачиваем файл
        file_url = f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}"
        async with await self._get_session() as session:
            async with session.get(file_url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if len(data) <= self.MAX_FILE_SIZE:
                        return data
                    else:
                        print(f"❌ Файл слишком велик: {len(data)} > {self.MAX_FILE_SIZE}")
                        return None
                else:
                    print(f"❌ Ошибка скачивания файла: HTTP {resp.status}")
                    return None

    async def send(self, chat_id: str, text: str,
                   photo_url: str = None,
                   video_url: str = None,
                   file_url: str = None,
                   file_name: str = None) -> bool:
        """
        Отправить сообщение в Telegram (опционально с вложением).

        Args:
            chat_id: ID чата Telegram
            text: Текст сообщения
            photo_url: URL фотографии (опционально)
            video_url: URL видео (опционально)
            file_url: URL файла (опционально)
            file_name: Имя файла для отправки (опционально)

        Returns:
            bool: True если успешно, False если ошибка
        """
        if not chat_id:
            print("❌ Telegram: не указан chat_id")
            return False

        session = await self._get_session()

        try:
            # Если есть фото
            if photo_url:
                return await self._send_photo(session, chat_id, text, photo_url)

            # Если есть видео
            elif video_url:
                return await self._send_video(session, chat_id, text, video_url)

            # Если есть файл
            elif file_url:
                return await self._send_document(session, chat_id, text, file_url, file_name)

            # Просто текст
            else:
                return await self._send_text(session, chat_id, text)

        except Exception as e:
            print(f"❌ Telegram: исключение при отправке - {e}")
            return False

    async def _send_text(self, session: aiohttp.ClientSession, chat_id: str, text: str) -> bool:
        """Отправить текстовое сообщение."""
        endpoint = f"{self.api_url}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML'
        }

        try:
            async with session.post(endpoint, json=payload) as resp:
                if resp.status == 200:
                    print(f"✅ Telegram: текст отправлен в чат {chat_id}")
                    return True
                else:
                    error_text = await resp.text()
                    print(f"❌ Telegram: ошибка {resp.status} - {error_text}")
                    return False
        except Exception as e:
            print(f"❌ Telegram: ошибка при отправке текста - {e}")
            return False

    async def _send_photo(self, session: aiohttp.ClientSession, chat_id: str,
                         caption: str, photo_url: str) -> bool:
        """Отправить фото."""
        endpoint = f"{self.api_url}/sendPhoto"

        payload = {
            'chat_id': chat_id,
            'photo': photo_url,
            'caption': caption,
            'parse_mode': 'HTML'
        }

        try:
            async with session.post(endpoint, json=payload) as resp:
                if resp.status == 200:
                    print(f"✅ Telegram: фото отправлено в чат {chat_id}")
                    return True
                else:
                    error_text = await resp.text()
                    print(f"❌ Telegram: ошибка {resp.status} - {error_text}")
                    return False
        except Exception as e:
            print(f"❌ Telegram: ошибка при отправке фото - {e}")
            return False

    async def _send_video(self, session: aiohttp.ClientSession, chat_id: str,
                         caption: str, video_url: str) -> bool:
        """Отправить видео."""
        endpoint = f"{self.api_url}/sendVideo"

        payload = {
            'chat_id': chat_id,
            'video': video_url,
            'caption': caption,
            'parse_mode': 'HTML'
        }

        try:
            async with session.post(endpoint, json=payload) as resp:
                if resp.status == 200:
                    print(f"✅ Telegram: видео отправлено в чат {chat_id}")
                    return True
                else:
                    error_text = await resp.text()
                    print(f"❌ Telegram: ошибка {resp.status} - {error_text}")
                    return False
        except Exception as e:
            print(f"❌ Telegram: ошибка при отправке видео - {e}")
            return False

    async def _send_document(self, session: aiohttp.ClientSession, chat_id: str,
                            caption: str, file_url: str, file_name: str = None) -> bool:
        """Отправить документ/файл."""
        endpoint = f"{self.api_url}/sendDocument"

        payload = {
            'chat_id': chat_id,
            'document': file_url,
            'caption': caption,
            'parse_mode': 'HTML'
        }

        try:
            async with session.post(endpoint, json=payload) as resp:
                if resp.status == 200:
                    print(f"✅ Telegram: документ отправлен в чат {chat_id}")
                    return True
                else:
                    error_text = await resp.text()
                    print(f"❌ Telegram: ошибка {resp.status} - {error_text}")
                    return False
        except Exception as e:
            print(f"❌ Telegram: ошибка при отправке документа - {e}")
            return False

    async def send_file_from_bytes(self, chat_id: str, file_bytes: bytes,
                                   file_name: str, file_type: str,
                                   caption: str = "") -> bool:
        """
        Отправить файл из bytes (для файлов, загруженных из памяти).

        Args:
            chat_id: ID чата Telegram
            file_bytes: Содержимое файла в виде bytes
            file_name: Имя файла
            file_type: Тип файла ('photo', 'video', 'document' и т.д.)
            caption: Подпись к файлу

        Returns:
            bool: True если успешно, False если ошибка
        """
        if not chat_id:
            print("❌ Telegram: не указан chat_id")
            return False

        # Проверить размер файла
        if len(file_bytes) > self.MAX_FILE_SIZE:
            print(f"❌ Telegram: файл слишком большой ({len(file_bytes)} байт, макс {self.MAX_FILE_SIZE})")
            return False

        session = await self._get_session()

        # Определить endpoint по типу файла
        if file_type == 'photo':
            endpoint = f"{self.api_url}/sendPhoto"
            field_name = 'photo'
        elif file_type == 'video':
            endpoint = f"{self.api_url}/sendVideo"
            field_name = 'video'
        elif file_type == 'audio':
            endpoint = f"{self.api_url}/sendAudio"
            field_name = 'audio'
        else:
            endpoint = f"{self.api_url}/sendDocument"
            field_name = 'document'

        try:
            # Создать multipart форму
            data = aiohttp.FormData()
            data.add_field('chat_id', chat_id)
            data.add_field(field_name, io.BytesIO(file_bytes), filename=file_name)
            if caption:
                data.add_field('caption', caption)
                data.add_field('parse_mode', 'HTML')

            async with session.post(endpoint, data=data) as resp:
                if resp.status == 200:
                    print(f"✅ Telegram: файл '{file_name}' отправлен в чат {chat_id}")
                    return True
                else:
                    error_text = await resp.text()
                    print(f"❌ Telegram: ошибка {resp.status} - {error_text}")
                    return False

        except Exception as e:
            print(f"❌ Telegram: ошибка при отправке файла - {e}")
            return False

    async def close(self):
        """Закрыть сессию."""
        if self._session:
            await self._session.close()
            self._session = None