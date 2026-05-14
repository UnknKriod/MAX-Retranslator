import time
import json

import aiohttp
import asyncio
from aiohttp_socks import ProxyConnector

from typing import List
import traceback
from logger import get_logger, LogLevel

class TelegramReceiver:
    def __init__(self, bot_token: str, source_chat_ids: List[str],
                 proxy_url: str = None, on_message_callback=None,
                 names_map_file: str = "telegram_names_map.json"):
        self.bot_token = bot_token
        self.source_chat_ids = set(str(cid) for cid in source_chat_ids)
        self.proxy_url = proxy_url
        self.on_message = on_message_callback
        self._session = None
        self._stop_event = asyncio.Event()
        self._offset = None
        self.names_map_file = names_map_file
        self.names_map = self._load_names_map()

        self.logger = get_logger(__name__)

    def _log(self, message: str, level: LogLevel = LogLevel.INFO):
        if level == LogLevel.DEBUG:
            self.logger.debug(message)
        elif level == LogLevel.INFO:
            self.logger.info(message)
        elif level == LogLevel.WARNING:
            self.logger.warning(message)
        elif level == LogLevel.ERROR:
            self.logger.error(message)

    def _load_names_map(self) -> dict:
        """Загрузить маппинг ID -> кастомное имя из JSON файла."""
        try:
            with open(self.names_map_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self._log(f"⚠️ Файл {self.names_map_file} не найден, создаю пустой.", LogLevel.WARNING)
            return {}
        except json.JSONDecodeError:
            self._log(f"❌ Ошибка в JSON файле {self.names_map_file}.", LogLevel.ERROR)
            return {}

    def _get_sender_name(self, from_data: dict) -> str:
        """Вернуть имя отправителя с подстановкой из мапы (если есть)."""
        user_id = str(from_data.get("id"))
        first_name = from_data.get("first_name", "")
        last_name = from_data.get("last_name", "")
        username = from_data.get("username")
        original = f"{first_name} {last_name}".strip() or username or "Unknown"

        mapped = self.names_map.get(user_id)
        if mapped:
            return f"{original} / {mapped}"
        return original
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            if self.proxy_url and self.proxy_url.startswith('socks5'):
                try:
                    connector = ProxyConnector.from_url(self.proxy_url)
                    self._session = aiohttp.ClientSession(connector=connector)
                except ImportError:
                    self._log("⚠️ aiohttp_socks не установлена, прокси не используется", LogLevel.WARNING)
                    self._session = aiohttp.ClientSession()
            else:
                self._session = aiohttp.ClientSession()
        return self._session
    
    async def _get_updates(self):
        """Запросить новые обновления."""
        session = await self._get_session()
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        params = {"timeout": 30, "allowed_updates": ["message"]}
        if self._offset is not None:
            params["offset"] = self._offset + 1
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        return data.get("result", [])
                    else:
                        self._log(f"❌ Ошибка Telegram API: {data}", LogLevel.ERROR)
                else:
                    self._log(f"❌ HTTP {resp.status} при getUpdates", LogLevel.ERROR)
        except Exception as e:
            self._log(f"❌ Ошибка получения обновлений: {e}", LogLevel.ERROR)
        return []
    
    async def _process_update(self, update):
        """Обработать одно обновление и вызвать колбэк."""
        if "message" not in update:
            return
        msg = update["message"]
        chat_id = str(msg["chat"]["id"])
        if chat_id not in self.source_chat_ids:
            return   # не из нужного чата
        
        # Извлекаем текст и медиа
        text = msg.get("text", "")
        caption = msg.get("caption", "")
        combined_text = text or caption
        
        # Формируем информацию об отправителе
        sender = msg["from"].get("first_name", "")
        if msg["from"].get("last_name"):
            sender += f" {msg['from']['last_name']}"

        sender = self._get_sender_name(msg["from"])
        timestamp = time.strftime('%H:%M:%S')
        
        formatted = f"[{timestamp}] {sender}:\n{combined_text}" if combined_text else f"[{timestamp}] {sender}"
        
        if self.on_message:
            await self.on_message(
                text=formatted
            )

    async def run(self):
        """Основной цикл получения сообщений."""
        self._log("📡 Запущен приёмник Telegram (long polling)")
        while not self._stop_event.is_set():
            try:
                updates = await self._get_updates()
                for upd in updates:
                    self._offset = upd["update_id"]
                    await self._process_update(upd)
                await asyncio.sleep(1)
            except Exception as e:
                self._log(f"❌ Ошибка в приёмнике Telegram: {e}", LogLevel.ERROR)
                traceback.print_exc()
                await asyncio.sleep(5)  # пауза перед повторной попыткой
        self._log("🛑 Приёмник Telegram остановлен")
    
    async def stop(self):
        self._stop_event.set()
        if self._session:
            await self._session.close()