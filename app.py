import json
import asyncio
import uuid
import time
import os
import sys
import signal
from collections import deque
from datetime import datetime, timedelta
from dotenv import load_dotenv
from webmax import WebMaxClient
from webmax.entities import Message
from notification_service import TelegramNotificationService
from telegram_receiver import TelegramReceiver

load_dotenv()

class FloodProtection:
	"""Защита от флуда — отслеживание паттерна спама."""
	
	def __init__(self, max_messages_per_minute: int = 60, 
				 max_messages_per_hour: int = 500):
		self.max_per_minute = max_messages_per_minute
		self.max_per_hour = max_messages_per_hour
		
		# Очереди временных меток
		self.minute_timestamps = deque(maxlen=max_messages_per_minute)
		self.hour_timestamps = deque(maxlen=max_messages_per_hour)
		
		self._lock = asyncio.Lock()
	
	async def check_flood(self, chat_id: str) -> tuple[bool, str]:
		"""
		Проверить, не превышен ли лимит.
		
		Returns:
			(is_flood, reason)
		"""
		async with self._lock:
			now = time.time()
			
			# Удаляем старые временные метки (> 1 часа)
			while self.hour_timestamps and (now - self.hour_timestamps[0]) > 3600:
				self.hour_timestamps.popleft()
			
			# Удаляем старые временные метки (> 1 минуты)
			while self.minute_timestamps and (now - self.minute_timestamps[0]) > 60:
				self.minute_timestamps.popleft()
			
			# Проверяем лимиты
			if len(self.minute_timestamps) >= self.max_per_minute:
				return True, f"⚠️ Достигнут лимит {self.max_per_minute} сообщений/минуту"
			
			if len(self.hour_timestamps) >= self.max_per_hour:
				return True, f"⚠️ Достигнут лимит {self.max_per_hour} сообщений/час"
			
			# Добавляем текущую метку времени
			self.minute_timestamps.append(now)
			self.hour_timestamps.append(now)
			
			return False, ""


class MaxBot:
	"""Обёртка для WebMaxClient с поддержкой вложений, таймаутов и защиты от флуда."""
	
	def __init__(self, session_name: str, phone: str, token: str = None, device_id: str = None,
				group_chat_id: str = None, group_name: str = None,
				telegram_bot_token: str = None, telegram_user_chat_id: str = None,
				telegram_group_chat_id: str = None, telegram_proxy_url: str = None,
				timeout: float = 30.0,
				history_chat_id: int = None,		# ID чата для сбора истории (если не совпадает с group_chat_id)
				history_limit: int = 20,		   # сколько последних сообщений получать при каждом запросе
				history_file: str = "history.json",
				max_target_chat_id: int = None,
				telegram_to_max_enabled: bool = False,
				telegram_names_map_file: str = None):
		
		self.client = WebMaxClient(session_name=session_name, phone=phone)
		self.client.token = token
		self.client.device_id = device_id or str(uuid.uuid4())

		self._shutdown_event = asyncio.Event()
		self._tasks = []
		
		self.group_chat_id = group_chat_id
		self.group_name = group_name
		
		# Rate limiting и таймауты
		self.flood_protection = FloodProtection()
		self.timeout = timeout
		
		self.history_chat_id = history_chat_id or (int(group_chat_id) if group_chat_id else None)
		self.history_limit = history_limit
		self.history_file = history_file
		self._history_task = None   # задача планировщика
		
		if telegram_bot_token:
			self.notifier = TelegramNotificationService(
				bot_token=telegram_bot_token, 
				proxy_url=telegram_proxy_url
			)
			self.telegram_user_chat_id = telegram_user_chat_id
			self.telegram_group_chat_id = telegram_group_chat_id

			self.max_target_chat_id = max_target_chat_id
			self.telegram_to_max_enabled = telegram_to_max_enabled
			self.telegram_receiver = None

			if self.telegram_to_max_enabled and self.notifier and self.max_target_chat_id:
				source_ids = os.getenv('TELEGRAM_SOURCE_CHAT_ID', '').split(',')
				if source_ids:
					self.telegram_receiver = TelegramReceiver(
						bot_token=telegram_bot_token,
						source_chat_ids=source_ids,
						proxy_url=telegram_proxy_url,
						on_message_callback=self._on_telegram_message,
						names_map_file=telegram_names_map_file
					)
					recv_task = asyncio.create_task(self.telegram_receiver.run())
					self._tasks.append(recv_task)
					print("✅ Telegram -> Max пересылка активирована")
			print(f"✅ Telegram-бот настроен")
		else:
			self.notifier = None
			print("⚠️ Telegram-бот не настроен — уведомления отключены")

		self.setup_handlers()

	async def _extract_attachments(self, message: Message) -> dict:
		attachments = {
			'photo_url': None,
			'video_url': None,
			'file_url': None,
			'file_name': None,
			'file_type': None
		}

		if not message.attaches:
			return attachments

		for attach in message.attaches:
			if isinstance(attach, dict):
				attach_type = attach.get('_type', 'UNKNOWN')

				if attach_type == 'PHOTO':
					if 'baseUrl' in attach:
						attachments['photo_url'] = attach['baseUrl']
						attachments['file_type'] = 'photo'
						attachments['file_name'] = f"photo_{attach.get('photoId', 'unknown')}.jpg"

				elif attach_type == 'VIDEO':
					video_id = attach.get('videoId')
					token = attach.get('token')
					if video_id and token:
						try:
							video_data = await self.client.play_video(video_id, token, message.chat_id, message.id)
							quality_keys = ['MP4_1080', 'MP4_720', 'MP4_480', 'MP4_360']
							video_url = None
							for key in quality_keys:
								if video_data.get(key):
									video_url = video_data[key]
									break
							attachments['video_url'] = video_url
							attachments['file_type'] = 'video'
							attachments['file_name'] = f"video_{video_id}.mp4"
						except Exception as e:
							print(f"⚠️ Не удалось получить ссылку на видео: {e}")

				elif attach_type == 'FILE':
					file_id = attach.get('fileId')
					token = attach.get('token')
					name = attach.get('name', 'file')
					if file_id and token:
						try:
							# Получаем ссылку на файл через API
							file_url = await self.client.get_file_url(file_id, message.chat_id, message.id)
							if file_url:
								attachments['file_url'] = file_url
								attachments['file_type'] = 'document'
								attachments['file_name'] = name
							else:
								print(f"⚠️ Не удалось получить URL для файла {name}")
						except Exception as e:
							print(f"⚠️ Ошибка получения URL файла: {e}")

				elif attach_type == 'AUDIO':
					if 'baseUrl' in attach:
						attachments['file_url'] = attach['baseUrl']
						attachments['file_type'] = 'audio'
						attachments['file_name'] = attach.get('name', f"audio_{attach.get('audioId', 'unknown')}.mp3")

		return attachments
	
	def setup_handlers(self):
		"""Установить обработчики сообщений."""
		
		@self.client.on_start()
		async def on_start():
			print(f"✅ Авторизован как {self.client.me.firstname} {self.client.me.lastname}")
		
		@self.client.on_message()
		async def handle_new_message(message: Message):
			await self.process_message(message)
				
	async def process_message(self, message: Message):
		"""Обработка одного сообщения (отправка в Telegram, логирование)"""
		try:
			# Автоматическое определение ID группы
			if self.group_chat_id is None and message.chat and self.group_name:
				if self.group_name.lower() in (message.chat.title or "").lower():
					self.group_chat_id = str(message.chat_id)
					print(f"✅ Определён ID группы: {self.group_chat_id}")

			# Проверка на флуд (опционально, можно отключить для истории)
			is_flood, reason = await self.flood_protection.check_flood(str(message.chat_id))
			if is_flood:
				print(reason)
				return

			timestamp = time.strftime('%H:%M:%S', time.localtime(message.time // 1000)) if message.time else "N/A"

			# Формируем текст
			if message.link and message.link.type == 'FORWARD' and message.link.forwarded_message:
				original = message.link.forwarded_message
				original_sender = await self.client.get_user_name(original.sender_id)
				text = original.text or ""
				attachments = await self._extract_attachments(original)
				prefix = f"[от {original_sender}]"
			else:
				text = message.text or ""
				attachments = await self._extract_attachments(message)
				prefix = ""

			sender = message.sender.firstname if message.sender else await self.client.get_user_name(message.sender_id) if message.sender_id else "Unknown"
			formatted_message = f"🆕 [{timestamp}] {sender}"

			if prefix:
				formatted_message += f" {prefix}"

			if text:
				formatted_message += f" {text}"

			print(formatted_message)

			message_to_send = f"""
<strong>⚠️⚠️⚠️ ВНИМАНИЕ! СООБЩЕНИЕ ИЗ MAX! ⚠️⚠️⚠️</strong>

🆕 [{timestamp}] <strong>{sender}</strong> <i>{prefix}</i>{f":\n{text}" if text else ""}
"""

			# Отправляем в Telegram
			if self.notifier:
				is_group_message = (self.group_chat_id is not None and 
								   str(message.chat_id) == self.group_chat_id)
				target_chat = self.telegram_group_chat_id if is_group_message else self.telegram_user_chat_id
				if not target_chat:
					print("⚠️ Не задан Telegram chat ID")
					return

				try:
					if attachments['photo_url']:
						await asyncio.wait_for(
							self.notifier.send(target_chat, message_to_send, photo_url=attachments['photo_url']),
							timeout=self.timeout
						)
					elif attachments['video_url']:
						# Скачать видео в байты
						video_bytes = await self.notifier.download_file_as_bytes(attachments['video_url'])
						if video_bytes:
							await asyncio.wait_for(
								self.notifier.send_file_from_bytes(
									chat_id=target_chat,
									file_bytes=video_bytes,
									file_name=attachments['file_name'],
									file_type='video',
									caption=message_to_send
								),
								timeout=self.timeout * 10  # больше времени на загрузку+отправку
							)
						else:
							# Запасной вариант: отправить только текст и ссылку
							fallback_text = message_to_send + f"\n\n⚠️ Не удалось загрузить видео: {attachments['video_url']}"
							await asyncio.wait_for(
								self.notifier.send(target_chat, fallback_text),
								timeout=self.timeout
							)
					elif attachments['file_url']:
						# Скачиваем файл в байты
						file_bytes = await self.notifier.download_file_as_bytes(attachments['file_url'])
						if file_bytes:
							await asyncio.wait_for(
								self.notifier.send_file_from_bytes(
									chat_id=target_chat,
									file_bytes=file_bytes,
									file_name=attachments['file_name'],
									file_type=attachments['file_type'],
									caption=message_to_send
								),
								timeout=self.timeout * 10
							)
						else:
							# Запасной вариант: отправить только текст и название файла
							fallback_text = message_to_send + f"\n\n⚠️ Не удалось загрузить файл: {attachments['file_name']}"
							await asyncio.wait_for(
								self.notifier.send(target_chat, fallback_text),
								timeout=self.timeout
							)
					else:
						await asyncio.wait_for(
							self.notifier.send(target_chat, message_to_send),
							timeout=self.timeout
						)
				except asyncio.TimeoutError:
					print(f"⏱️ Таймаут при отправке в Telegram (>{self.timeout}с)")
				except Exception as e:
					print(f"❌ Ошибка при отправке в Telegram: {e}")
		except Exception as e:
			print(f"❌ Ошибка в обработчике сообщений: {e}")
			import traceback
			traceback.print_exc()

	async def send_to_max(self, chat_id: int, text: str) -> bool:
		"""
		Отправить текстовое сообщение в указанный чат Max.

		Args:
			chat_id: ID чата (положительное число — личный диалог, отрицательное — группа)
			text: Текст сообщения

		Returns:
			bool: True в случае успеха
		"""
		try:
			# Генерация уникального локального ID сообщения (cid)
			cid = int(time.time() * 1000)

			# Вызов send_message
			message = await self.client.send_message(
				chat_id=chat_id,
				cid=cid,
				text=text,
				notify=False
			)
			return message is not None
		except Exception as e:
			print(f"❌ Ошибка отправки в Max: {e}")
			return False
	
	async def _history_scheduler(self):
		"""Планировщик: запускает получение истории в 11:00, 18:00, 20:00."""
		if not self.history_chat_id:
			print("⚠️ Не задан history_chat_id, планировщик истории не запущен.")
			return

		env_value = os.getenv('HISTORY_SCHEDULE_TIMES', '11:0,18:0,20:0')
		schedule_times = []
		for part in env_value.split(','):
			hour, minute = map(int, part.strip().split(':'))
			schedule_times.append((hour, minute))

		while True:
			try:
				now = datetime.now()
				next_run = None
				for hour, minute in schedule_times:
					candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
					if candidate > now:
						next_run = candidate
						break
				if next_run is None:
					first = schedule_times[0]
					next_run = (now + timedelta(days=1)).replace(hour=first[0], minute=first[1], second=0, microsecond=0)

				wait_seconds = (next_run - now).total_seconds()
				print(f"⏰ Следующая проверка истории в {next_run.strftime('%H:%M:%S')} (через {wait_seconds/3600:.1f} ч.)")
				await asyncio.sleep(wait_seconds)

				print(f"\n🕒 {datetime.now().strftime('%H:%M:%S')} – Запуск плановой проверки истории")
				await self.fetch_and_process_new_messages(self.history_chat_id)
			except Exception as e:
				print(f"❌ Ошибка в планировщике истории: {e}")
				import traceback
				traceback.print_exc()
				# Небольшая задержка перед повторной попыткой, чтобы не спамить
				await asyncio.sleep(60)
	
	async def fetch_and_process_new_messages(self, chat_id: int):
		"""Получить историю чата, найти новые сообщения и обработать их."""
		if not self.client or not hasattr(self.client, 'get_chat_history'):
			print("⚠️ Метод get_chat_history недоступен. Обновите библиотеку webmax.")
			return

		print(f"📥 Запрашиваем историю чата {chat_id}...")
		try:
			messages = await self.client.get_chat_history(
				chat_id,
				from_time=None,
				backward=self.history_limit,
				forward=0
			)
		except ConnectionError as e:
			print(f"❌ Потеряно соединение: {e}. Перезапуск клиента через 10 секунд...")
			asyncio.create_task(self._restart())
			return
		except Exception as e:
			print(f"❌ Ошибка получения истории: {e}")
			return

		if not messages:
			print("ℹ️ Нет сообщений в истории.")
			return
		
		# Определяем путь к файлу истории
		history_file = self._get_history_file_path(chat_id)
		file_exists = os.path.exists(history_file)

		# Если файла нет — первый запуск для этого чата: сохраняем ID и выходим без обработки
		if not file_exists:
			all_ids = {msg.id for msg in messages}
			self._save_message_ids(chat_id, all_ids)
			print(f"💾 Первый запуск для чата {chat_id}: сохранено {len(all_ids)} ID. Сообщения не обрабатывались.")
			return

		# Загружаем сохранённые ID
		saved_ids = self._load_saved_message_ids(chat_id)
		all_message_ids = {msg.id for msg in messages}
		new_message_ids = all_message_ids - saved_ids

		if not new_message_ids:
			print("ℹ️ Новых сообщений в истории не обнаружено.")
			# Обновляем сохранённые ID, если появились новые (например, вручную удалили из файла)
			if all_message_ids != saved_ids:
				self._save_message_ids(chat_id, all_message_ids)
			return

		print(f"✨ Найдено {len(new_message_ids)} новых сообщений.")

		new_messages = []
		duplicate_counter = 0

		for msg in reversed(messages):
			if msg.id in saved_ids:
				duplicate_counter += 1
				if duplicate_counter >= 3:
					break
			else:
				duplicate_counter = 0
				new_messages.append(msg)
				saved_ids.add(msg.id)

		# Сортируем по времени (от старых к новым)
		new_messages.sort(key=lambda m: m.time)

		for msg in new_messages:
			await self.process_message(msg)

		print(f"✅ Обработано {len(new_messages)} новых сообщений.")

		# Сохраняем все ID (и старые, и новые)
		self._save_message_ids(chat_id, saved_ids)
		print(f"✅ История обновлена, сохранено {len(saved_ids)} ID.")
	
	def _get_history_file_path(self, chat_id: int) -> str:
		"""Вернуть имя файла для указанного чата."""
		return f"history_{chat_id}.json"

	def _load_saved_message_ids(self, chat_id: int) -> set:
		"""Загрузить ID сообщений из history_{chat_id}.json."""
		file_path = self._get_history_file_path(chat_id)
		try:
			with open(file_path, 'r') as f:
				data = json.load(f)
				# Поддерживаем как старый формат (просто список), так и новый
				if isinstance(data, list):
					return set(data)
				else:
					return set(data.get(str(chat_id), []))
		except (FileNotFoundError, json.JSONDecodeError):
			return set()

	def _save_message_ids(self, chat_id: int, message_ids: set):
		"""Сохранить ID сообщений в history_{chat_id}.json."""
		file_path = self._get_history_file_path(chat_id)
		with open(file_path, 'w') as f:
			json.dump(list(message_ids), f, indent=2)

	async def _on_telegram_message(self, text: str):
		"""Колбэк, вызываемый при получении сообщения из Telegram."""
		if not self.max_target_chat_id:
			print("⚠️ Не задан MAX_TARGET_CHAT_ID, сообщение не отправлено")
			return

		# Отправляем в Max
		success = await self.send_to_max(
			chat_id=self.max_target_chat_id,
			text=text
		)
		if success:
			print(f"✅ Переслано в Max: {text}")
		else:
			print(f"❌ Ошибка пересылки в Max")
	
	def setup_signal_handlers(self):
		"""Установить обработчики сигналов для graceful shutdown."""
		
		def signal_handler(signum, frame):
			print(f"\n🛑 Получен сигнал {signal.Signals(signum).name}")
			asyncio.create_task(self.shutdown())
		
		signal.signal(signal.SIGINT, signal_handler)
		signal.signal(signal.SIGTERM, signal_handler)
	
	async def shutdown(self):
		"""Graceful shutdown."""
		print("\n⏹️ Инициируем graceful shutdown...")
		self._shutdown_event.set()
	
	async def start(self):
		"""Запустить клиент."""
		self.setup_signal_handlers()
		
		if self.client.token:
			await self._start_with_token()
		else:
			await self.client.start()
	
	async def _start_with_token(self):
		"""Внутренняя логика для запуска с токеном."""
		receiver_task = None
		action_task = None
		ping_task = None
	
		try:
			print("🚀 Инициализация клиента...")
			await self.client.connect_web_socket()
			print("✅ WebSocket подключен")
			
			from webmax import payloads
			instance = payloads.UserAgent(os_version='Windows', device_name='Opera')
			self.client.user_agent = instance.to_dict()
			print("✅ User-Agent создан")
			
			receiver_task = asyncio.create_task(
				self.client.message_receiver(),
				name='MessageReceiver'
			)
			self._tasks.append(receiver_task)
		
			await asyncio.wait_for(
				self.client.init(device_id=self.client.device_id),
				timeout=self.timeout
			)
			print("✅ Init успешен")
		
			await asyncio.wait_for(
				self.client.login(token=self.client.token),
				timeout=self.timeout
			)
			print(f"✅ Авторизация успешна")
		
			action_task = asyncio.create_task(
				self.client.action_handler(),
				name='ActionHandler'
			)
			self._tasks.append(action_task)
		
			ping_task = asyncio.create_task(
				self.client.ping_loop(),
				name='PingLoop'
			)
			self._tasks.append(ping_task)
			
			history_task = asyncio.create_task(self._history_scheduler(), name='HistoryScheduler')
			self._tasks.append(history_task)
		
			if self.client.on_start_handler:
				if asyncio.iscoroutinefunction(self.client.on_start_handler):
					await self.client.on_start_handler()
				else:
					self.client.on_start_handler()
		
			print("✅ Запущен")
			print("💡 Нажмите Ctrl+C для остановки\n")
			
			# Ожидаем сигнал shutdown или завершение задач
			shutdown_task = asyncio.create_task(self._shutdown_event.wait())

			done, pending = await asyncio.wait(
				self._tasks + [shutdown_task],
				return_when=asyncio.FIRST_COMPLETED
			)
		
			if shutdown_task in done:
				print("⏹️ Получен сигнал shutdown")
	
		except asyncio.TimeoutError:
			print(f"⏱️ Таймаут подключения (>{self.timeout}с)")
		except KeyboardInterrupt:
			print("\n⏹️ Ctrl+C")
		except Exception as e:
			print(f"❌ Ошибка: {e}")
			import traceback
			traceback.print_exc()
		finally:
			await self._cleanup()
		
	async def _cleanup(self):
		"""Очистка ресурсов при выходе."""
		print("\n🧹 Выполняем очистку...")

		print("⏳ Отмена задач...")
		for task in self._tasks:
			if not task.done():
				task.cancel()
		
		if self._tasks:
			await asyncio.gather(*self._tasks, return_exceptions=True)

			self._tasks.clear()
		
		if self.client.websocket:
			try:
				await self.client.websocket.close()
				print("✅ WebSocket закрыт")
			except Exception as e:
				print(f"⚠️ Ошибка при закрытии WebSocket: {e}")
		
		if self.notifier:
			await self.notifier.close()

		if self.telegram_receiver:
			await self.telegram_receiver.stop()
		
		print("✅ Очистка завершена")

	async def _restart(self):
		"""Перезапуск клиента: закрываем всё и запускаем заново."""
		print("🔄 Перезапуск клиента...")

		await self._cleanup()

		# Повторно запускаем start()
		await self.start()

async def main():
	manual_shutdown = False

	def signal_handler(signum, frame):
		nonlocal manual_shutdown
		print(f"\n🛑 Получен сигнал {signal.Signals(signum).name}")
		manual_shutdown = True

	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)

	while not manual_shutdown:
		bot = MaxBot(
			session_name=os.getenv('SESSION_NAME'),
			phone=os.getenv('PHONE'),
			token=os.getenv('TOKEN'),
			device_id=os.getenv('DEVICE_ID'),
			group_chat_id=os.getenv('GROUP_CHAT_ID'),
			group_name=os.getenv('GROUP_NAME'),
			telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN'),
			telegram_user_chat_id=os.getenv('TELEGRAM_USER_CHAT_ID'),
			telegram_group_chat_id=os.getenv('TELEGRAM_GROUP_CHAT_ID'),
			telegram_proxy_url=os.getenv('TELEGRAM_PROXY_URL'),
			timeout=30.0,
			history_chat_id=int(os.getenv('HISTORY_CHAT_ID')) if os.getenv('HISTORY_CHAT_ID') else None,
			history_limit=int(os.getenv('HISTORY_LIMIT', 20)),
			history_file=os.getenv('HISTORY_FILE', 'history.json'),
			max_target_chat_id=int(os.getenv('MAX_TARGET_CHAT_ID')),
			telegram_to_max_enabled=os.getenv('TELEGRAM_TO_MAX_ENABLED', 'false').lower() == 'true',
			telegram_names_map_file=os.getenv('TELEGRAM_NAMES_MAP_FILE', 'telegram_names_map.json')
		)

		try:
			await bot.start()
		except NeedRestartError:
			print("🔄 Перезапуск из-за потери соединения...")
			await asyncio.sleep(5)
			continue
		except KeyboardInterrupt:
			print("\n👋 Завершение по запросу пользователя")
			manual_shutdown = True
			break
		except Exception as e:
			print(f"❌ Необработанная ошибка: {e}")
			await asyncio.sleep(5)
			continue

		# Если бот завершился без исключения (например, по внутреннему shutdown), тоже перезапустим
		if not manual_shutdown:
			print("🔄 Бот остановлен")
			sys.exit(0)

if __name__ == "__main__":
	try:
		asyncio.run(main())
	except KeyboardInterrupt:
		print("\n✅ Программа завершена")