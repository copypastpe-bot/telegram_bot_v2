# Техническая настройка интеграции с amoCRM через Wahelp

## Как это работает технически

### Схема потока данных:

```
Клиент → Telegram → Ваш бот (polling)
                ↓
        Wahelp API → Канал "clients_tg" → amoCRM
                ↓
        Менеджер отвечает в amoCRM → Wahelp webhook → Ваш бот → Клиент
```

---

## Канал Wahelp: `clients_tg`

### Что это такое?

`clients_tg` — это канал в Wahelp, который:
- Предназначен для клиентского Telegram бота
- Уже настроен в вашем Wahelp аккаунте (если используется админ-бот)
- Подключен к amoCRM (если настроена интеграция)

### Где используется?

В админ-боте (`tgbot-v1`) канал `clients_tg` уже используется для:
- Отправки уведомлений клиентам
- Получения ответов от клиентов
- Интеграции с amoCRM

---

## Что нужно настроить

### 1. Переменные окружения для Wahelp API

Добавить в `.env` клиентского бота:

```bash
# Wahelp API credentials
WAHELP_LOGIN=your_login
WAHELP_PASSWORD=your_password
# ИЛИ использовать токен напрямую:
WAHELP_ACCESS_TOKEN=your_access_token

# Wahelp API base URL (обычно не нужно менять)
WAHELP_API_BASE=https://api.wahelp.ru

# Канал для клиентского Telegram бота
WAHELP_CLIENTS_PROJECT_ID=your_project_id
WAHELP_CLIENTS_CHANNEL_UUID=your_channel_uuid
```

### 2. Где взять эти значения?

#### `WAHELP_CLIENTS_PROJECT_ID` и `WAHELP_CLIENTS_CHANNEL_UUID`

Эти значения уже есть в админ-боте, если он использует Wahelp. Можно:
- Посмотреть в `.env` админ-бота (`tgbot-v1/.env`)
- Или получить через Wahelp API/интерфейс

#### `WAHELP_LOGIN` и `WAHELP_PASSWORD`

Это credentials для доступа к Wahelp API. Обычно те же, что используются в админ-боте.

#### `WAHELP_ACCESS_TOKEN` (альтернатива)

Если есть токен доступа, можно использовать его вместо логина/пароля.

---

## Техническая реализация отправки

### Как отправляется сообщение в amoCRM:

```python
from crm.wahelp_service import send_text_to_phone

# Отправка сообщения клиента в Wahelp
await send_text_to_phone(
    channel_kind="clients_tg",  # Канал для клиентского Telegram бота
    phone=client["phone"],       # Телефон клиента
    name=client.get("full_name"), # Имя клиента
    text=message.text            # Текст сообщения
)
```

### Что происходит внутри:

1. **`send_text_to_phone`** вызывает Wahelp API:
   ```
   POST https://api.wahelp.ru/app/projects/{project_id}/channels/{channel_uuid}/messages
   {
     "user": {"phone": "+79991234567", "name": "Иван"},
     "message": {"text": "Привет, нужна помощь"}
   }
   ```

2. **Wahelp создаёт или находит пользователя** в канале `clients_tg`

3. **Wahelp отправляет сообщение** в канал

4. **Если канал подключен к amoCRM**, Wahelp автоматически пересылает сообщение в amoCRM

5. **Менеджер видит сообщение** в интерфейсе amoCRM и может ответить

---

## Техническая реализация получения ответов

### Webhook от Wahelp:

```python
# Настройка webhook сервера
from notifications.webhook import start_wahelp_webhook

wahelp_webhook = await start_wahelp_webhook(
    pool=pool,
    host="0.0.0.0",
    port=8080,
    token="your_webhook_token",
    inbound_handler=handle_wahelp_inbound
)
```

### Обработчик входящих сообщений:

```python
async def handle_wahelp_inbound(payload: dict) -> bool:
    data = payload.get("data", {})
    destination = str(data.get("destination") or data.get("direction") or "").lower()
    
    # Только ответы от менеджера
    if destination not in {"from_operator", "operator"}:
        return False
    
    # Получаем телефон и текст
    phone = extract_phone_from_payload(data)
    text = extract_text_from_payload(data)
    
    # Находим клиента и пересылаем
    client = await get_client_by_phone(phone)
    if client and client.get("bot_tg_user_id"):
        await bot.send_message(client["bot_tg_user_id"], text)
    
    return True
```

---

## Настройка канала в Wahelp

### Если канал `clients_tg` ещё не настроен:

1. **Зайти в Wahelp** (https://wahelp.ru)
2. **Создать проект** (если ещё нет)
3. **Создать канал** типа "Telegram" с алиасом `clients_tg`
4. **Подключить канал к amoCRM:**
   - В настройках канала выбрать интеграцию с amoCRM
   - Указать credentials для amoCRM
   - Настроить маппинг полей (телефон, имя и т.д.)

5. **Получить `project_id` и `channel_uuid`:**
   - `project_id` — ID проекта в Wahelp
   - `channel_uuid` — UUID канала

6. **Настроить webhook:**
   - URL: `https://your-server.com/wahelp/webhook`
   - Token: сгенерировать токен для безопасности

---

## Вопросы для уточнения

1. **Используется ли уже канал `clients_tg` в админ-боте?**
   - Если да → можно использовать те же credentials
   - Если нет → нужно создать канал в Wahelp

2. **Есть ли доступ к Wahelp аккаунту?**
   - Нужны логин/пароль или токен доступа
   - Нужны права на создание/настройку каналов

3. **Подключен ли канал `clients_tg` к amoCRM?**
   - Если да → всё готово, просто отправляем сообщения
   - Если нет → нужно настроить интеграцию в Wahelp

4. **Есть ли публичный URL для webhook?**
   - Нужен для получения ответов от менеджеров
   - Должен быть доступен из интернета
   - Должен иметь SSL (HTTPS)

---

## Итог

**Канал:** `clients_tg` — канал Wahelp для клиентского Telegram бота

**Как отправлять:**
- Использовать функцию `send_text_to_phone(channel_kind="clients_tg", ...)`
- Wahelp автоматически перешлёт в amoCRM (если канал подключен)

**Как получать ответы:**
- Настроить webhook от Wahelp
- Обрабатывать сообщения с `destination="from_operator"`
- Пересылать клиенту в Telegram

**Что нужно:**
- Credentials для Wahelp API
- `WAHELP_CLIENTS_PROJECT_ID` и `WAHELP_CLIENTS_CHANNEL_UUID`
- Канал `clients_tg` должен быть подключен к amoCRM в Wahelp

