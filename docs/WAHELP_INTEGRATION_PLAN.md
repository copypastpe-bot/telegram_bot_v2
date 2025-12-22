# План интеграции с Wahelp для amoCRM

## Требования

1. ✅ Все сообщения клиента и ответы бота отображаются в amoCRM
2. ✅ Можно переписываться с клиентом из amoCRM вручную
3. ✅ Когда менеджер переписывается с клиентом в amoCRM, бот НЕ обрабатывает сообщения кодом

---

## Как это работает

### Схема потока сообщений:

```
Клиент → Telegram → Ваш бот (polling)
                ↓
        Отправка в Wahelp → amoCRM
                ↓
        Менеджер отвечает в amoCRM → Wahelp webhook → Ваш бот → Клиент
```

### Проблема: Двойная обработка

Сообщения от клиентов могут приходить ДВА раза:

1. **Через Telegram polling** (от клиента напрямую) → нужно обработать кодом
2. **Через Wahelp webhook** (когда клиент пишет в amoCRM) → НЕ нужно обрабатывать кодом

---

## Решение

### Механизм определения источника сообщения

В Wahelp webhook payload есть поле `destination` или `direction`:

- `"from_operator"` или `"operator"` → сообщение от менеджера (из amoCRM)
- `"from_user"` или `"user"` → сообщение от клиента (через amoCRM)
- Отсутствует → сообщение от клиента напрямую (через Telegram)

### Логика обработки:

1. **Сообщение через Telegram polling:**
   - Обрабатываем кодом (бонусы, заказы и т.д.)
   - Отправляем в Wahelp → попадает в amoCRM

2. **Сообщение через Wahelp webhook от клиента:**
   - НЕ обрабатываем кодом (уже обработано при получении через polling)
   - Просто пересылаем клиенту в Telegram

3. **Сообщение через Wahelp webhook от менеджера:**
   - НЕ обрабатываем кодом
   - Пересылаем клиенту в Telegram

---

## Реализация

### Шаг 1: Отправка сообщений в Wahelp

При получении сообщения от клиента через Telegram:

```python
@dp.message(F.text)
async def handle_client_message(message: Message, state: FSMContext):
    client = await get_client_by_tg(message.from_user.id)
    if client and client.get("phone"):
        # Отправляем в Wahelp
        await send_to_wahelp(
            phone=client["phone"],
            name=client.get("full_name"),
            text=message.text,
            channel="clients_tg"
        )
    
    # Ваша логика обработки (бонусы, заказы и т.д.)
    ...
```

### Шаг 2: Получение ответов через Wahelp webhook

Настроить webhook от Wahelp для получения сообщений:

```python
async def handle_wahelp_inbound(payload: dict) -> bool:
    data = payload.get("data", {})
    destination = str(data.get("destination") or data.get("direction") or "").lower()
    
    # Если сообщение от менеджера или от клиента через amoCRM
    if destination in {"from_operator", "operator", "from_user", "user"}:
        # НЕ обрабатываем кодом, просто пересылаем клиенту
        await forward_to_client(data)
        return True
    
    return False
```

### Шаг 3: Пересылка ответов менеджера клиенту

```python
async def forward_to_client(wahelp_data: dict):
    # Получаем телефон из payload
    phone = extract_phone_from_payload(wahelp_data)
    
    # Находим клиента
    client = await get_client_by_phone(phone)
    if not client or not client.get("bot_tg_user_id"):
        return
    
    # Получаем текст сообщения
    text = extract_text_from_payload(wahelp_data)
    
    # Отправляем клиенту в Telegram
    await bot.send_message(client["bot_tg_user_id"], text)
```

---

## Как избежать двойной обработки

### Вариант 1: Проверка источника в обработчике

Добавить флаг в обработчик сообщений:

```python
# Глобальный словарь для отслеживания сообщений из Wahelp
wahelp_message_ids = set()

@dp.message(F.text)
async def handle_client_message(message: Message, state: FSMContext):
    # Если сообщение пришло через Wahelp webhook, не обрабатываем
    if message.message_id in wahelp_message_ids:
        return
    
    # Обрабатываем как обычно
    ...
```

### Вариант 2: Использовать флаг "в переписке с менеджером"

Добавить поле в БД `in_manager_chat` и проверять его:

```python
# При получении сообщения от менеджера через Wahelp
await conn.execute(
    "UPDATE clients SET in_manager_chat = true WHERE id = $1",
    client_id
)

# В обработчике сообщений
if client.get("in_manager_chat"):
    # Не обрабатываем кодом, только пересылаем
    return
```

### Вариант 3: Проверять timestamp последнего сообщения

Если последнее сообщение было от менеджера (через Wahelp), не обрабатывать кодом:

```python
# При получении сообщения от менеджера
last_manager_message_at = datetime.now()

# В обработчике
if last_manager_message_at and (datetime.now() - last_manager_message_at) < timedelta(minutes=5):
    # Недавно был ответ менеджера, не обрабатываем
    return
```

---

## Рекомендуемое решение

### Использовать проверку источника в Wahelp webhook

1. **Сообщения через Telegram polling:**
   - Обрабатываем кодом
   - Отправляем в Wahelp

2. **Сообщения через Wahelp webhook:**
   - Проверяем `destination`:
     - Если `"from_operator"` → пересылаем клиенту, НЕ обрабатываем кодом
     - Если `"from_user"` → пересылаем клиенту, НЕ обрабатываем кодом (уже обработано)
   - НЕ обрабатываем кодом в любом случае

3. **Дополнительно:** Добавить флаг `in_manager_chat` для случаев, когда менеджер активно переписывается

---

## Вопросы для уточнения

1. **Как часто менеджеры переписываются с клиентами?**
   - Если редко → можно использовать простую проверку источника
   - Если часто → нужен флаг `in_manager_chat`

2. **Нужно ли автоматически отключать обработку кодом при начале переписки с менеджером?**
   - Или достаточно проверять источник сообщения?

3. **Как долго должна быть "пауза" после ответа менеджера?**
   - 5 минут? 30 минут? До следующего сообщения от клиента?

