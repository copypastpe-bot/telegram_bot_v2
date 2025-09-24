# Telegram Bot v2

Бот для внутреннего учета заказов, бонусов и клиентов.

## Стек
- Python 3.10+
- aiogram v3
- asyncpg
- PostgreSQL
- python-dotenv

## Запуск
1. Установить зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Настроить `.env` (см. `docs/dev_guide.md`).
3. Запустить:
   ```bash
   python bot.py
   ```

## Документация
- [Project Spec](docs/project_spec.md)
- [Dev Guide](docs/dev_guide.md)

## Миграции

Применить миграции к базе:

```bash
# вариант 1: DB_DSN уже в .env
scripts/apply_migrations.sh

# вариант 2: явная передача
DB_DSN="postgresql://bot:***@localhost:5432/clients_db" scripts/apply_migrations.sh
```

## Roadmap

- [ ] Настроить миграции и базовые таблицы (`clients`, `orders`, `staff`, `bonus_transactions`).
- [ ] Реализовать кнопку **«Я выполнил заказ»** (FSM пошаговый ввод).
- [ ] Добавить расчёт зарплаты мастеров.
- [ ] Ведение кассовой книги (доходы/расходы/прибыль).
- [ ] Реализовать поздравления с ДР и бонусы по правилам.
- [ ] Сделать рассылки по клиентам и лидам (WhatsApp).
- [ ] Добавить роли (суперадмин, админ, мастер) с правами доступа.
- [ ] Подключить интеграции с внешними сервисами (по необходимости).