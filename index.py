import motor.motor_asyncio
import bson
import json
from datetime import datetime, timedelta
import asyncio
import calendar
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
import os
from dotenv import load_dotenv

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
db = client['company_db']
collection = db['salaries']

def load_bson(file_path):
    with open(file_path, 'rb') as file:
        return list(bson.decode_all(file.read()))

def load_metadata(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

async def insert_data(data):
    await collection.insert_many(data)

# Генерация периодов времени между двумя датами в зависимости от group_type
def generate_periods(dt_from, dt_upto, group_type):
    current = dt_from
    while current <= dt_upto:
        yield current
        if group_type == 'day':
            current += timedelta(days=1)
        elif group_type == 'month':
            current = datetime(current.year + (current.month // 12), (current.month % 12) + 1, 1)
        elif group_type == 'hour':
            current += timedelta(hours=1)

# Функция агрегации зарплат по месяцам
async def aggregate_by_month(dt_from, dt_upto, collection):
    pipeline = [
        {'$match': {'dt': {'$gte': dt_from, '$lte': dt_upto}}},
        {'$group': {
            '_id': {'year': {'$year': '$dt'}, 'month': {'$month': '$dt'}},
            'total_amount': {'$sum': {'$ifNull': ['$value', 0]}}
        }},
        {'$sort': {'_id.year': 1, '_id.month': 1}}
    ]

    try:
        cursor = collection.aggregate(pipeline)
        dataset = []
        labels = []

        current_month = dt_from

        async for doc in cursor:
            label_month = datetime(doc['_id']['year'], doc['_id']['month'], 1)

            while current_month < label_month:
                dataset.append(0)
                labels.append(current_month.isoformat())
                current_month = current_month.replace(
                    year=current_month.year + (current_month.month // 12),
                    month=((current_month.month % 12) + 1)
                )

            dataset.append(doc['total_amount'])
            labels.append(label_month.isoformat())
            current_month = current_month.replace(
                year=current_month.year + (current_month.month // 12),
                month=((current_month.month % 12) + 1)
            )

        while current_month <= dt_upto:
            dataset.append(0)
            labels.append(current_month.isoformat())
            current_month = current_month.replace(
                year=current_month.year + (current_month.month // 12),
                month=((current_month.month % 12) + 1)
            )

        return {'dataset': dataset, 'labels': labels}

    except Exception as e:
        print(f"Ошибка в ходе аггрегации: {e}")
        return {'dataset': [], 'labels': []}

# Функция агрегации зарплат по дням
async def aggregate_by_day(dt_from, dt_upto, collection):
    pipeline = [
        {'$match': {'dt': {'$gte': dt_from, '$lte': dt_upto}}},
        {'$group': {
            '_id': {'year': {'$year': '$dt'}, 'month': {'$month': '$dt'}, 'day': {'$dayOfMonth': '$dt'}},
            'total_amount': {'$sum': {'$ifNull': ['$value', 0]}}
        }},
        {'$sort': {'_id.year': 1, '_id.month': 1, '_id.day': 1}}
    ]

    cursor = collection.aggregate(pipeline)

    results = {
        datetime(doc['_id']['year'], doc['_id']['month'], doc['_id']['day']): doc['total_amount']
        for doc in await cursor.to_list(length=None)
    }

    dataset = []
    labels = []

    for period in generate_periods(dt_from, dt_upto, 'day'):
        dataset.append(results.get(period, 0))
        labels.append(period.isoformat())

    return {'dataset': dataset, 'labels': labels}

# Функция агрегации зарплат по часам
async def aggregate_by_hour(dt_from, dt_upto, collection):
    pipeline = [
        {'$match': {'dt': {'$gte': dt_from, '$lte': dt_upto}}},
        {'$group': {
            '_id': {'year': {'$year': '$dt'}, 'month': {'$month': '$dt'}, 'day': {'$dayOfMonth': '$dt'},
                    'hour': {'$hour': '$dt'}},
            'total_amount': {'$sum': {'$ifNull': ['$value', 0]}}
        }},
        {'$sort': {'_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.hour': 1}}
    ]

    cursor = collection.aggregate(pipeline)

    results = {
        f"{doc['_id']['year']}-{str(doc['_id']['month']).zfill(2)}-{str(doc['_id']['day']).zfill(2)}T{str(doc['_id']['hour']).zfill(2)}:00:00":
            doc['total_amount'] for doc in await cursor.to_list(length=None)}

    labels = [f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}T{str(hour).zfill(2)}:00:00" for year in
              range(dt_from.year, dt_upto.year + 1) for month in range(1, 13) for day in
              range(1, calendar.monthrange(year, month)[1] + 1) for hour in range(24)]
    dataset = [results.get(label, 0) for label in labels if dt_from <= datetime.fromisoformat(label) <= dt_upto]

    return {'dataset': dataset,
            'labels': [label for label in labels if dt_from <= datetime.fromisoformat(label) <= dt_upto]}

# Общая функция аггрегации для выбора типа группировки
async def aggregate_salaries(dt_from, dt_upto, group_type, collection):
    if group_type == 'month':
        return await aggregate_by_month(dt_from, dt_upto, collection)
    elif group_type == 'day':
        return await aggregate_by_day(dt_from, dt_upto, collection)
    elif group_type == 'hour':
        return await aggregate_by_hour(dt_from, dt_upto, collection)
    else:
        raise ValueError(f"Некорректный тип группировки group_type: {group_type}")

# Тестирование функции агрегации
async def test_aggregate():
    await collection.drop()

    # Загрузка данных из дампа
    data = load_bson('sample_collection.bson')
    metadata = load_metadata('sample_collection.metadata.json')

    # Вставка данных в MongoDB
    await insert_data(data)

    # Аггрегация и тестирование на примерах из тестового задания
    examples = [
        {
            "dt_from": "2022-09-01T00:00:00",
            "dt_upto": "2022-12-31T23:59:00",
            "group_type": "month"
        },
        {
            "dt_from": "2022-10-01T00:00:00",
            "dt_upto": "2022-11-30T23:59:00",
            "group_type": "day"
        },
        {
            "dt_from": "2022-02-01T00:00:00",
            "dt_upto": "2022-02-02T00:00:00",
            "group_type": "hour"
        }
    ]

    for example in examples:
        result = await aggregate_salaries(
            datetime.fromisoformat(example['dt_from']),
            datetime.fromisoformat(example['dt_upto']),
            example['group_type'],
            collection
        )

        print(f"Запрос: {example}")
        print(f"Ответ: {result}")
        print("-" * 50)

# Инициализация бота
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def send_welcome(message: Message):
    """
    Обработчик для сообщений с командами `/start` и `/help`

    await message.answer("Отправьте сюда JSON сообщение в следующем формате:\n"
                         "{\n"
                         "    \"dt_from\": \"YYYY-MM-DDTHH:MM:SS\",\n"
                         "    \"dt_upto\": \"YYYY-MM-DDTHH:MM:SS\",\n"
                         "    \"group_type\": \"month/day/hour\"\n"
                         "}")
    """
@dp.message()
async def aggregate_data(message: Message):
    """
    Обработчик для обработки и ответа на JSON сообщения
    """
    try:
        data = json.loads(message.text)
        dt_from = datetime.fromisoformat(data['dt_from'])
        dt_upto = datetime.fromisoformat(data['dt_upto'])
        group_type = data['group_type']

        result = await aggregate_salaries(dt_from, dt_upto, group_type, collection)

        dataset = [result['dataset']] if isinstance(result['dataset'], int) else result['dataset']
        labels = [result['labels']] if isinstance(result['labels'], str) else result['labels']

        response = json.dumps({
            "dataset": [result['dataset']],
            "labels": [result['labels']]
        })

        await message.reply(response, parse_mode=ParseMode.MARKDOWN)

    except json.JSONDecodeError:
        await message.reply("Неправильно введен JSON. Введите запрос в JSON формате.")
    except Exception as e:
        await message.reply(f"Ошибка в ходе выполнения запроса: {e}")

async def main():
    """
    Основная функция
    """
    bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())