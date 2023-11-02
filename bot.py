import asyncio
from datetime import datetime, timedelta
import logging
import os
import sys

from aiogram import Bot, Dispatcher, types

import motor.motor_asyncio
from bson.json_util import dumps, loads


dp = Dispatcher()

# Присваиваем переменные из окружения
# Получаем токен бота
TOKEN = os.getenv("BOT_TOKEN")
# Получаем строку аутентификации
MONGO_AUTH = os.getenv("MONGO_AUTH")
# Получаем название базы данных в MongoDB
MONGO_DB = os.getenv("MONGO_DB")
# Получаем название коллекции в базе
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")



# Инициализируем базу MongoDB
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_AUTH)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]


def add_month(start_date):
    """Добавляет месяц к текущей дате

    Args:
        start_date: дата
    """
    # Если текущий месяц - декабрь, увеличиваем год и устанавливаем месяц январем
    if start_date.month == 12:
        next_month = start_date.replace(year=start_date.year + 1, month=1)
    else:
        next_month = start_date.replace(month=start_date.month + 1)
    return next_month

def generate_delta(dt_from, dt_upto, group_type):
    """Генерирует список временных точек, в диапазоне от dt_from до dt_upto, в зависимости от group_type

    Args:
        dt_from: Дата и время старта агрегации
        dt_upto: Дата и время окончания агрегации
        group_type (str): тип агрегации (hour, day, month)

    Returns:
        list: Список временных точек в указанном промежутке
    """

    # Определяем метки времени начала и конца
    start = datetime.fromisoformat(dt_from)
    end = datetime.fromisoformat(dt_upto)
    times = [start.strftime("%Y-%m-%dT%H:00:00")]
    
    while start <= end:  # Изменено с < на <=
        if group_type == 'hour':
            start += timedelta(hours=1)
        elif group_type == 'day':
            start += timedelta(days=1)
        else:
            # Ввиду отсутсвия у timedelta атрибута months - напишем функцию сами
            start = add_month(start)
        
        if start <= end:
            times.append(start.strftime("%Y-%m-%dT%H:00:00"))
    
    return times


@dp.message()
async def handle_json(message: types.Message):
    """Обрабатывает текстовое сообщение как JSON

    Args:
        message (types.Message): текстовое сообщение
    """

    # Проверка текста сообщения на json-овость
    try:
        data = loads(message.text)
    except:
        return
    
    if not all(key in data for key in ['dt_from', "dt_upto", 'group_type']):
        await message.answer("Неверно составлен запрос!")
        return
    
    # Получение значения ключей
    dt_from = data.get('dt_from')
    dt_upto = data.get('dt_upto')
    group_type = data.get('group_type')


    # Устанавливаем формат времени в зависимости от типа группировки
    if group_type == 'hour':
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'month':
        date_format = "%Y-%m-01T00:00:00"
    else:
        await message.answer("Поддерживаемые типы группировок: 'hour' (час), 'day' (день), 'month' (месяц)")
        return

    # Создаем запрос к MongoDB
    query = [
        # Выбираем данные, которые соответсвуют указанному временному диапазону
        {
            "$match": {
                "dt": {"$gte": datetime.fromisoformat(dt_from),
                       "$lte": datetime.fromisoformat(dt_upto)}
                }
        },
        # Группируем эти данные по заданному формату времени и считаем сумму значений в каждой группе
        {
            "$group": {
                "_id": {"$dateToString": {"format": date_format, "date": "$dt"}},
                "count": {"$sum": "$value"}
            }
        },
        # Сортируем результаты по времени (по возрастанию)
        {"$sort": {"_id": 1}}
    ]

    # Выполняем запрос и преобразуем в python List (length устанавливает максимальное кол-во выводимых результатов)
    result = await collection.aggregate(query).to_list(length=999999)

    # Для удобства использования преобразуем результат в словарь python
    result_dict = {item['_id']: item['count'] for item in result}

    # Получаем временные метки из данного диапазона
    times = generate_delta(dt_from, dt_upto, group_type)

    # Заполняем недостающие значения нулями
    dataset = [result_dict.get(timestamp, 0) for timestamp in times] 

    response = {
        'dataset': dataset,
        'labels': times
    }

    await message.answer(dumps(response))


async def main():
    # Инициализируем бота
    bot = Bot(TOKEN)
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logging.log(logging.ERROR, f'Ошибка во время работы бота: {e}')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())