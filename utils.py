import calendar
import random


def data_generator(year: int, month: int) -> str:
    days = calendar.monthrange(year, month)[1]
    return f'{year}-{month}-{random.randint(1, days)}'
