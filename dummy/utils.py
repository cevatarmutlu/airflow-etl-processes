from random import randint, random
from datetime import datetime
import numpy as np

def random_date_generator(start_year):
    return datetime(start_year, randint(1, 12), randint(1, 28), randint(0, 23), randint(1, 59), randint(1, 59), randint(0, 999999))

def generate_id_values(row_number):
    return np.arange(1, row_number + 1, 1)

def generate_date_values(start_year, row_number):
    return [random_date_generator(start_year) for i in range(row_number)]

def generate_price_values(row_number, price_start, price_stop, digit):
    return np.round(np.random.rand(row_number) * price_stop + price_start, digit)

def generate_status_values(row_number):
    return [1 if random() < 0.8 else 0 for i in range(row_number)]

def generate_name_values(row_number, name_prefix):
    numbers = np.arange(1, row_number + 1).astype(str)
    names = np.array([name_prefix for i in range(row_number)])
    return np.core.defchararray.add(names, numbers)

def generate_city_values(row_number):
    cities = np.array(["İstanbul", "İzmir", "Ankara", "Erzurum", "Muğla", "Adana", "Antalya"])
    return [np.random.choice(cities) for i in range(row_number)]