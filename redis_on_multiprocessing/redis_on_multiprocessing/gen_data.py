import random
import datetime
from typing import Optional

from pydantic import EmailStr
from redis_om import HashModel
from redis_on_multiprocessing.libs.Python_Random_Name_Generator import random_names as rn


class Customer(HashModel):
    first_name: str
    last_name: str
    email: EmailStr
    join_date: datetime.date
    age: int


def generate_message():
    first = rn.First()
    last = rn.Last()
    return Customer(
        first_name=first,
        last_name=last,
        email=f'{first}.{last}@gmail.com',
        join_date=datetime.date.today(),
        age=random.randint(1, 100),
    )


if __name__ == '__main__':
    person = generate_message()
    print(f'Person: {person}')