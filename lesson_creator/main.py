import asyncio
from argparse import ArgumentParser
from time import monotonic

import aiohttp
import pandas as pd
import requests
from loguru import logger

schools = [
    {
        'file': './timetables/lyceum2_3.csv',
        'name': 'Лицей №2',
        'address': 'Иркутск, пер. Волконского, 7'
    }
]
URL = ''
groups_cache = {}
subgroups_cache = {}


def urljoin(*args):
    return "/".join(map(lambda x: str(x).rstrip('/'), args))


def parse_table(file: str):
    if file.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        raise ValueError('Unsupported file type')
    return df


async def create_school(name: str, address: str, session: aiohttp.ClientSession):
    body = {
        'name': name,
        'address': address,
    }
    resp = await session.post(urljoin(URL, 'schools'), json=body)
    resp.raise_for_status()
    return (await resp.json())['id']


async def create_group(school_id, number, letter,
                       session):
    if (number, letter) in groups_cache.keys():
        return groups_cache[(number, letter)]
    body = {'letter': letter, 'number': number, 'school_id': school_id}
    resp = await session.post(urljoin(URL, 'classes'), json=body)
    resp.raise_for_status()
    class_id = (await resp.json())['id']
    groups_cache[(number, letter)] = class_id
    return class_id


async def create_groups(school_id: int, df: pd.DataFrame) -> pd.DataFrame:
    classes = df[['ClassNumber', 'ClassLetter']].values
    tasks = [asyncio.create_task(create_group(school_id, num, ltr))
             for num, ltr in classes]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    df['ClassID'] = [task.result() for task in done]
    return df


async def create_subgroup(class_id: int, subroup: str):
    if 


async def create_subgroups(df: pd.DataFrame):
    subgroups = df[['ClassID', 'Subgroup']].values
    


async def create_school_table(file: str, name: str, address: str,
                              session: aiohttp.ClientSession):
    logger.info(f'Creating school {name}')
    start_time = monotonic()

    df: pd.DataFrame = parse_table(file)
    school_id = await create_school(name, address, session)
    df = await create_groups(school_id, df)
    df = await create_subgroups(df)


async def create_all():
    global URL, auth_token
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', default='http://127.0.0.1:8080/api')
    parser.add_argument('-t', '--token', default='123456')
    args = parser.parse_args()
    URL = args.host
    auth_token = args.token
    session.headers = {'auth-token': auth_token}

    async with asyncio.TaskGroup() as tg:
        for school in schools:
            async_session = aiohttp.ClientSession(
                headers={'auth-token': auth_token}
            )
            tg.create_task(create_school_table(**school, session=async_session))
