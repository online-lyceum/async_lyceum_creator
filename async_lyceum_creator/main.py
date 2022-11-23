from argparse import ArgumentParser
import asyncio
import aiohttp
import os
import json
import pandas as pd
import numpy as np


URL = 'https://test-async-api.lava-land.ru'


async def create_class(school_id: int, number: int, letter: str,
                       session: aiohttp.ClientSession):
    body = {'number': number, 'letter': letter}
    json_body = json.dumps(body, ensure_ascii=False)
    async with session.post(f'{URL}/school/{school_id}/class',
                            body=json_body) as resp:
        return (await resp.json())['class_id']


async def create_school(name: str, address: str,
                        session: aiohttp.ClientSession):
    body = {'name': name, 'address': address}
    json_body = json.dumps(body, ensure_ascii=False)
    async with session.post('/school', body=json_body) as resp:
        return (await resp.json())['school_id']


async def parse_table(file: str):
    if file.endswith('.xls'):
        df = pd.read_excel(file)
    else:
        raise ValueError('Unsupported file type')

    # TODO: paste DataFrame transforms

    return df


async def create_classes(school_id: int, df: pd.DataFrame,
                         session: aiohttp.ClientSession):
    class_id_tasks = []
    async with asyncio.TaskGroup() as tg:
        classes = np.unique(df[['ClassNumber', 'ClassLetter']].values, axis=0)
        for number, letter in classes:
            task = tg.create_task(create_class(
                school_id,
                number,
                letter,
                session
            ))
            class_id_tasks.append(task)
    class_ids = [x.result() for x in class_id_tasks]
    df['ClassID'] = class_ids
    return df


async def create_subgroup(class_id: int, subgroup: str,
                          session: aiohttp.ClientSession):
    body = {'name': subgroup}
    json_body = json.dumps(body, ensure_ascii=False)
    with session.post(f'{URL}/class/{class_id}/subgroup',
                      body=json_body) as resp:
        return (await resp.json())['subgroup_id']


async def create_subgroups(df: pd.DataFrame, session: aiohttp.ClientSession):
    tasks = []
    with asyncio.TaskGroup() as tg:
        subgroups = np.unique(df[['ClassID', 'Subgroup']].values, axis=0)
        for class_id, subgroup in subgroups:
            task = tg.create_task(create_subgroup(class_id, subgroup, session))
            tasks.append(task)
    subgroup_ids = [x.result() for x in tasks]
    df['SubgroupID'] = subgroup_ids
    return df


async def create_lesson(lesson: np.ndarray, session: aiohttp.ClientSession):
    pass


async def create_lessons(df: pd.DataFrame, session: aiohttp.ClientSession):
    tasks = []
    with asyncio.TaskGroup() as tg:
        for lesson in df.values:
            task = tg.create_task(create_lesson(lesson, session))
            tasks.append(task)

    return df


async def create_table_for_school(file: str, name: str, address: str):
    print(f'Start creating lessons for {name}')
    df: pd.DataFrame = await parse_table(file)
    async with aiohttp.ClientSession() as session:
        school_id = await create_school(name, address, session)
        df = await create_classes(school_id, df, session)
        df = await create_subgroups(df, session)
        await create_lessons(df, session)


def create_all():
    parser = ArgumentParser()
    parser.add_argument('-p', '--path', default='./timetables')
    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    tasks = []
    schools = [
        {'file': os.path.join(args.path, 'lyceum_2'),
         'name': 'Лицей №2', 'address': 'Иркутск, ул. Волконского'}
    ]
    for school in schools:
        tasks.append(create_table_for_school(**school))
    loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == "__main__":
    create_all()
