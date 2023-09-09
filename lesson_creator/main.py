import asyncio
import random
from argparse import ArgumentParser
from time import monotonic
import calendar
import datetime as dt

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
subgroups_cache = {}
cal = calendar.Calendar()

lessons_begin_date = dt.date(dt.datetime.now().year, 9, 1)
lessons_end_date = dt.date(lessons_begin_date.year + 1, 5, 31)
lessons_end_date = dt.date(lessons_begin_date.year, 9, 15)


def urljoin(*args):
    return "/".join(map(lambda x: str(x).rstrip('/'), args))


def parse_table(file: str):
    if file.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        raise ValueError('Unsupported file type')
    return df


async def create_school(name: str, address: str, session: aiohttp.ClientSession):
    resp = await session.post(urljoin(URL, 'schools'),
                              json={'name': name, 'address': address})
    resp.raise_for_status()
    return (await resp.json())['id']


async def create_group(school_id, number, letter,
                       session: aiohttp.ClientSession):
    body = {'letter': letter, 'number': number, 'school_id': school_id}
    resp = await session.post(urljoin(URL, 'groups'), json=body)
    assert resp.status in (201, 409)
    if resp.status == 201:
        return (await resp.json())['id'], number, letter
    return None, number, letter


async def create_groups(school_id: int, df: pd.DataFrame,
                        session: aiohttp.ClientSession) -> pd.DataFrame:
    logger.info("Creating groups")
    groups = df[['ClassNumber', 'ClassLetter']].values
    cache = {}
    for number, letter in groups:
        if (number, letter) in cache.keys():
            continue
        cache[(number, letter)] = asyncio.create_task(create_group(school_id, number, letter, session))
    done, _ = await asyncio.wait(cache.values(), return_when=asyncio.ALL_COMPLETED)
    cache = {(g_id, sg): sg_id for sg_id, g_id, sg in [task.result() for task in done]}
    df['GroupID'] = [cache.get((num, ltr)) for num, ltr in groups]
    return df


async def create_subgroup(group_id: int, subgroup: str,
                          session: aiohttp.ClientSession):
    body = {'name': subgroup, 'group_id': group_id}
    async with await session.post(urljoin(URL, 'subgroups'), json=body) as resp:
        return (await resp.json())['id'], group_id, subgroup


async def create_subgroups(df: pd.DataFrame, session):
    logger.info("Creating subgroups")
    subgroups = df[['GroupID', 'Subgroup']].values
    cache = {}
    for group_id, subgroup in subgroups:
        if (group_id, subgroup) in cache.keys():
            continue
        cache[(group_id, subgroup)] = asyncio.create_task(create_subgroup(group_id, subgroup, session))
    done, _ = await asyncio.wait(cache.values(), return_when=asyncio.ALL_COMPLETED)
    cache = {(g_id, sg): sg_id for sg_id, g_id, sg in [task.result() for task in done]}
    df['SubgroupID'] = [cache.get((group_id, subgroup)) for group_id, subgroup in subgroups]
    return df


async def create_lesson(lesson, session):
    lesson_name = str(lesson.LessonName)
    start_time = dt.time(int(lesson.StartHour), int(lesson.StartMinute))
    end_time = dt.time(int(lesson.EndHour), int(lesson.EndMinute))
    first_date = [day for day in (lessons_begin_date + dt.timedelta(n) for n in range(7))
                  if day.weekday() == int(lesson.Weekday)][0]
    try:
        room = f"Кабинет №{int(lesson.Room)}"
    except ValueError:
        room = ""
    for date in (first_date + dt.timedelta(7 * n)
                 for n in range(1, (lessons_end_date - lessons_begin_date).days // 7 + 1)):
        body = {
            'name': lesson_name[0].upper() + lesson_name[1:],
            'room': room,
            'teacher': '',
            'start_dt': dt.datetime.combine(date, start_time).isoformat(),
            'end_dt': dt.datetime.combine(date, end_time).isoformat(),
            'breaks': []
        }
        async with await session.post(urljoin(URL, 'lessons'), json=body) as resp:
            lesson_id = (await resp.json())['id']
        async with await session.post(urljoin(URL, 'lessons',
                                              str(lesson_id), 'subgroup', int(lesson.SubgroupID))) as resp:
            assert resp.status == 200, f'Error creating lesson: {resp.status} {await resp.text()}'


async def create_lessons(df: pd.DataFrame, session):
    logger.info("Creating lessons")
    async with asyncio.TaskGroup() as tg:
        for lesson in df.iloc:
            tg.create_task(create_lesson(lesson, session))


async def create_school_table(file: str, name: str, address: str,
                              session: aiohttp.ClientSession):
    logger.info(f'Creating school {name}')
    start_time = monotonic()

    df: pd.DataFrame = parse_table(file)
    school_id = await create_school(name, address, session)
    df = await create_groups(school_id, df, session)
    df = await create_subgroups(df, session)
    await create_lessons(df, session)
    await session.close()

    logger.info(f'Creating time: {monotonic() - start_time}')


async def create_all():
    global URL, auth_token
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', default='http://127.0.0.1:8080/api')
    parser.add_argument('-t', '--token', default='123456')
    args = parser.parse_args()
    URL = args.host
    auth_token = args.token
    # session.headers = {'auth-token': auth_token}

    async with asyncio.TaskGroup() as tg:
        for school in schools:
            async_session = aiohttp.ClientSession(
                headers={'auth-token': auth_token}
            )
            # async_session = S()
            tg.create_task(create_school_table(**school, session=async_session))


if __name__ == '__main__':
    asyncio.run(create_all())