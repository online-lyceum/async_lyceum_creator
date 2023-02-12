import asyncio
from argparse import ArgumentParser
import datetime as dt
from typing import Optional
from time import monotonic

import aiohttp
import pandas as pd
import requests
from pydantic import BaseSettings, BaseModel


class Date(BaseModel):
    year: int
    month: int
    day: int

    class Config:
        orm_mode = True


class Semester(BaseModel):
    start_date: Date
    end_date: Date
    week_reverse: Optional[bool]


class Config(BaseSettings):
    postgres_host: str = 'localhost'
    postgres_user: str = 'postgres'
    postgres_password: str = 'password'
    postgres_db: str = 'db'


config = Config()
session = requests.Session()
async_session: Optional[aiohttp.ClientSession] = None

classes_cache = {}
subgroups_cache = {}
teachers_cache = {}

URL = None


def create_teacher(teacher: str) -> int:
    if not isinstance(teacher, str):
        teacher = ''
    if teacher in teachers_cache.keys():
        return teachers_cache[teacher]
    body = {'name': teacher}
    with session.get(f'{URL}/teachers', json=body) as resp:
        if resp.status_code == 200:
            for gotten_teacher in resp.json()['teachers']:
                if teacher == gotten_teacher['name']:
                    teachers_cache[teacher] = gotten_teacher['teacher_id']
                    return teachers_cache[teacher]
    with session.post(f'{URL}/teachers', json=body) as resp:
        teacher_id = resp.json()['teacher_id']
        teachers_cache[teacher] = teacher_id
    return teachers_cache[teacher]


def create_teachers(df: pd.DataFrame) -> pd.DataFrame:
    teacher_ids = []
    teachers = df['Teacher'].values
    for teacher in teachers:
        teacher_ids.append(create_teacher(teacher))
    df['TeacherID'] = teacher_ids
    return df


def create_class(school_id: int, number: int, letter: str):
    if (number, letter) in classes_cache.keys():
        return classes_cache[(number, letter)]
    body = {'number': number, 'letter': letter, 'school_id': school_id}
    with session.post(f'{URL}/classes',
                      json=body) as resp:
        res = resp.json()
        class_id = res['class_id']
        classes_cache[(number, letter)] = class_id
        return class_id


def create_school(name: str, address: str, is_university: bool):
    body = {
        'name': name,
        'address': address,
        'is_university': is_university
    }
    with session.post(f'{URL}/schools', json=body) as resp:
        json_resp = resp.json()
        return json_resp['school_id']


def parse_table(file: str):
    if file.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        raise ValueError('Unsupported file type')
    return df


def create_classes(school_id: int, df: pd.DataFrame):
    class_ids = []
    classes = df[['ClassNumber', 'ClassLetter']].values
    for number, letter in classes:
        class_ids.append(create_class(
            school_id,
            number,
            letter
        ))
    df['ClassID'] = class_ids
    return df


def create_subgroup(class_id: int, subgroup: str):
    if (class_id, subgroup) in subgroups_cache.keys():
        return subgroups_cache[(class_id, subgroup)]
    body = {'name': subgroup, 'class_id': class_id}
    with session.post(f'{URL}/subgroups', json=body) as resp:
        subgroup_id = resp.json()['subgroup_id']
        subgroups_cache[(class_id, subgroup)] = subgroup_id
        return subgroup_id


def create_subgroups(df: pd.DataFrame):
    subgroups = df[['ClassID', 'Subgroup']].values
    subgroup_ids = [create_subgroup(class_id, subgroup)
                    for class_id, subgroup in subgroups]
    df['SubgroupID'] = subgroup_ids
    return df


async def create_lesson(lesson, school_id: int, semester_id: int):
    lesson_name = str(lesson.LessonName)
    try:
        room = f"Кабинет №{int(lesson.Room)}"
    except ValueError:
        room = ""
    body = {
        'name': lesson_name[0].upper() + lesson_name[1:],
        'start_time': {
            'hour': int(lesson.StartHour),
            'minute': int(lesson.StartMinute)
        },
        'end_time': {
            'hour': int(lesson.EndHour),
            'minute': int(lesson.EndMinute)
        },
        'is_odd_week': int(lesson.Week),
        'weekday': int(lesson.Weekday),
        'room': room,
        'school_id': int(school_id),
        'teacher_id': int(lesson.TeacherID),
        'semester_id': semester_id
    }
    async with async_session.post(f'{URL}/lessons', json=body) as resp:
        res = await resp.json()
        lesson_id = res['lesson_id']
    body = {'lesson_id': lesson_id, 'subgroup_id': int(lesson.SubgroupID)}
    async with async_session.post(f'{URL}/lessons/subgroups',
                                  json=body) as resp:
        assert resp.status // 100 == 2


async def create_lessons(df: pd.DataFrame, school_id: int, semester_id: int):
    global async_session
    async_session = aiohttp.ClientSession()
    async with asyncio.TaskGroup() as tg:
        for lesson in df.iloc:
            tg.create_task(create_lesson(lesson, school_id, semester_id))
    await async_session.close()


class ApiException(Exception):
    pass


def create_semester(
        school_id: int,
        semester: Semester
) -> int:
    body = semester.dict()
    body['school_id'] = school_id
    with session.post(f'{URL}/semesters', json=body) as resp:
        if resp.status_code // 100 != 2:
            text = f'Semester creating return {resp.status_code}'
            raise ApiException(text)
        return resp.json()['semester_id']


async def create_table_for_school(
        file: str,
        name: str,
        address: str,
        is_university: bool,
        semester: Semester
):
    print(f'Start creating lessons for {name}')
    start_time = monotonic()

    df: pd.DataFrame = parse_table(file)
    school_id = create_school(name, address, is_university)
    df = create_teachers(df)
    df = create_classes(school_id, df)
    df = create_subgroups(df)
    semester_id = create_semester(school_id, semester)
    # semester_id = 2
    await create_lessons(df, school_id, semester_id)

    print("Creating time:", monotonic() - start_time)


def create_all():
    global URL
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', default='http://127.0.0.1:8080/api')
    args = parser.parse_args()
    URL = args.host
    print(f"URL is {URL}")
    schools = [
        {
            'file': './timetables/lyceum2_3.csv',
            'name': 'Лицей №2',
            'address': 'Иркутск, пер. Волконского, 7',
            'is_university': False,
            'semester':
                Semester(
                    start_date=dt.date(2023, 1, 9),
                    end_date=dt.date(2023, 5, 31),
                    week_reverse=False
                )
         }
    ]

    for school in schools:
        asyncio.run(create_table_for_school(**school))


if __name__ == "__main__":
    create_all()
