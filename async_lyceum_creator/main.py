from argparse import ArgumentParser
import requests
import pandas as pd


URL = 'http://localhost:8080/api'
session = requests.Session()
classes_cache = {}
subgroups_cache = {}


def create_class(school_id: int, number: int, letter: str):
    if (number, letter) in classes_cache.keys():
        return classes_cache[(number, letter)]
    body = {'number': number, 'letter': letter}
    with session.post(f'{URL}/school/{school_id}/class',
                       json=body) as resp:
        class_id = resp.json()['class_id']
        classes_cache[(number, letter)] = class_id
        return class_id


def create_school(name: str, city: str, place: str):
    body = {'name': name, 'city': city, 'place': place}
    with session.post(f'{URL}/school', json=body) as resp:
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
    body = {'name': subgroup}
    with session.post(f'{URL}/class/{class_id}/subgroup',
                       json=body) as resp:
        subgroup_id = resp.json()['subgroup_id']
        subgroups_cache[(class_id, subgroup)] = subgroup_id
        return subgroup_id


def create_subgroups(df: pd.DataFrame):
    subgroups = df[['ClassID', 'Subgroup']].values
    subgroup_ids = [create_subgroup(class_id, subgroup)
                    for class_id, subgroup in subgroups]
    df['SubgroupID'] = subgroup_ids
    return df


def create_lesson(lesson, school_id: int):
    lesson_name = str(lesson.LessonName)
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
        'week': int(lesson.Week),
        'weekday': int(lesson.Weekday),
        'teacher_id': int(lesson.TeacherID)
    }
    status = 0
    with session.post(f'{URL}/school/{school_id}/lesson', json=body) as resp:
                lesson_id = resp.json()['lesson_id']
    body = {'lesson_id': lesson_id}
    with session.post(f'{URL}/subgroup/{lesson.SubgroupID}/lesson',
                       json=body) as resp:
        assert resp.status_code // 100 == 2


def create_lessons(df: pd.DataFrame, school_id: int):
    c = df.shape[0] // 20 
    for lesson in df.iloc:
        create_lesson(lesson, school_id)
    return df


def create_table_for_school(file: str, name: str, city: str, place: str):
    print(f'Start creating lessons for {name}')
    df: pd.DataFrame = parse_table(file)
    school_id = create_school(name, city, place)
    df = create_classes(school_id, df)
    df = create_subgroups(df)
    create_lessons(df, school_id)


def create_all():
    global URL
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', default='http://127.0.0.1:8080/api')
    args = parser.parse_args()
    URL = args.host
    schools = [
        {'file': './timetables/lyceum_2.csv',
         'name': 'Лицей №2', 'city': 'Иркутск', 'place': 'пер. Волконского, 7'}
    ]

    for school in schools:
        create_table_for_school(**school)


if __name__ == "__main__":
    create_all()
