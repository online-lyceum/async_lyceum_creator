from argparse import ArgumentParser
import asyncio
import aiohttp
import os
import json


async def create_table_data(file: str):
    print(f'Start creating lessons from {file}')
    async with aiohttp.ClientSession() as session:
        data = json.dumps({'number': 10, 'letter': 'А'},
                          ensure_ascii=False)
        async with session.post('http://0.0.0.0:8002/school/1/class', 
                                data=data) as resp:
            print(resp.status)
            print(await resp.json())
        data = json.dumps(
                {
                    'name': 'Новый урок', 
                    'start_time': [9, 15], 
                    'end_time': [9, 55], 
                    'weekday': 0, 
                    'week': None, 
                    'teacher_id': 1, 
                },
                ensure_ascii=False
            )
        async with session.post('http://0.0.0.0:8002/class/1/lesson', 
                                data=data) as resp:
            print(resp.status)
            print(await resp.json())


def create_all():
    parser = ArgumentParser()
    parser.add_argument('-p', '--path', default='./timetables')
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    tasks = []
    for root, dirs, files in os.walk(args.path):
        for file in files:
            tasks.append(create_table_data(file))
    loop.run_until_complete(asyncio.gather(*tasks))



