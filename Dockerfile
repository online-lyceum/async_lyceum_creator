FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY setup.py ./
COPY async_lyceum_creator ./
RUN pip3 install .

COPY timetables ./

CMD create_lessons -H ${HOST}
