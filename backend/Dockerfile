FROM python:3.13
RUN mkdir /app
COPY requirements.txt /app
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt --no-cache-dir
COPY . /app
WORKDIR /app/app