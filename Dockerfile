# pull a Python 3.12 version
FROM python:3.12.1

# install GDAL and the matching pathon bindings
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev && \
    pip install --upgrade pip && \
    pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# install everything
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    rm requirements.txt

# create a folder for the application
RUN mkdir /app
COPY ./processor /app/processor
COPY ./run.py /app/run.py
COPY ./app.py /app/app.py

# set the working directory
WORKDIR /app

# run the application
CMD ["python", "app.py"]