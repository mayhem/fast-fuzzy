FROM ghcr.io/abebus/free-threaded-python-docker-image:main

RUN apt search postgresql-server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
                       build-essential \
		       postgresql-server-dev-15 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /code
WORKDIR /code

RUN pip install --upgrade pip && pip install setuptools uwsgi

RUN mkdir /code/fuzzy
WORKDIR /code/fuzzy

COPY requirements.txt /code/fuzzy
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get purge -y build-essential && \
    apt-get autoremove -y && \
    apt-get clean -y

# Now install our code, which may change frequently
COPY . /code/fuzzy

CMD uwsgi --gid=www-data --uid=www-data --http-socket :3031 \
          --vhost --module=server --callable=app --chdir=/code/fuzzy \
          --enable-threads --processes=8
