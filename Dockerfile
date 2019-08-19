FROM jupyter/base-notebook:notebook-6.0.0

RUN mkdir /tmp/config
COPY Pipfile /tmp/config
COPY Pipfile.lock /tmp/config

WORKDIR /tmp/config
RUN pip install pipenv
RUN pipenv install --system

WORKDIR $HOME

# COPY ./src/ /lib/src
VOLUME ["/lib/src"]

ENV PYTHONPATH "${PYTHONPATH}:/lib/src"