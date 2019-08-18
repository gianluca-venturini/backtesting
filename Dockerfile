FROM jupyter/base-notebook:notebook-6.0.0


COPY Pipfile ~/
COPY Pipfile.lock ~/

WORKDIR ~/
RUN pip install pipenv
RUN pipenv install

# COPY ./src/ /lib/src
VOLUME ["/lib/src"]

ENV PYTHONPATH "${PYTHONPATH}:/lib/src"