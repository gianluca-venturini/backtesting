FROM jupyter/base-notebook:notebook-6.0.0

RUN pip install pandas matplotlib requests

COPY ./src/ /lib/src

ENV PYTHONPATH "${PYTHONPATH}:/lib/src"