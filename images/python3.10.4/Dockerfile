FROM python:3.9.13-alpine3.16

COPY . KubePipe
RUN cd KubePipe && pip install . --no-deps
RUN pip install dill minio sklearn
