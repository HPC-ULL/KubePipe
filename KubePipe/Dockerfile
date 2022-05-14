FROM python:3.9.12


RUN pip install dill 
RUN pip install scikit-learn
RUN pip install torch torchvision

RUN pip install minio

WORKDIR /usr/src/app

COPY PytorchWrapper.py /usr/src/app


