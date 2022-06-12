FROM python:3.10

WORKDIR /usr/app 

ADD main.py ./src

RUN pip3 install pandas

COPY zno ./zno

COPY src ./src

CMD ["python3", "main.py"]