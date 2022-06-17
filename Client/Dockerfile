FROM python

WORKDIR /Client

COPY . /Client

RUN pip install requests

EXPOSE 8081

CMD ["python" , "client.py"]