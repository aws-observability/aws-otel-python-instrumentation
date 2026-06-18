FROM python:3.10
WORKDIR /app
COPY requirements.txt /app/
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY lambda_function.py /app/
COPY amazon/ /app/amazon/

EXPOSE 8080

CMD ["python", "-u", "./lambda_function.py"]
