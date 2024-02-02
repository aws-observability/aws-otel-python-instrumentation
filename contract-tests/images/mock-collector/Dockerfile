FROM python:3.9-slim
WORKDIR /mock-collector
COPY . /mock-collector

RUN pip install -r requirements.txt

# Without `-u`, logs will be buffered and `wait_for_logs` will never return.
CMD ["python", "-u", "./mock_collector_server.py"]