FROM python:3.10
COPY requirements.txt .
RUN pip install -q -r requirements.txt
COPY src/metric_calculation /src/metric_calculation
COPY src/__init__.py /src/__init__.py
COPY config /config
CMD ["python", "/src/metric_calculation/metrics.py"]
