FROM python:3.10-slim

WORKDIR /streamlit-app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY streamlit-app/requirements.txt /streamlit-app/requirements.txt
COPY config /streamlit-app/config
COPY streamlit-app /streamlit-app

RUN pip3 install -r requirements.txt


EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
