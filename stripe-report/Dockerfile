FROM python:3.9-slim AS compile-image

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential gcc

RUN python3 -m venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install -r requirements.txt

FROM python:3.9-slim AS build-image

LABEL MAINTAINER="Cloud Team <cloud@limepay.com.au>"

COPY --from=compile-image /opt/venv /opt/venv

# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY report-run.py /

CMD ["python3", "/report-run.py"]
