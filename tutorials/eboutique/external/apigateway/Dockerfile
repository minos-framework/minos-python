FROM python

RUN pip install minos-apigateway==0.1.0

COPY config.yml ./config.yml
CMD ["api_gateway", "start", "config.yml"]
