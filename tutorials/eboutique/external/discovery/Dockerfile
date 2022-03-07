FROM python

RUN apt install git
RUN pip install minos-discovery==0.0.8

COPY config.yml ./config.yml
CMD ["discovery", "start", "config.yml"]
