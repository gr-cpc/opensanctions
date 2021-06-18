FROM python

WORKDIR /code

RUN apt-get update &&\
	pip install psycopg2 &&\
	pip install elastic-enterprise-search
	
# CMD ["tail", "-f", "/dev/null"]
CMD ["python", "-u", "connector.py"]