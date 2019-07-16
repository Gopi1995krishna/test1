FROM python:3

RUN pip3 install requests

ENV WD /migration
WORKDIR ${WD}
ADD functions.py ${WD}
ADD migrate.py ${WD}
ADD config.json ${WD}

CMD ["python", "migrate.py", "-h"]
