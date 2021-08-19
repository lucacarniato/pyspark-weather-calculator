FROM godatadriven/pyspark

COPY requirements.txt /opt/app/requirements.txt

RUN pip install -r /opt/app/requirements.txt

COPY weathercalculator job/weathercalculator

COPY setup.py job

WORKDIR job

RUN python setup.py sdist bdist_wheel

RUN pip install /job/dist/weathercalculator-0.0.0-py3-none-any.whl