FROM godatadriven/pyspark

# extra Python requirements
COPY requirements.txt /opt/app/requirements.txt

RUN pip install -r /opt/app/requirements.txt

# copy Python files required for building the weathercalculator package
COPY weathercalculator job/weathercalculator

COPY setup.py job

# change the working directory
WORKDIR job

# build the weathercalculator package
RUN python setup.py sdist bdist_wheel

# install the package
RUN pip install /job/dist/weathercalculator-0.0.0-py3-none-any.whl