FROM godatadriven/pyspark

COPY requirements.txt /opt/app/requirements.txt

RUN pip install -r /opt/app/requirements.txt

COPY weathercalculator/ /job/weathercalculator


#FROM alpine

#RUN mkdir job

#COPY weathercalculator/ /job/weathercalculator

#CMD ["sleep","3600"]
