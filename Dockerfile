#FROM godatadriven/pyspark

#COPY requirements.txt /opt/app/requirements.txt

#COPY weathercalculator/ /job/

#RUN pip install -r /opt/app/requirements.txt

#RUN ls -la /job/*

FROM alpine

CMD ["sleep","3600"]
