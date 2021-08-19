FROM godatadriven/pyspark

# Create app directory
RUN mkdir -p /usr/src/app

# change working dir to /usr/src/app
WORKDIR /usr/src/app

# map volumns
VOLUME . /usr/src/app

CMD ["pyspark" , "--version" ]