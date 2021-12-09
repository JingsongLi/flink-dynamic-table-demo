FROM openjdk:11
COPY . /usr/src/datagen
WORKDIR /usr/src/datagen
CMD java -cp datagen.jar org.apache.datagen.DataGen $DB_HOST $DB_PORT
