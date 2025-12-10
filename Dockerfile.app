# Dockerfile
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    postgresql-client \
    wget \
    curl \
    vim \
	collectl \
    time \
    python3.10 \
    python3-pip \
    # We need this to connect to Spark
	openjdk-17-jre \
	maven \
    && rm -rf /var/lib/apt/lists/*

COPY automations/requirements.txt .

RUN pip install -r requirements.txt

# Build Spark extensions. We need to build these in `app` and not in the spark
# container because .config('spark.jars') expects to find them in the local
# file system.

COPY engines/pyspark/udfs/scalar/extractmonth_java/ /tmp/extractmonth_java/
WORKDIR /tmp/extractmonth_java
RUN mvn clean && mvn clean package

COPY engines/pyspark/udfs/scalar/extractday_scala/ /tmp/extractday_scala/
WORKDIR /tmp/extractday_scala
RUN mvn clean && mvn clean package

COPY engines/pyspark/udfs/aggregate/aggregate_avg_scala /tmp/aggregate_avg_scala/
WORKDIR /tmp/aggregate_avg_scala
RUN mvn clean && mvn clean package

COPY engines/pyspark/udfs/table/extractfromdate_scala /tmp/extractfromdate_scala/
WORKDIR /tmp/extractfromdate_scala
RUN mvn clean && mvn clean package

WORKDIR /jars

# Make jars available
RUN cp /tmp/extractmonth_java/target/JavaUDFjarfile.jar /jars/
RUN cp /tmp/extractday_scala/target/ScalaUDFjarfile.jar /jars/
RUN cp /tmp/aggregate_avg_scala/target/ScalaUDFjarfileAggregate.jar /jars/
RUN cp /tmp/extractfromdate_scala/target/ScalaUDFjarfileTable.jar /jars/

# Clean up build artifacts
RUN rm -rf /tmp/extractmonth_java
RUN rm -rf /tmp/extractday_scala
RUN rm -rf /tmp/aggregate_avg_scala
RUN rm -rf /tmp/extractfromdate_scala

WORKDIR /app

# Set a default command otherwise the container will exit immediately.
CMD ["tail", "-f", "/dev/null"]
