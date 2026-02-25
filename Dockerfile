FROM apache/spark:3.5.1
USER root
RUN pip3 install pandas pyarrow>=4.0.0
USER spark
