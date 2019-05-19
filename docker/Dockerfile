FROM hysds/usgs-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Ingest products from metadata"

RUN sudo yum -y install unzip

USER ops

COPY . /home/ops/verdi/ops/eo1_hyperion

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

