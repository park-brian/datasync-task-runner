FROM quay.io/centos/centos:stream8

RUN dnf -y update \
 && dnf -y install \
    epel-release \
    python3 \
 && dnf clean all

RUN pip3 install boto3

RUN mkdir /deploy

WORKDIR /deploy

COPY run-task.py templates /deploy

RUN chmod +x run-task.py

CMD ./run-task.py -c config.py