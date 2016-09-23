FROM centos

RUN yum update -y && \
    yum install -y java-1.8.0-openjdk-devel && \
    yum install -y wget && \
    wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo && \
    yum -y install apache-maven

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

WORKDIR /root/hadoop

RUN git clone git@github.com:baikai/hadoop_samples.git && \
    cd hadoop_samples && \
    mvn clean package

ENTRYPOINT ["/bin/bash"]
