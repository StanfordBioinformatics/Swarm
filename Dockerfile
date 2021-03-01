FROM ubuntu:20.04

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt update && apt install -y maven openjdk-11-jre

COPY . /app
WORKDIR /app
RUN cd lib && sh install-lib.sh
RUN mvn package
EXPOSE 80/tcp

CMD ["java", "-jar", "target/swarm-1.0-SNAPSHOT.jar"]