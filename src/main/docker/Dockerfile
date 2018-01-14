FROM softsky/spark
MAINTAINER SOFTSKY Arsen A. Gutsal <a.gutsal@softsky.com.ua>

# Copy start script
COPY src/main/docker/start-ui /opt/util/bin/start-ui
RUN chmod +x /opt/util/bin/start-ui

ENV PATH=$PATH:/opt/util/bin

# add jar
ADD target/people-match-ai-ui-0.1.0.jar /app.jar

RUN useradd -ms /bin/bash spark

USER spark

EXPOSE 8888
