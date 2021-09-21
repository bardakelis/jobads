FROM python:3.7.7-slim-buster
ARG RSA_KEY=default
ARG DB_USER=default
ARG DB_PASSWD=default

ENV MONGODB_USER=$DB_USER
ENV MONGODB_PASSWD=$DB_PASSWD

# Time zone setting:
ENV TZ=Europe/Vilnius
# Install time zone stuff
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
# create dirs
    mkdir -p /opt/itdarborinka_app/application/ && mkdir -p /opt/itdarborinka_app/hugo_tmp/ && \
# add non-root user
    useradd -ms /bin/bash appuser

WORKDIR /opt/itdarborinka_app/application
#COPY extras/requirements.txt ./
#COPY extras/chrome/google-chrome-stable_current_amd64.deb ./
COPY extras/requirements.txt extras/chrome/google-chrome-stable_current_amd64.deb ./

RUN apt-get update && apt-get install -f -y ./google-chrome-stable_current_amd64.deb && rm -rf ./google-chrome-stable_current_amd64.deb && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get install -y tesseract-ocr tesseract-ocr-lit tesseract-ocr-eng && apt-get install -y git && \
    mkdir -p /home/appuser/.ssh && ssh-keyscan -t rsa github.com >> /home/appuser/.ssh/known_hosts && \
    echo "$RSA_KEY" > /home/appuser/.ssh/id_rsa && chmod 600 /home/appuser/.ssh/id_rsa && \
    chown -R appuser /opt/itdarborinka_app/ /home/appuser && rm -rf /var/cache/apk/*

COPY jobads  ./

USER appuser
RUN git config --global user.name "IT Darbo Rinka" && git config --global user.email "itdarborinka@gmail.com"
WORKDIR /opt/itdarborinka_app/application
CMD ["python3", "./script.py"]

