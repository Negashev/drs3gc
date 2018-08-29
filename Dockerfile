FROM alpine

CMD ["python3", "-um", "japronto", "run.app"]

WORKDIR /src

EXPOSE 80

RUN apk add --update python3
RUN apk add --no-cache --virtual .build-deps build-base python3-dev py3-pip \
    && pip3 --no-cache install \
    apscheduler \
    minio \
    www_authenticate \
    requests \
    https://github.com/squeaky-pl/japronto/archive/master.zip \
	&& apk del .build-deps \
	&& rm -rf /var/cache/apk/*

ADD *.py ./