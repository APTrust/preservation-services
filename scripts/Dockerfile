FROM python:2.7-alpine3.11

WORKDIR /FIDO

RUN apk update && \
    apk add --no-cache curl bash ca-certificates tzdata

ADD identify_format.sh .

RUN pip install opf-fido

ENTRYPOINT ["./identify_format.sh"]
