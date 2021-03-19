# Container image that runs your code
FROM alpine:3.10

COPY test.sh /test.sh
ENTRYPOINT ["/test.sh"]