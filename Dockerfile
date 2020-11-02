FROM alpine:3

COPY app /app
ENTRYPOINT ["/app"]