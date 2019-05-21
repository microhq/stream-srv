FROM alpine
ADD stream-srv /stream-srv
ENTRYPOINT [ "/stream-srv" ]
