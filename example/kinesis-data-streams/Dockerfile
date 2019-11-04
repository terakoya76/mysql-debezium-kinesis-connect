FROM node:12

RUN mkdir /kinesis-data-streams
WORKDIR /kinesis-data-streams

# Avoid `user "nobody" does not have permission to access`
# see: https://github.com/nodejs/node-gyp/issues/1236
RUN npm -g install kinesalite --unsafe

EXPOSE 4568
CMD ["kinesalite", "--port", "4568", "--path", "/kinesis-data-streams/data", "--ssl"]
