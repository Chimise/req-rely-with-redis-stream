import Redis from "ioredis";
import { nanoid } from "nanoid";

export class RedisRequest {
  constructor(reqStream, replyStream) {
    this.reqStream = reqStream;
    this.replyStream = replyStream;
    this.pub = new Redis();
    this.sub = new Redis();
    this.correlationMap = new Map();
    this.initialize().then(() => console.log('Received new data'));
  }

  async initialize(lastId = "$") {
    const [[, records]] = await this.sub.xread(
      "BLOCK",
      '0',
      "STREAMS",
      this.replyStream,
      lastId
    );

    for (const [id, [, data]] of records) {
      const parsedData = JSON.parse(data);
      const callback = this.correlationMap.get(parsedData.correlationId);
      callback && callback(parsedData.data);
      lastId = id;
    }

    await this.initialize(lastId);
  }

  send(data) {
    return new Promise((res, rej) => {
      const correlationId = nanoid();
      const timeout = setTimeout(() => {
        this.correlationMap.delete(correlationId);
        rej(new Error("Request timeout"));
      }, 10000);

      this.correlationMap.set(correlationId, (message) => {
        clearTimeout(timeout);
        this.correlationMap.delete(correlationId);
        res(message);
      });

      const message = {
        correlationId,
        data,
        replyTo: this.replyStream,
      };

      this.pub.xadd(this.reqStream, '*', 'message', JSON.stringify(message));
    });
  }

  close() {
    this.sub.disconnect();
    this.pub.disconnect();
  }
}

export class RedisReply {
  constructor(reqStream, consumerGroup, consumer) {
    this.reqStream = reqStream;
    this.consumerGroup = consumerGroup;
    this.consumer = consumer;
    this.pub = new Redis();
    this.sub = new Redis();
  }

  _createConsumerGroup() {
    return this.sub
      .xgroup("CREATE", this.reqStream, this.consumerGroup, "$", "MKSTREAM")
      .then(
        () => {
          console.log("Stream Created");
        },
        () => {
          console.log("Stream already exists");
        }
      );
  }

  async _processData(id, message, handler) {
    message = JSON.parse(message);
    const result = await handler(message.data);
    await this.pub.xadd(
      message.replyTo,
      "*",
      "message",
      JSON.stringify({ data: result, correlationId: message.correlationId })
    );
    await this.sub.xack(this.reqStream, this.consumerGroup, id);
  }

  async _readOldEntries(handler) {
    const [[, records]] = await this.sub.xreadgroup(
      "GROUP",
      this.consumerGroup,
      this.consumer,
      "STREAMS",
      this.reqStream,
      "0"
    );
    for (const [id, [, data]] of records) {
      await this._processData(id, data, handler);
    }
  }

  async _readNewEntries(handler) {
    const [[, records]] = await this.sub.xreadgroup(
      "GROUP",
      this.consumerGroup,
      this.consumer,
      "BLOCK",
      "0",
      "COUNT",
      "1",
      "STREAMS",
      this.reqStream,
      ">"
    );
    for (const [id, [, data]] of records) {
      await this._processData(id, data, handler);
    }

    await this._readNewEntries(handler);
  }

  onMessage(handler) {
    this._createConsumerGroup()
      .then(() => {
        return this._readOldEntries(handler);
      })
      .then(() => {
        return this._readNewEntries(handler);
      })
      .catch((err) => console.log(err));
  }

  close() {
    this.sub.disconnect();
    this.pub.disconnect();
  }
}
