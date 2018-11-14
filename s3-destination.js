'use strict';

const https = require('https');
const { PassThrough, Writable } = require('stream');
const zlib = require('zlib');

const aws = require('aws-sdk');

class S3Destination extends Writable {
  constructor(options) {
    super({ objectMode: true });

    this.bucket = options.buclet;
    this.client = options.client;
    this.getValue = options.value;
    this.getPartition = options.partition;
    this.uploads = new Map();
  }

  _write(event, encoding, callback) {
    try {
      const partition = this.getPartition(event);
      const stream = this.getStream(partition);
      const value = this.getValue(event);

      stream.push(value);
      stream.push('\n');

      callback();
    } catch (err) {
      this.emit('error', err);
    }
  }

  _final(callback) {
    Promise.resolve()
      .then(() => {
        const values = this.uploads.values();
        const promises = Array.from(values).map(upload => {
          upload.stream.push(null);
          return upload.promise;
        });

        return Promise.all(promises);
      })
      .then(() => callback())
      .catch(e => callback(e));
  }

  getStream(partition) {
    const upload = this.uploads.get(partition);

    if (upload) {
      return upload.stream;
    }

    const date = new Date().getTime();
    const key = `${partition}-${date}.zip`;
    const stream = new PassThrough();
    const zip = zlib.createGzip();

    const promise = this.client.upload({
      Key: key,
      Bucket: this.bucket,
      Body: stream.pipe(zip),
    }).promise();

    this.destinations.set(partition, {
      promise,
      stream,
    });

    return stream;
  }
}

function defaultClient() {
  const agent = new https.Agent({ keepAlive: true });
  return new aws.S3({
    httpOptions: {
      agent
    }
  });
}

module.exports = (options = {}) => {
  const {
    bucket,
    client = defaultClient(),
    partition,
    value,
  } = options;

  if (typeof partition !== 'function') {
    throw new Error('fauxherhose-destination-s3: must provide a partition() function');
  }

  if (typeof value !== 'function') {
    throw new Error('fauxherhose-destination-s3: must provide a value() function');
  }

  return () => {
    return new S3Destination({ bucket, client, partition, value });
  };
};