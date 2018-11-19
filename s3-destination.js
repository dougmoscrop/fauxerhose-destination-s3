'use strict';

const assert = require('assert');
const https = require('https');
const { PassThrough, Writable } = require('stream');
const zlib = require('zlib');

const aws = require('aws-sdk');
const uuid = require('uuid');

function defaultClient() {
  const agent = new https.Agent({ keepAlive: true });
  return new aws.S3({
    httpOptions: {
      agent
    }
  });
}

module.exports = (options = {}) => {
  const { bucket, client = defaultClient() } = options;

  const getValue = options.value;
  const getPrefix = options.prefix;

  assert(typeof getPrefix === 'function', 'fauxherhose-destination-s3: must provide a prefix function');
  assert(typeof getValue === 'function', 'fauxherhose-destination-s3: must provide a value function');

  return class S3Destination extends Writable {
    constructor() {
      super({ objectMode: true });
  
      this.uploads = new Map();
    }
  
    _write(record, encoding, callback) {
      try {
        const prefix = getPrefix(record);
        const value = getValue(record);

        const stream = this.getStream(prefix);
  
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
  
    getStream(prefix) {
      const upload = this.uploads.get(prefix);
  
      if (upload) {
        return upload.stream;
      }
  
      const date = new Date().getTime();
      const random = uuid.v4();
      const key = `${prefix}/${date}-${random}.gz`;
      const stream = new PassThrough();
      const zip = zlib.createGzip();
  
      const promise = client.upload({
        Key: key,
        Bucket: bucket,
        Body: stream.pipe(zip),
      }).promise();
  
      this.uploads.set(prefix, {
        promise,
        stream,
        key,
      });
  
      return stream;
    }
  };
};