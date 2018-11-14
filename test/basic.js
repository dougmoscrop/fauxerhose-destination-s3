'use strict';

const test = require('ava');

const destination = require('..');

test('throws when missing partition()', t => {
  const err = t.throws(() => destination());
  t.is(err.message, 'fauxherhose-destination-s3: must provide a partition() function');
});

test('throws when missing value()', t => {
  const err = t.throws(() => destination({ partition: Function.prototype }));
  t.is(err.message, 'fauxherhose-destination-s3: must provide a value() function');
});