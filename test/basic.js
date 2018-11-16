'use strict';

const test = require('ava');

const destination = require('..');

test('throws when missing prefix', t => {
  const err = t.throws(() => destination());
  t.is(err.message, 'fauxherhose-destination-s3: must provide a prefix function');
});

test('throws when missing value', t => {
  const err = t.throws(() => destination({ prefix: Function.prototype }));
  t.is(err.message, 'fauxherhose-destination-s3: must provide a value function');
});