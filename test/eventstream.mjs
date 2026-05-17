import test from 'node:test';
import assert from 'node:assert/strict';

import { createEvent, eventstream, formatServerSentEvent } from '../index.js';

test('createEvent returns one of the supported event types with the provided id', () => {
  const registered = createEvent({ id: 7, random: () => 0 });
  assert.equal(registered.id, 7);
  assert.equal(registered.event, 'component.evt.registered');
  assert.equal(registered.data.id, 7);
  assert.equal(registered.data.payload.component.id, 'component-lorem');

  const started = createEvent({ id: 8, random: () => 0.99 });
  assert.equal(started.event, 'componentInstance.evt.started');
  assert.equal(started.data.payload.componentInstance.status, 'started');
});

test('formatServerSentEvent emits valid SSE fields', () => {
  const formatted = formatServerSentEvent({
    id: 3,
    event: 'component.evt.registered',
    data: { message: 'Lorem ipsum' },
  });

  assert.equal(
    formatted,
    'id: 3\nevent: component.evt.registered\ndata: {"message":"Lorem ipsum"}\n\n',
  );
});

test('eventstream writes sequential ids', async () => {
  const writes = [];
  const listeners = new Map();
  const response = {
    setHeader() {},
    flushHeaders() {},
    write(chunk) {
      writes.push(chunk);
    },
    end() {},
    on(name, listener) {
      listeners.set(name, listener);
    },
  };

  eventstream({ random: () => 0, maxDelayMs: 0 })({}, response);

  await new Promise((resolve) => setTimeout(resolve, 10));
  listeners.get('close')();

  const eventWrites = writes.filter((chunk) => chunk.startsWith('id: '));
  assert.ok(eventWrites.length >= 2);
  assert.match(eventWrites[0], /^id: 1\n/);
  assert.match(eventWrites[1], /^id: 2\n/);
});
