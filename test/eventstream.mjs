import test from 'node:test';
import assert from 'node:assert/strict';

import {
  COMPONENT_SERVICE_SUBJECTS,
  createEvent,
  eventstream,
  formatServerSentEvent,
  parseComponentServiceSubject,
} from '../index.js';

function createDiagnosticsSpy() {
  return {
    child() {
      return this;
    },
    info() {},
    warn() {},
  };
}

function createSubscription(messages) {
  let closed = false;

  return {
    unsubscribe() {
      closed = true;
    },
    async *[Symbol.asyncIterator]() {
      for (const message of messages) {
        if (closed) {
          return;
        }

        yield message;
      }
    },
  };
}

test('parseComponentServiceSubject maps component-service subject tokens', () => {
  assert.deepEqual(
    parseComponentServiceSubject('prod.component-service._._.evt.componentInstance.startDone.v1.instance-1'),
    {
      env: 'prod',
      ns: 'component-service',
      tenant: '_',
      context: '_',
      channel: 'evt',
      entity: 'componentInstance',
      action: 'startDone',
      version: 'v1',
      id: 'instance-1',
    },
  );
});

test('createEvent formats a NATS message as a component-service SSE event', () => {
  const evt = createEvent({
    id: 7,
    now: () => new Date('2026-05-18T12:00:00.000Z'),
    message: {
      subject: 'prod.component-service._._.cmd.componentInstance.start.v1.instance-1',
      reply: 'reply.subject',
      json: () => ({ data: { instanceId: 'instance-1' } }),
    },
  });

  assert.equal(evt.id, 7);
  assert.equal(evt.event, 'component-service.cmd');
  assert.equal(evt.data.subject, 'prod.component-service._._.cmd.componentInstance.start.v1.instance-1');
  assert.equal(evt.data.receivedAt, '2026-05-18T12:00:00.000Z');
  assert.equal(evt.data.tokens.channel, 'cmd');
  assert.deepEqual(evt.data.payload, { data: { instanceId: 'instance-1' } });
});

test('formatServerSentEvent emits valid SSE fields', () => {
  const formatted = formatServerSentEvent({
    id: 3,
    event: 'component-service.evt',
    data: { subject: 'prod.component-service._._.evt.component.registerDone.v1._' },
  });

  assert.equal(
    formatted,
    'id: 3\nevent: component-service.evt\ndata: {"subject":"prod.component-service._._.evt.component.registerDone.v1._"}\n\n',
  );
});

test('eventstream creates core NATS subscriptions and streams received messages', async () => {
  const writes = [];
  const listeners = new Map();
  const subscribedSubjects = [];
  const message = {
    subject: 'prod.component-service._._.evt.component.registerDone.v1.component-1',
    json: () => ({ data: { componentId: 'component-1' } }),
  };
  const natsContext = {
    connection: async () => ({
      subscribe(subject) {
        subscribedSubjects.push(subject);
        return createSubscription(subject.endsWith('.evt.>') ? [message] : []);
      },
    }),
  };
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

  eventstream({ natsContext, diagnostics: createDiagnosticsSpy() })({}, response);

  await new Promise((resolve) => setTimeout(resolve, 10));
  listeners.get('close')();

  assert.deepEqual(subscribedSubjects, COMPONENT_SERVICE_SUBJECTS);
  const eventWrites = writes.filter((chunk) => chunk.startsWith('id: '));
  assert.equal(eventWrites.length, 1);
  assert.match(eventWrites[0], /^id: 1\nevent: component-service\.evt\n/);
  assert.match(eventWrites[0], /"subject":"prod\.component-service\._\._\.evt\.component\.registerDone\.v1\.component-1"/);
  assert.match(eventWrites[0], /"componentId":"component-1"/);
});
