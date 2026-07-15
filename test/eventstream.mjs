import test from 'node:test';
import assert from 'node:assert/strict';

import { eventstream } from '../index.js';
import { createEvent } from '../src/events.js';
import { formatServerSentEvent } from '../src/sse.js';
import { parseComponentServiceSubject, parseNatsSubject } from '../src/subjects.js';

const TEST_COMPONENT_SERVICE_SUBJECTS = [
  'prod.component-service.*.*.cmd.>',
  'prod.component-service.*.*.evt.>',
  'prod.component-service.*.*.exec.>',
];

function createDiagnosticsSpy() {
  return {
    child() {
      return this;
    },
    info() {},
    warn() {},
  };
}

function createConsumerMessages(messages) {
  let closed = false;
  let closeCount = 0;

  return {
    get closeCount() {
      return closeCount;
    },
    async close() {
      closed = true;
      closeCount += 1;
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

test('public entrypoint exports only eventstream', async () => {
  const exportedNames = Object.keys(await import('../index.js')).sort();

  assert.deepEqual(exportedNames, ['eventstream']);
});

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

test('parseNatsSubject maps domain subject tokens and retains the component-service parser alias', () => {
  const subject = 'prod.domain._._.vertex.stateMachine.completed.v1.instance-1';

  assert.deepEqual(parseNatsSubject(subject), {
    env: 'prod',
    ns: 'domain',
    tenant: '_',
    context: '_',
    channel: 'vertex',
    entity: 'stateMachine',
    action: 'completed',
    version: 'v1',
    id: 'instance-1',
  });
  assert.deepEqual(parseComponentServiceSubject(subject), parseNatsSubject(subject));
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

test('createEvent uses the NATS namespace and channel for domain SSE event names', () => {
  const evt = createEvent({
    id: 8,
    now: () => new Date('2026-05-18T12:00:01.000Z'),
    message: {
      subject: 'prod.domain._._.vertex.stateMachine.completed.v1.instance-1',
      json: () => ({ data: { instanceId: 'instance-1' } }),
    },
  });

  assert.equal(evt.event, 'domain.vertex');
  assert.equal(evt.data.namespace, 'domain');
  assert.equal(evt.data.channel, 'vertex');
  assert.equal(evt.data.tokens.entity, 'stateMachine');
  assert.equal(evt.data.tokens.action, 'completed');
  assert.deepEqual(evt.data.payload, { data: { instanceId: 'instance-1' } });
});

test('createEvent preserves the existing component-service SSE name for gateway messages', () => {
  const evt = createEvent({
    id: 9,
    message: {
      subject: 'prod.gateway._._.cmd.component.compute_function.v1._',
      json: () => ({ data: { instanceId: 'instance-1' } }),
    },
  });

  assert.equal(evt.event, 'component-service.cmd');
  assert.equal(evt.data.namespace, 'gateway');
  assert.equal(evt.data.channel, 'cmd');
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

test('eventstream creates an ephemeral JetStream consumer and replays retained messages and streams received messages', async () => {
  const writes = [];
  const listeners = new Map();
  const addedConsumers = [];
  const deletedConsumers = [];
  const streamName = 'EVENTSTREAM_TEST_STREAM';
  const message = {
    subject: 'prod.component-service._._.evt.component.registerDone.v1.component-1',
    json: () => ({ data: { componentId: 'component-1' } }),
    ackCount: 0,
    ack() {
      this.ackCount += 1;
    },
  };
  const consumerMessages = createConsumerMessages([message]);
  const natsContext = {
    jetstreamManager: async () => ({
      consumers: {
        add(stream, configuration) {
          addedConsumers.push({ stream, configuration });
        },
        delete(stream, consumerName) {
          deletedConsumers.push({ stream, consumerName });
        },
      },
    }),
    jetstream: async () => ({
      consumers: {
        get: async (stream, consumerName) => ({
          consume: async () => consumerMessages,
          delete: async () => {
            deletedConsumers.push({ stream, consumerName });
            return true;
          },
        }),
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

  eventstream({
    natsContext,
    diagnostics: createDiagnosticsSpy(),
    streamName,
    subjects: TEST_COMPONENT_SERVICE_SUBJECTS,
  })({}, response);

  await new Promise((resolve) => setTimeout(resolve, 10));
  listeners.get('close')();
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(addedConsumers.length, 1);
  assert.equal(addedConsumers[0].stream, streamName);
  assert.match(addedConsumers[0].configuration.name, /^iface_eventstream_1_/);
  assert.equal(addedConsumers[0].configuration.durable_name, undefined);
  assert.equal(addedConsumers[0].configuration.ack_policy, 'explicit');
  assert.equal(addedConsumers[0].configuration.deliver_policy, 'all');
  assert.deepEqual(addedConsumers[0].configuration.filter_subjects, TEST_COMPONENT_SERVICE_SUBJECTS);

  const eventWrites = writes.filter((chunk) => chunk.startsWith('id: '));
  assert.equal(eventWrites.length, 1);
  assert.match(eventWrites[0], /^id: 1\nevent: component-service\.evt\n/);
  assert.match(eventWrites[0], /"subject":"prod\.component-service\._\._\.evt\.component\.registerDone\.v1\.component-1"/);
  assert.match(eventWrites[0], /"componentId":"component-1"/);
  assert.equal(message.ackCount, 1);
  assert.equal(consumerMessages.closeCount, 1);
  assert.deepEqual(deletedConsumers, [{ stream: streamName, consumerName: addedConsumers[0].configuration.name }]);
});
