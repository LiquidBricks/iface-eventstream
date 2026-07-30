import { EVENTSTREAM_CONSUMER_ERROR } from '@liquid-bricks/lib-diagnostics/codes';
import { COMPONENT_SERVICE_SUBJECT_PATTERN } from './constants.js';
import { createConsumerName, createEphemeralConsumer, removeConsumer } from './consumer.js';
import { safeDiagnostics } from './diagnostics.js';
import { createEvent } from './events.js';
import { formatServerSentEvent } from './sse.js';

export function eventstream({
  natsContext,
  diagnostics: rootDiagnostics,
  streamName,
  subjects,
} = {}) {
  const consumerRegistry = new Map();
  let connectionCounter = 0;

  return (_req, res) => {
    const connectionId = ++connectionCounter;
    const diagnostics = safeDiagnostics(rootDiagnostics).child({
      system: 'eventstream',
      interface: 'iface-eventstream',
      connectionId,
      streamName,
      subjectPattern: COMPONENT_SERVICE_SUBJECT_PATTERN,
    });
    let closed = false;
    let nextId = 1;
    const consumerRecords = new Set();
    consumerRegistry.set(connectionId, consumerRecords);

    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders?.();
    res.write('retry: 3000\n\n');

    const writeEvent = (event) => {
      if (closed) {
        return;
      }

      res.write(formatServerSentEvent(event));
    };

    const reportError = (error) => {
      diagnostics.warn(false, EVENTSTREAM_CONSUMER_ERROR, 'eventstream consumer error', {
        error: String(error?.stack || error),
        subjects,
      });

      writeEvent({
        id: `error-${Date.now()}`,
        event: 'eventstream.error',
        data: {
          event: 'eventstream.error',
          receivedAt: new Date().toISOString(),
          message: String(error?.message || error),
        },
      });
    };

    const close = () => {
      if (closed) {
        return;
      }

      closed = true;
      const records = consumerRegistry.get(connectionId) ?? new Set();
      consumerRegistry.delete(connectionId);

      for (const record of records) {
        record.closed = true;
        void removeConsumer(record, diagnostics);
      }
    };

    const consumeConsumer = async (record) => {
      const consumerDiagnostics = diagnostics.child({ consumerName: record.name, subjects });
      consumerDiagnostics.info('eventstream consumer started', { consumerName: record.name, subjects });

      try {
        for await (const message of record.messages) {
          if (closed) {
            break;
          }

          try {
            writeEvent(createEvent({ id: nextId, message }));
            message.ack?.();
          } catch (error) {
            message.nak?.();
            throw error;
          }

          nextId += 1;
        }
      } catch (error) {
        if (!closed) {
          reportError(error);
        }
      } finally {
        consumerDiagnostics.info('eventstream consumer stopped', { consumerName: record.name });
      }
    };

    Promise.resolve()
      .then(async () => {
        if (closed) {
          return;
        }

        diagnostics.info('eventstream connected to nats jetstream', { streamName, subjects });

        const record = await createEphemeralConsumer({
          natsContext,
          streamName,
          subjects,
          consumerName: createConsumerName(connectionId),
        });

        if (closed || !consumerRegistry.has(connectionId)) {
          await removeConsumer(record, diagnostics);
          return;
        }

        consumerRecords.add(record);
        consumeConsumer(record);
      })
      .catch((error) => {
        reportError(error);
        close();
        res.end?.();
      });

    res.on?.('close', close);
    res.on?.('error', close);
  };
}
