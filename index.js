export const COMPONENT_SERVICE_SUBJECT_PATTERN = 'prod.component-service.*.*.{cmd|evt|exec}.>';
export const COMPONENT_SERVICE_SUBJECTS = [
  'prod.component-service.*.*.cmd.>',
  'prod.component-service.*.*.evt.>',
  'prod.component-service.*.*.exec.>',
];
export const EVENTSTREAM_STREAM_NAME = 'COMPONENT_EVENTSTREAM_STREAM';

const ACK_POLICY_EXPLICIT = 'explicit';
const DELIVER_POLICY_NEW = 'new';
const EPHEMERAL_INACTIVE_THRESHOLD_NANOS = 60_000_000_000;

const SUBJECT_TOKEN_NAMES = [
  'env',
  'ns',
  'tenant',
  'context',
  'channel',
  'entity',
  'action',
  'version',
  'id',
];

function safeDiagnostics(diagnostics) {
  return diagnostics ?? {
    child: () => safeDiagnostics(),
    debug: () => undefined,
    info: () => undefined,
    warn: () => undefined,
  };
}

function decodeMessagePayload(message) {
  try {
    return message.json();
  } catch (jsonError) {
    try {
      return {
        raw: message.string(),
        parseError: String(jsonError),
      };
    } catch {
      return {
        raw: Array.from(message.data ?? []),
        parseError: String(jsonError),
      };
    }
  }
}

function createConsumerName(connectionId) {
  const suffix = Math.random().toString(36).slice(2, 10);
  return `iface_eventstream_${connectionId}_${Date.now().toString(36)}_${suffix}`;
}

async function createEphemeralConsumer({
  natsContext,
  streamName,
  subjects,
  consumerName,
}) {
  if (!natsContext?.jetstream) {
    throw new Error('eventstream requires natsContext.jetstream');
  }

  if (!natsContext?.jetstreamManager) {
    throw new Error('eventstream requires natsContext.jetstreamManager');
  }

  const jetstream = await natsContext.jetstream();
  const jetstreamManager = await natsContext.jetstreamManager();
  let consumer;

  try {
    await jetstreamManager.consumers.add(streamName, {
      name: consumerName,
      ack_policy: ACK_POLICY_EXPLICIT,
      deliver_policy: DELIVER_POLICY_NEW,
      filter_subjects: subjects,
      inactive_threshold: EPHEMERAL_INACTIVE_THRESHOLD_NANOS,
    });

    consumer = await jetstream.consumers.get(streamName, consumerName);
    const messages = await consumer.consume();

    return { consumer, jetstreamManager, messages, name: consumerName, streamName };
  } catch (error) {
    try {
      if (consumer?.delete) {
        await consumer.delete();
      } else {
        await jetstreamManager.consumers.delete(streamName, consumerName);
      }
    } catch {
      // Best-effort cleanup for a partially-created ephemeral consumer.
    }

    throw error;
  }
}

export function parseComponentServiceSubject(subject) {
  const parts = String(subject ?? '').split('.');
  const tokens = SUBJECT_TOKEN_NAMES.reduce((acc, tokenName, index) => {
    acc[tokenName] = parts[index] ?? '';
    return acc;
  }, {});
  const extra = parts.slice(SUBJECT_TOKEN_NAMES.length);

  if (extra.length > 0) {
    tokens.extra = extra;
  }

  return tokens;
}

export function createEvent({ id, message, now = () => new Date() }) {
  const tokens = parseComponentServiceSubject(message.subject);
  const channel = tokens.channel || 'message';
  const event = `component-service.${channel}`;

  return {
    id,
    event,
    data: {
      id,
      event,
      receivedAt: now().toISOString(),
      subject: message.subject,
      reply: message.reply || undefined,
      channel,
      tokens,
      payload: decodeMessagePayload(message),
    },
  };
}

export function formatServerSentEvent({ id, event, data }) {
  return [
    `id: ${id}`,
    `event: ${event}`,
    ...String(JSON.stringify(data)).split(/\r?\n/).map((line) => `data: ${line}`),
    '',
    '',
  ].join('\n');
}

export function eventstream({
  natsContext,
  diagnostics: rootDiagnostics,
  streamName = EVENTSTREAM_STREAM_NAME,
  subjects = COMPONENT_SERVICE_SUBJECTS,
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
      diagnostics.warn(false, 'EVENTSTREAM_CONSUMER_ERROR', 'eventstream consumer error', {
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

    const removeConsumer = async (record) => {
      try {
        await record.messages?.close?.();
      } catch {
        // Best-effort cleanup for a client-owned iterator.
      }

      try {
        if (record.consumer?.delete) {
          await record.consumer.delete();
        } else {
          await record.jetstreamManager?.consumers?.delete?.(record.streamName, record.name);
        }
      } catch (error) {
        try {
          await record.jetstreamManager?.consumers?.delete?.(record.streamName, record.name);
        } catch {
          // Best-effort cleanup for an already-closing ephemeral consumer.
        }

        diagnostics.warn(false, 'EVENTSTREAM_CONSUMER_DELETE_ERROR', 'eventstream consumer cleanup error', {
          consumerName: record.name,
          error: String(error?.stack || error),
        });
      }
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
        void removeConsumer(record);
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
          await removeConsumer(record);
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

export default eventstream;
