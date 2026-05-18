export const COMPONENT_SERVICE_SUBJECT_PATTERN = 'prod.component-service.*.*.{cmd|evt|exec}.>';
export const COMPONENT_SERVICE_SUBJECTS = [
  'prod.component-service.*.*.cmd.>',
  'prod.component-service.*.*.evt.>',
  'prod.component-service.*.*.exec.>',
];

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
  subjects = COMPONENT_SERVICE_SUBJECTS,
} = {}) {
  return (_req, res) => {
    const diagnostics = safeDiagnostics(rootDiagnostics).child({
      system: 'eventstream',
      interface: 'iface-eventstream',
      subjectPattern: COMPONENT_SERVICE_SUBJECT_PATTERN,
    });
    let closed = false;
    let nextId = 1;
    const subscriptions = [];

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
      diagnostics.warn(false, 'EVENTSTREAM_SUBSCRIPTION_ERROR', 'eventstream subscription error', {
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
      closed = true;
      for (const subscription of subscriptions) {
        try {
          subscription.unsubscribe();
        } catch {
          // Best-effort cleanup for a client-owned stream.
        }
      }
    };

    const consumeSubscription = async (subscription, subject) => {
      const subscriptionDiagnostics = diagnostics.child({ subject });
      subscriptionDiagnostics.info('eventstream subscription started', { subject });

      try {
        for await (const message of subscription) {
          if (closed) {
            break;
          }

          writeEvent(createEvent({ id: nextId, message }));
          nextId += 1;
        }
      } catch (error) {
        if (!closed) {
          reportError(error);
        }
      } finally {
        subscriptionDiagnostics.info('eventstream subscription stopped', { subject });
      }
    };

    Promise.resolve()
      .then(async () => {
        if (!natsContext?.connection) {
          throw new Error('eventstream requires natsContext.connection');
        }

        const natsConnection = await natsContext.connection();
        if (closed) {
          return;
        }

        diagnostics.info('eventstream connected to nats', { subjects });

        for (const subject of subjects) {
          if (closed) {
            break;
          }

          const subscription = natsConnection.subscribe(subject);
          subscriptions.push(subscription);
          consumeSubscription(subscription, subject);
        }
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
