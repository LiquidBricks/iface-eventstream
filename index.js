const eventTypes = [
  'component.evt.registered',
  'componentInstance.evt.started',
];

const loremPayloads = {
  'component.evt.registered': () => ({
    component: {
      id: 'component-lorem',
      name: 'Lorem Ipsum Processor',
      description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
    },
    detail: 'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.',
  }),
  'componentInstance.evt.started': () => ({
    componentInstance: {
      id: 'instance-ipsum',
      componentId: 'component-lorem',
      status: 'started',
    },
    detail: 'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.',
  }),
};

export function createEvent({ id, random = Math.random }) {
  const type = eventTypes[Math.floor(random() * eventTypes.length)] ?? eventTypes[0];

  return {
    id,
    event: type,
    data: {
      id,
      event: type,
      emittedAt: new Date().toISOString(),
      payload: loremPayloads[type](),
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
  random = Math.random,
  maxDelayMs = 3000,
} = {}) {
  return (_req, res) => {
    let closed = false;
    let timer;
    let nextId = 1;

    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders?.();
    res.write('retry: 3000\n\n');

    const schedule = () => {
      const delayMs = Math.floor(random() * (maxDelayMs + 1));
      timer = setTimeout(() => {
        if (closed) return;
        res.write(formatServerSentEvent(createEvent({ id: nextId, random })));
        nextId += 1;
        schedule();
      }, delayMs);
    };

    const close = () => {
      closed = true;
      clearTimeout(timer);
    };

    res.on?.('close', close);
    res.on?.('error', close);
    schedule();
  };
}

export default eventstream;
