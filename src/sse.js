export function formatServerSentEvent({ id, event, data }) {
  return [
    `id: ${id}`,
    `event: ${event}`,
    ...String(JSON.stringify(data)).split(/\r?\n/).map((line) => `data: ${line}`),
    '',
    '',
  ].join('\n');
}
