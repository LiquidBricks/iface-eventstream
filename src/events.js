import { parseComponentServiceSubject } from './subjects.js';

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
