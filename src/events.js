import { parseNatsSubject } from './subjects.js';

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
  const tokens = parseNatsSubject(message.subject);
  const namespace = tokens.ns || 'component-service';
  const channel = tokens.channel || 'message';
  const eventNamespace = namespace === 'domain' ? namespace : 'component-service';
  const event = `${eventNamespace}.${channel}`;

  return {
    id,
    event,
    data: {
      id,
      event,
      receivedAt: now().toISOString(),
      subject: message.subject,
      reply: message.reply || undefined,
      namespace,
      channel,
      tokens,
      payload: decodeMessagePayload(message),
    },
  };
}
