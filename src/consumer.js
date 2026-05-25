import {
  ACK_POLICY_EXPLICIT,
  DELIVER_POLICY_ALL,
  EPHEMERAL_INACTIVE_THRESHOLD_NANOS,
} from './constants.js';

export function createConsumerName(connectionId) {
  const suffix = Math.random().toString(36).slice(2, 10);
  return `iface_eventstream_${connectionId}_${Date.now().toString(36)}_${suffix}`;
}

export async function createEphemeralConsumer({
  natsContext,
  streamName,
  subjects,
  consumerName,
}) {
  if (!streamName) {
    throw new Error('eventstream requires streamName');
  }

  if (!Array.isArray(subjects) || subjects.length === 0) {
    throw new Error('eventstream requires subjects');
  }

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
      deliver_policy: DELIVER_POLICY_ALL,
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

export async function removeConsumer(record, diagnostics) {
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
}
