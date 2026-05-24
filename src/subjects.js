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
