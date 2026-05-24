export function safeDiagnostics(diagnostics) {
  return diagnostics ?? {
    child: () => safeDiagnostics(),
    debug: () => undefined,
    info: () => undefined,
    warn: () => undefined,
  };
}
