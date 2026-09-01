const secretPatterns = [
  /\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi,
  /\b(?:api[_-]?key|token|password|credential)\s*[:=]\s*[^\s,;]+/gi,
  /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g,
];

/**
 * Defensive final pass for operational text. Runtime events must use fixed,
 * reviewed labels; this protects future error paths from accidentally making
 * a credential or PII value observable.
 */
export function redactOperationalText(value: string): string {
  return secretPatterns
    .reduce((safe, pattern) => safe.replace(pattern, "[REDACTED]"), value)
    .slice(0, 500);
}

export function safeErrorClass(
  _error: unknown,
  fallback = "runtime_operation_failed",
): string {
  // Never surface arbitrary exception text: it can include request headers,
  // provider responses, or tenant input. Callers may use a reviewed fallback.
  return fallback;
}
