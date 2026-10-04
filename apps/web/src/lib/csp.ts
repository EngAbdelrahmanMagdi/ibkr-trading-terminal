/** Exact public destinations; never derive a policy from request Host or forwarded headers. */
export function contentSecurityPolicy(
  nonce: string,
  development: boolean,
): string {
  const core = new URL(
    process.env.NEXT_PUBLIC_CORE_ORIGIN ?? "http://localhost:18080",
  );
  const gateway = new URL(
    process.env.NEXT_PUBLIC_GATEWAY_ORIGIN ?? "http://localhost:18090",
  );
  const socket = new URL(
    process.env.NEXT_PUBLIC_GATEWAY_WS_URL ?? "ws://localhost:18090/ws",
  );
  for (const endpoint of [core, gateway, socket]) {
    if (
      endpoint.username ||
      endpoint.password ||
      !["http:", "https:", "ws:", "wss:"].includes(endpoint.protocol)
    ) {
      throw new Error("Invalid public origin configuration");
    }
  }
  return [
    "default-src 'self'",
    `script-src 'self' 'nonce-${nonce}' 'strict-dynamic'${development ? " 'unsafe-eval'" : ""}`,
    "script-src-attr 'none'",
    `style-src-elem 'self' 'nonce-${nonce}'`,
    // Lightweight Charts and React use style attributes for measured geometry, not executable content.
    "style-src-attr 'unsafe-inline'",
    `connect-src 'self' ${[...new Set([core.origin, gateway.origin, socket.origin])].join(" ")}`,
    "img-src 'self' data:",
    "font-src 'self'",
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'self'",
    "frame-ancestors 'none'",
  ].join("; ");
}
