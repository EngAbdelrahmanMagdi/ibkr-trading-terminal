import { contentSecurityPolicy } from "@/lib/csp";

test("production blocks inline scripts while allowing chart geometry attributes", () => {
  const policy = contentSecurityPolicy("test-nonce", false);
  expect(policy).toContain(
    "script-src 'self' 'nonce-test-nonce' 'strict-dynamic';",
  );
  expect(policy).not.toContain("unsafe-eval");
  expect(policy).toContain("script-src-attr 'none'");
  expect(policy).toContain("style-src-attr 'unsafe-inline'");
  expect(policy).toContain("frame-ancestors 'none'");
});

test("development eval is isolated to script policy", () => {
  expect(contentSecurityPolicy("dev", true)).toContain("'unsafe-eval'");
});
