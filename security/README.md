# Security evidence policy

Scanners retain their complete findings. A separate policy evaluates exact, reviewed,
time-bounded exceptions for trusted local simulated use. Acceptance is not remediation.

Exceptions bind to the artifact role, architecture, image recipe, immutable bases,
advisory, severity, package version and target. Development dependencies also bind
to their lockfile. New, changed or expired High/Critical findings fail the policy.
Scanner failures and incomplete evidence fail closed.

Any observed runtime Critical finding blocks runtime-image release artifacts,
including an accepted finding. Source and non-image build delivery remain separate.
Every built image digest is retained for traceability.

Pull requests are evaluated against the trusted base-branch baseline. Policy changes
require repository-owner review; repository protection settings must require it.
No exception is automatically extended, and no severity is suppressed.
