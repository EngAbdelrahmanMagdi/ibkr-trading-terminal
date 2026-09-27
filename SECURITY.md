# Security Policy

## Scope

This project is a portfolio and demonstration real-time stock trading terminal.

- Public deployments operate in **MOCK mode only** and do not contain or use brokerage credentials.
- **IBKR Paper Trading** integration is intended only for trusted private development environments.
- **Live-money trading is not supported.**
- Broker credentials and API secrets must never be exposed to the browser, committed to the repository, or written to logs.
- The AI insights component is isolated from trading execution and cannot place, modify, or cancel orders.

## Reporting a Vulnerability

Please do not disclose security vulnerabilities, credentials, tokens, or broker account information through public GitHub issues.

If private vulnerability reporting is enabled for this repository, use GitHub's **Security → Report a vulnerability** option.

When reporting an issue, include:

- a description of the vulnerability;
- the potential impact;
- steps to reproduce;
- the affected component.

This is a personal portfolio project and does not operate a bug bounty program or formal response SLA.

## Supported Versions

Only the latest version of the `main` branch is maintained.