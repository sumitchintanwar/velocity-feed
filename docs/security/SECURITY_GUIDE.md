# RTMDS Security & Authorization Guide

## 1. Overview
The Real-Time Market Data System (RTMDS) prioritizes speed and scale. As such, security controls are designed to minimize latency overhead while ensuring robust protection of market data streams, internal APIs, and configuration secrets.

This document outlines the authentication, authorization, secret management, and audit logging strategies for the RTMDS platform.

## 2. Authentication Model

### WebSocket Gateway Authentication
RTMDS is designed for high-frequency trading clients. WebSocket authentication avoids heavy per-message cryptographic overhead.

- **Initial Handshake:** Authentication is performed *once* during the initial HTTP to WebSocket Upgrade request.
- **Token Mechanism:** Clients must provide a short-lived JSON Web Token (JWT) via the `Authorization: Bearer <token>` HTTP header.
- **Token Validation:** The Gateway validates the JWT signature using a cached public key (JWKS) from the central IAM provider. This ensures sub-millisecond validation without network calls to the auth server.

### Administrative API Authentication
The `cmd/server` exposes administrative and observability endpoints (`/metrics`, `/health`, `/ready`).
- **Internal Network Policy:** Administrative endpoints should NOT be exposed to the public internet. Kubernetes NetworkPolicies and Ingress controllers must route these exclusively to internal administration subnets.
- **mTLS:** Machine-to-machine communication (e.g., Prometheus scraping `/metrics`) relies on Service Mesh mTLS (e.g., Istio/Linkerd) rather than application-layer credentials.

## 3. Authorization Model

### Topic-Level Access Control (Entitlements)
Not all clients are entitled to all market data feeds (e.g., premium vs. delayed data).

1. **JWT Claims:** The client's JWT contains an `entitlements` claim listing the data tiers or specific symbols they are authorized to consume (e.g., `"entitlements": ["tier:basic", "exchange:nasdaq"]`).
2. **Subscription Enforcement:** When a client sends a `{"action": "subscribe", "symbols": ["AAPL"]}` message, the Gateway's `TopicManager` cross-references the requested symbols against the client's cached JWT entitlements.
3. **Rejection:** Unauthorized subscription requests are rejected with a `403 Forbidden` WebSocket error frame, and the connection remains open.

## 4. Secret Management

RTMDS services require secrets (e.g., Redis passwords, TLS certificates, Feed Provider API keys). **No secrets are stored in version control.**

- **Kubernetes Secrets:** Secrets are injected into the RTMDS pods via Kubernetes Secrets, mounted as environment variables (`RTMDS_REDIS_PASSWORD`) or tmpfs volumes for certificates.
- **External Secret Store:** Production environments utilize an external vault (e.g., HashiCorp Vault, AWS Secrets Manager) synced to Kubernetes via the External Secrets Operator.
- **Secret Rotation:** RTMDS components are designed to automatically reconnect to downstream dependencies (like Redis) if authentication fails during rotation, avoiding the need for manual pod restarts.

## 5. TLS & Encryption

- **Data in Transit (External):** All client-facing WebSocket connections MUST use `wss://` (TLS 1.3). TLS termination is handled by the Ingress Controller / Load Balancer to offload CPU overhead from the RTMDS Gateway.
- **Data in Transit (Internal):** Internal communication (Gateway to Redis) supports TLS. Set `RTMDS_REDIS_TLS_ENABLED=true` in production environments.
- **Data at Rest:** Persistent logs (if utilizing Redis AOF or PostgreSQL for replays) must be stored on encrypted volumes (e.g., AWS EBS encryption).

## 6. Audit Logging

RTMDS generates high-volume telemetry. For security auditing:

- **Connection Audits:** Every WebSocket connection establishment, authentication success/failure, and disconnection is logged securely via structured JSON logging.
- **Subscription Audits:** Every subscription request (both granted and denied) is logged with the client's `correlation_id` and IP address.
- **Log Retention:** Security-relevant logs must be routed to a centralized SIEM (Security Information and Event Management) system and retained per regulatory requirements (typically 7 years for financial systems).
