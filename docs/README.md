# Real-Time Market Data System (RTMDS) Documentation

Welcome to the RTMDS engineering documentation. This repository serves as the single source of truth for the platform's architecture, operational runbooks, and development guidelines.

## 🧭 Onboarding: Where to Start?
If you are a new engineer or SRE joining the team, please read the documentation in the following sequence:

1. **[System Overview](architecture/SYSTEM_OVERVIEW_ARCHITECTURE.md)** - Understand the business SLAs and core capabilities.
2. **[Glossary](architecture/GLOSSARY_GUIDE.md)** - Learn the domain-specific vocabulary used by the team.
3. **[Component Architecture](architecture/COMPONENT_ARCHITECTURE.md)** - Visualize the microservice boundaries.
4. **[Local Development](development/LOCAL_DEVELOPMENT_GUIDE.md)** - Boot the stack on your local machine and inject synthetic data.

## 📚 Documentation Directory

### 🏗️ Architecture (`/architecture`)
Defines the physical boundaries, data flows, API contracts, and observability strategies.
- [System Overview](architecture/SYSTEM_OVERVIEW_ARCHITECTURE.md)
- [Component Architecture](architecture/COMPONENT_ARCHITECTURE.md)
- [Data Flow](architecture/DATA_FLOW_ARCHITECTURE.md)
- [Observability Architecture](architecture/OBSERVABILITY_ARCHITECTURE.md)
- [API Reference & Contracts](architecture/API_REFERENCE.md)
- [Glossary](architecture/GLOSSARY_GUIDE.md)

### 🏛️ Architecture Decision Records (`/adr`)
Immutable historical records of our major technical pivots and infrastructure choices.
- [0001: Redis Pub/Sub for Live Routing](adr/0001_REDIS_PUBSUB_DECISION.md)
- [0002: PostgreSQL for Event Storage](adr/0002_POSTGRES_EVENT_STORE_DECISION.md)
- [0003: Command Bus Admin API](adr/0003_COMMAND_BUS_ADMIN_DECISION.md)

### 🛠️ Operations (`/operations`)
"Break Glass" procedures, runbooks, and deployment guides for on-call SREs.
- [Troubleshooting Guide](operations/TROUBLESHOOTING_GUIDE.md)
- [Operational Runbooks](operations/RUNBOOKS_INDEX.md)
- [Recovery Guide](operations/RECOVERY_GUIDE.md)
- [Deployment Guide](operations/GENERAL_DEPLOYMENT_GUIDE.md)

### 💻 Development (`/development`)
Engineering culture, coding standards, and contribution expectations.
- [Local Development](development/LOCAL_DEVELOPMENT_GUIDE.md)
- [Coding Standards](development/CODING_STANDARDS_GUIDE.md)
- [Contributor Guide](development/CONTRIBUTOR_GUIDE.md)
