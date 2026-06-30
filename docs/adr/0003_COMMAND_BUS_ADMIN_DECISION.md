# ADR 0003: Command Bus Architecture for the Admin API

**Date:** 2026-06-28
**Status:** Approved
**Purpose:** To document the adoption of the Command Bus pattern for operational state mutations.
**Intended Audience:** Backend Engineers, SREs.
**Maintenance Strategy:** Immutable historical record.

## Context
The platform requires an Operational Administration API to allow SREs to pause feeds, trigger snapshots, or drain gateways. Initially, these were hardcoded HTTP handlers directly manipulating global variables or shared controller structs. 

## Options Considered
1. **Direct HTTP Handlers:** `func HandlePause(w, r)` directly accesses the Publisher struct and calls `pub.Pause()`. 
   - *Pros:* Simple. 
   - *Cons:* Causes "route explosion" as we add hundreds of commands. Tightly couples HTTP transport logic to domain logic.
2. **Command Bus Pattern:** HTTP routes are parameterized (`/operations/{service}/{action}`). The router dynamically instantiates a struct implementing the `Command` interface and pushes it to a `CommandBus` for execution.

## Decision
We decided to implement the **Command Bus Pattern** for all mutative administrative actions.

## Consequences
- **Positive:** Infinite scalability. Adding a new command across 100 microservices requires zero changes to the `api.Router` HTTP configuration.
- **Positive:** Idempotency and audit logging can be applied via middleware to the Bus itself, guaranteeing every action is structurally captured.
- **Negative:** Slight indirection. Engineers must trace the `CommandRegistry` string map to find the actual implementation of an action.
