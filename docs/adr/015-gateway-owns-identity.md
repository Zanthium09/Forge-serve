# ADR-015: The gateway owns identity in its own database

Status: accepted (production gateway, 2026-11-12)

## Context
Organizations keep users in their own databases. Integrating would couple
the gateway to one organization's schema and bring it near password data.

## Decision
The gateway keeps its own Postgres schema (teams, users, api_keys,
model_grants, usage_events, admin_audit, schema_migrations) and does not
connect to any organization's user database. Admins add users through the admin
API or a CSV import; leavers are disabled by an admin. No passwords exist in
this project: an API key is the only credential, and only its SHA-256 hash
is stored. Spec §7.1, §7.7.

## Rationale
No site-specific code means the open-source version needs no changes,
and the project never touches passwords.

## Consequences
Offboarding is manual; an "inactive users" admin query helps catch
forgotten accounts.
