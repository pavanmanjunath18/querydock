# ADR 0002: MongoDB 8.0 rather than 9.0 or 8.3

- Status: accepted (2026-10-13)

## Context
On 2026-10-13 the current versions were 8.0.32, 8.3.11 and 9.0.2. 9.0 GA'd on 2026-09-28 with three patches on day one, one of them a sharding-metadata fix, and its docs still say Community availability is "coming soon". 8.3 is a rapid release with limited Community support.

## Decision
Use the 8.0 series (8.0.32), with Go driver v2.9.2 and PyMongo 4.18.3.

## Consequences
A stable, well-documented line for sharding. Upgrading to 9.0 later is a rolling upgrade (8.0 → 9.0 is supported) and gets its own ADR.
