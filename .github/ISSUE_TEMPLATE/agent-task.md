---
name: Agent Task
about: Task to be picked up by an AI agent
title: ''
labels: status:available
assignees: ''
---

## Objective

<!-- Clear description of what needs to be done -->

## Scope Labels

<!-- Check all that apply - the setup script will create these labels -->
- [ ] scope:api - Touches rucket-api crate
- [ ] scope:storage - Touches rucket-storage crate
- [ ] scope:consensus - Touches rucket-consensus crate
- [ ] scope:cluster - Touches rucket-cluster crate
- [ ] scope:geo - Touches rucket-geo crate
- [ ] scope:core - Touches rucket-core crate
- [ ] scope:docs - Documentation only
- [ ] scope:tests - Tests only

## Files Likely Affected

<!-- List the main files that will need changes -->
- `crates/rucket-*/src/...`

## Acceptance Criteria

- [ ] Implementation complete
- [ ] Tests pass (`cargo test`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Code formatted (`cargo fmt`)

## Size Estimate

<!-- Check one -->
- [ ] size:small - < 100 lines, 1-2 files
- [ ] size:medium - 100-500 lines, 3-5 files
- [ ] size:large - > 500 lines, 5+ files

## Dependencies

<!-- Leave blank if none -->
Blocked by:
Blocks:

## Additional Context

<!-- Any helpful context, examples, or references -->
