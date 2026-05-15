# Phoenix F Drive Recovery Source Of Truth

Created: 2026-05-15 19:00 UTC

This repository was recovered from the VPS runtime tree at
`/opt/phoenix-testnet` after the original G drive checkout became unstable.

## Recovery Source

- VPS runtime: `/opt/phoenix-testnet`
- VPS export: `/opt/phoenix-testnet/recovery_exports/phoenix_recovery_20260515_185231`
- VPS tarball: `/opt/phoenix-testnet/recovery_exports/phoenix_recovery_20260515_185231.tar.gz`
- F drive workdir: `F:\ba\币安交易\phoenix`
- F drive recovery package: `F:\ba\币安交易\phoenix_recovery_20260515_185231`

## Safety Scope

The recovery and Git solidification pass is code-asset only:

- no testnet exploration session
- no real testnet micro cycle
- no mainnet
- no cleanup
- no auto promotion
- no G drive deletion

## Test Count Note

The recovered active test tree has 40 `tests/test_*.py` files and 435 collected
pytest tests. The older 468/475 counts came from the former G drive local
working tree. Because `G:\ba\币安交易` is currently unreadable, those extra
tests cannot be enumerated from G drive. The VPS runtime, recovery package, and
F drive active test tree match each other.

## Verification

- `python -m py_compile` on key runtime files: pass
- `python -m pytest --collect-only -q`: 435 tests collected
- `python -m pytest -q`: pass
- local no-order smoke: executor not called, no testnet order, no mainnet order

## Git Policy

Commit source code, configs, tests, and small documentation only. Do not commit
runtime logs, recovery tarballs, `.codex_backups`, `deploy_backups`, `.env`,
keys, tokens, cache files, or Python bytecode.
