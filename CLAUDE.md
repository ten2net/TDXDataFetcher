# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TDXDataFetcher (tdxapi) is a Python library for directly connecting to 通达信 (TDX) market data servers via TCP protocol. It does not depend on any TDX client software - it implements the binary protocol directly.

**Key Design Principle**: All protocol implementations are calibrated against the pytdx source code to ensure binary-level compatibility.

## Build & Test Commands

```bash
# Install in editable mode
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run specific test files
pytest tests/test_protocol.py
pytest tests/test_parser.py
pytest tests/test_integration.py

# Run tests with verbose output
pytest -v

# Run real network tests (requires internet connection to TDX servers)
pytest tests/test_real_network.py -v
```

## Architecture

The library is organized into four layers:

### 1. Protocol Layer (`src/tdxapi/protocol/`)
- `constants.py`: Market codes (SZ=0, SH=1), K-line categories, server list, setup commands
- `requests.py`: Binary request builders for each API command
- `packet.py`: Response header parsing and low-level receive logic

**Important**: Protocol constants (especially `SETUP_CMD1/2/3`) are derived from pytdx reverse engineering. Do not modify these without validating against pytdx source.

### 2. Network Layer (`src/tdxapi/network/`)
- `client.py`: `TdxClient` class - the main user-facing API
  - Auto server selection (latency-based)
  - Connection handshake (3-step setup)
  - Auto-reconnect with retry logic
  - Thread-safe operations (optional locking)
  - Background heartbeat (30s interval)

### 3. Parser Layer (`src/tdxapi/parser/`)
- `quote_parser.py`: Binary response parsers for quotes, K-lines, ticks, etc.
- Response data uses zlib compression when `zip_size < unzip_size`

### 4. Models (`src/tdxapi/models/`)
- `quote.py`: `StockQuote`, `Bar`, `Tick` dataclasses

## Key Implementation Details

### Connection Handshake
All TDX connections require a 3-step setup sequence (`SETUP_CMD1`, `SETUP_CMD2`, `SETUP_CMD3`) sent immediately after TCP connect. The content of these commands is fixed and must not change.

### Request/Response Flow
1. Build request body using functions in `requests.py`
2. Send raw bytes via `_send_recv()` which handles:
   - Response header parsing (16 bytes)
   - Decompression (zlib) if needed
   - Retry on connection failure

### Market Codes
- SH (上海) = 1
- SZ (深圳) = 0
- BJ (北交所) = 2
- Futures markets: 6=上海期货, 7=中金所, 8=大连, 9=郑州

### K-Line Period Mapping
- Daily: 9, Weekly: 5, Monthly: 6
- Minute: 1min=7, 5m=0, 15m=1, 30m=2, 60m=3

### Default Servers
Hardcoded list in `constants.py`. Client auto-selects lowest latency server if no IP specified.

## Adding New API Commands

1. Add request builder in `protocol/requests.py`
2. Add response parser in `parser/quote_parser.py` (or new parser file)
3. Add client method in `network/client.py`
4. Add test in `tests/test_protocol.py` to verify binary output matches pytdx
5. Add integration test in `tests/test_integration.py`

## Verification Scripts

- `_verify_layout.py`: Validates packet structure against pytdx
- `_verify_parse.py`: Validates parser output against known data
