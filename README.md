# Project Aegis — Kite MCP Server

A production-ready MCP server that automates Zerodha Kite Connect daily login via headless Playwright and exposes authenticated broker tools through the Model Context Protocol (MCP).

## Overview

`src/kite_mcp.py` performs the following:

- Loads Kite Connect credentials securely from a `.env` file using `python-dotenv`
- Automates Zerodha login on `https://kite.zerodha.com/connect/login?v=3&api_key=...` with Playwright
- Generates a live 6-digit TOTP PIN with `pyotp`
- Intercepts the final redirect to extract `request_token` without loading the redirect page
- Exchanges the `request_token` for an `access_token` via the `kiteconnect` SDK
- Starts a `FastMCP` server and exposes MCP tools to validate the authenticated session

## Files

- `src/kite_mcp.py` — main MCP server script
- `requirements.txt` — pinned Python dependencies
- `.env.example` — example Kite credential environment variables

## Requirements

- Python 3.13+
- `pip`
- `playwright` browsers installed

## Setup

1. Create a virtual environment and activate it.

```powershell
python -m venv .venv
.\.venv\Scripts\activate.bat
```

2. Install dependencies.

```powershell
python -m pip install -r requirements.txt
```

3. Install Playwright browser binaries.

```powershell
python -m playwright install chromium
```

4. Copy `.env.example` to `.env` and fill in your credentials.

```powershell
copy .env.example .env
```

## Required `.env` variables

- `KITE_API_KEY`
- `KITE_API_SECRET`
- `KITE_USER_ID`
- `KITE_PASSWORD`
- `KITE_TOTP_SECRET`

## Usage

Run the MCP server:

```powershell
python src\kite_mcp.py
```

Run login-only mode to verify authentication and print the access token suffix:

```powershell
python src\kite_mcp.py --login-only
```

## MCP Tools

The MCP server exposes tools including:

- `verify_connection` — validates the Kite session and returns account/profile details
- `refresh_session` — refreshes the Kite Connect session via the automated login flow

## Notes

- This implementation uses Playwright exclusively, not Selenium.
- Logging is configured to output structured, timestamped messages to stderr so MCP stdio transport remains clean.
- The headless browser flow is designed for reliable daily TOTP login automation.
