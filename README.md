# Ringba Webhook to Google Sheets

A FastAPI webhook service that receives Ringba call events and writes them to a Google Sheet with deduplication and background processing.

## Features

- **POST /ringba-webhook**: Receives Ringba call events
- **POST /admin/refresh-map**: Builds DID→Publisher mapping
- **GET /healthz**: Health check endpoint
- Security via webhook and admin secrets
- DID normalization (last 10 digits)
- Campaign filtering
- SQLite deduplication
- Background batch writing to Google Sheets
- Real-time DID cache maintenance

## Setup

### Environment Variables

Set the following environment variables:

- `WEBHOOK_SECRET`: Secret for webhook authentication
- `ADMIN_SECRET`: Secret for admin endpoints
- `RINGBA_CAMPAIGNS`: Comma-separated list of allowed campaigns (optional)
- `GOOGLE_CREDENTIALS_JSON`: Google service account credentials as JSON string
- `MASTER_CPA_DATA`: Google Sheet ID or full URL

### Google Sheets Setup

1. Create a Google Sheet with the following tabs:
   - "Ringba Raw" (will be created automatically)
   - "Real Time" (contains normalized DIDs)
   - "DID Publisher Map" (created by admin endpoint)
   - "Publisher DID Counts" (created by admin endpoint)

2. Create a Google Cloud service account:
   - Go to Google Cloud Console
   - Create a new project or select existing
   - Enable Google Sheets API
   - Create a service account
   - Download the JSON credentials
   - Share your Google Sheet with the service account email

### Installation

```bash
pip install -r requirements.txt
python app.py
```

## API Endpoints

### POST /ringba-webhook

Receives Ringba call events. Requires authentication via:
- Query parameter: `?secret=YOUR_WEBHOOK_SECRET`
- Header: `X-Webhook-Secret: YOUR_WEBHOOK_SECRET`

### POST /admin/refresh-map

Builds DID→Publisher mapping. Requires authentication via:
- Header: `X-Admin-Secret: YOUR_ADMIN_SECRET`

### GET /healthz

Returns health status and realtime DID count.

## Deployment

### Render.com

Use the provided `render.yaml` for easy deployment on Render.com.

### Other Platforms

The app runs on any platform that supports Python and can set environment variables.

## Data Flow

1. Ringba sends webhook data to `/ringba-webhook`
2. Data is validated, normalized, and filtered
3. Duplicates are checked against SQLite database
4. Valid data is queued for background processing
5. Background writer flushes data to Google Sheets every 5 seconds or 50 rows
6. Background cache updates realtime DIDs every 5 minutes
7. Admin endpoint builds publisher mappings from latest call data
