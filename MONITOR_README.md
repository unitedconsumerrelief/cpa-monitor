# Ringba Performance Monitor

This monitoring system automatically tracks Ringba performance metrics every 2 hours and sends formatted summaries to Slack.

## Features

- **🕐 Automated Monitoring**: Fetches data every 2 hours
- **📊 CPA Calculation**: Automatically calculates Cost Per Acquisition (Revenue ÷ Completed Calls)
- **📱 Slack Integration**: Sends formatted summaries to your Slack channel
- **📈 Performance Tracking**: Monitors all metrics from your Ringba dashboard
- **🏆 Top Performers**: Highlights best performing publishers

## Setup

### 1. Environment Variables

You'll need these environment variables:

```bash
# Required
RINGBA_API_TOKEN=your_ringba_api_token_here
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK

# Optional (defaults to provided account ID)
RINGBA_ACCOUNT_ID=RA092c10a91f7c461098e354a1bbeda598
```

### 2. Get Ringba API Token

1. Log into your Ringba dashboard
2. Go to Settings → API
3. Generate a new API token
4. Copy the token and set it as `RINGBA_API_TOKEN`

### 3. Get Slack Webhook URL

1. Go to your Slack workspace
2. Create a new app or use existing one
3. Go to Incoming Webhooks
4. Create a new webhook for your desired channel
5. Copy the webhook URL and set it as `SLACK_WEBHOOK_URL`

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Run Monitoring Service

```bash
python monitor.py
```

This will:
- Start monitoring immediately
- Fetch data every 2 hours
- Send summaries to Slack
- Run continuously until stopped

### Test the System

```bash
python test_monitor.py
```

This will test the monitoring system with sample data without making real API calls.

## Slack Message Format

The system sends rich, formatted messages to Slack including:

### 📊 Key Metrics
- Total Calls
- Completed Calls
- Converted Calls
- Conversion Rate
- Total Revenue
- Total Payout
- Total Profit
- **CPA (Cost Per Acquisition)**

### 🏆 Top 5 Publishers
- Ranked by completed calls
- Shows CPA for each publisher

### 📋 Detailed Performance Table
- Top 10 publishers
- Completed calls, CPA, Revenue, Profit
- Formatted table for easy reading

## Sample Slack Message

```
📊 Ringba Performance Summary - 2025-01-15 10:00 - 2025-01-15 12:00 ET

📈 Key Metrics:
• Total Calls: 36
• Completed: 27
• Converted: 4
• Conversion Rate: 11.11%
• Total Revenue: $400.00
• Total Payout: $320.00
• Total Profit: $80.00
• CPA: $14.81

🏆 Top 5 Publishers by Completed Calls:
1. TDES023-YT: 4 completed, $25.00 CPA
2. FITZ: 2 completed, $0.00 CPA
3. Koji Digital: 2 completed, $0.00 CPA
...

📋 Detailed Performance (Top 10):
Publisher             Completed  CPA      Revenue    Profit    
----------------------------------------------------------------------
TDES023-YT           4          $25.00   $100.00    $20.00    
FITZ                 2          $0.00    $0.00      $0.00     
Koji Digital         2          $0.00    $0.00      $0.00     
...
```

## Deployment Options

### Option 1: Run on Same Server as Webhook
Add to your existing `app.py` or run as separate process.

### Option 2: Separate Monitoring Server
Deploy `monitor.py` to a separate server for better reliability.

### Option 3: Cloud Function
Deploy as a scheduled cloud function (AWS Lambda, Google Cloud Functions, etc.)

## Monitoring Metrics

The system tracks all metrics from your Ringba dashboard:

- **Call Metrics**: Incoming, Live, Completed, Ended, Connected
- **Conversion Metrics**: Paid, Converted, Conversion %
- **Quality Metrics**: No Connect, Duplicate, Blocked, IVR Hangup
- **Financial Metrics**: Revenue, Payout, Profit, Margin, Total Cost
- **Time Metrics**: TCL (Total Call Length), ACL (Average Call Length)
- **Performance Metrics**: CPA (Cost Per Acquisition)

## Error Handling

- **API Errors**: Logs and continues monitoring
- **Network Issues**: Retries after 5 minutes
- **Invalid Data**: Skips invalid entries and continues
- **Slack Failures**: Logs errors but doesn't stop monitoring

## Customization

### Modify Time Intervals
Change the monitoring frequency by modifying the sleep duration in `start_monitoring()`:

```python
# Change from 2 hours to 1 hour
await asyncio.sleep(1 * 60 * 60)  # 1 hour
```

### Add More Metrics
Extend the `PublisherMetrics` class to include additional fields from the Ringba API.

### Customize Slack Format
Modify the `send_slack_summary()` method to change the message format.

## Troubleshooting

### Common Issues

1. **"RINGBA_API_TOKEN environment variable is required"**
   - Set the `RINGBA_API_TOKEN` environment variable

2. **"SLACK_WEBHOOK_URL environment variable is required"**
   - Set the `SLACK_WEBHOOK_URL` environment variable

3. **"Ringba API error 401"**
   - Check your API token is valid and has proper permissions

4. **"Slack webhook error 404"**
   - Verify your Slack webhook URL is correct

5. **No data in Slack messages**
   - Check if there's actual data in the time range
   - Verify the Ringba account ID is correct

### Logs

The system logs all activities. Check logs for:
- API call status
- Data parsing results
- Slack message delivery
- Error details

## Support

If you encounter issues:
1. Check the logs for error messages
2. Verify all environment variables are set correctly
3. Test with `test_monitor.py` first
4. Ensure your Ringba API token has proper permissions
