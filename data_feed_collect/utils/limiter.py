"""Centralized RateLimiter instance for managing API call rates."""

# Import the correct classes: Limiter, Rate, Duration
from pyrate_limiter import Limiter, Rate, Duration

# Define the key used for rate limiting Yahoo Finance requests
# This is defined here to keep the limiter configuration self-contained
YAHOO_FINANCE_LIMIT_KEY = 'yahoo_finance'

# Define the rate for Yahoo Finance: 60 calls per minute
yahoo_finance_rate = Rate(limit=60, interval=Duration.MINUTE)

# Initialize the Limiter instance with the defined rate(s)
# This instance will be shared across the application
limiter = Limiter(yahoo_finance_rate)

# You can add more rates for different APIs here if needed
# Example: another_api_rate = Rate(limit=100, interval=Duration.HOUR)
# limiter = Limiter([yahoo_finance_rate, another_api_rate])

# The 'limiter' object is now ready to be imported and used
