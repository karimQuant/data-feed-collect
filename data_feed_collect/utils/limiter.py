"""Centralized RateLimiter instance for managing API call rates."""

# Import the correct classes: Limiter, Rate, Duration
from pyrate_limiter import Limiter, Rate, Duration

# Define the key used for rate limiting Yahoo Finance requests
# This is defined here to keep the limiter configuration self-contained
YAHOO_FINANCE_LIMIT_KEY = 'yahoo_finance'

# Define the rate for Yahoo Finance: 60 calls per minute
yahoo_finance_rate = Rate(limit=60, interval=Duration.MINUTE)

# Initialize the Limiter instance with the defined rate(s)
# Configure it to NOT raise an exception immediately (raise_when_fail=False)
# and to wait up to max_delay milliseconds if the limit is hit.
# 60000 ms = 60 seconds, which should be enough for a 60/min rate.
limiter = Limiter(
    yahoo_finance_rate,
    raise_when_fail=False, # Set to False to enable waiting via max_delay
    max_delay=60000        # Set a maximum delay in milliseconds (e.g., 60 seconds)
)

# You can add more rates for different APIs here if needed
# Example: another_api_rate = Rate(limit=100, interval=Duration.HOUR)
# limiter = Limiter([yahoo_finance_rate, another_api_rate], raise_when_fail=False, max_delay=60000)

# The 'limiter' object is now ready to be imported and used
