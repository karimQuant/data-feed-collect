"""Centralized RateLimiter instance for managing API call rates."""

from pyratelimiter import RateLimiter

# Define the key used for rate limiting Yahoo Finance requests
# This is defined here to keep the limiter configuration self-contained
YAHOO_FINANCE_LIMIT_KEY = 'yahoo_finance'

# Initialize the RateLimiter instance
# This instance will be shared across the application
limiter = RateLimiter()

# Add the rate limit rule for Yahoo Finance: 60 calls per minute
# The key 'yahoo_finance' must match the key used in the collectors
limiter.add_rule(calls=60, period=60, key=YAHOO_FINANCE_LIMIT_KEY)

# You can add more rules for different APIs here if needed
# Example: limiter.add_rule(calls=100, period=3600, key='another_api')

# The 'limiter' object is now ready to be imported and used
