import logging
import sys

def setup_logging():
    """
    Sets up the basic logging configuration for the project.
    """
    # Define the desired log format
    # Includes timestamp, logger name, log level, filename, line number, and message
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'

    # Configure the root logger
    logging.basicConfig(
        level=logging.INFO,  # Set the minimum logging level
        format=log_format,
        handlers=[
            # Log to console (standard output)
            logging.StreamHandler(sys.stdout)
            # You could add other handlers here, e.g., FileHandler for logging to a file
            # logging.FileHandler("app.log")
        ]
    )

    # Optional: Set logging level for specific libraries if they are too noisy
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    # logging.getLogger('yfinance').setLevel(logging.WARNING) # yfinance might not use standard logging


if __name__ == '__main__':
    # Example usage of the configured logger
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Logging setup complete.")
    logger.debug("This is a debug message (won't show with INFO level).")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
