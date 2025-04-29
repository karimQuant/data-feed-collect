import logging
import sys

def setup_logging(level=logging.INFO):
    """
    Sets up centralized logging configuration for the application.

    Configures the root logger with a StreamHandler that outputs to stderr
    using a standard format including timestamp, logger name, level,
    filename, line number, and the message.

    Args:
        level: The minimum logging level to capture (e.g., logging.INFO, logging.DEBUG).
    """
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Prevent adding duplicate handlers if setup_logging is called multiple times
    if not root_logger.handlers:
        # Create a formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        # Create a handler (e.g., StreamHandler for console output)
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(formatter)

        # Add the handler to the root logger
        root_logger.addHandler(handler)

    # Optional: Disable propagation for loggers that might have handlers configured elsewhere
    # (though configuring the root logger is usually sufficient for a simple setup)
    # logging.getLogger('some_library').propagate = False

    # Log a confirmation message
    root_logger.info("Centralized logging configured.")

if __name__ == '__main__':
    # Example usage if you want to test the logging config file directly
    setup_logging(logging.DEBUG)
    logging.debug("This is a debug message.")
    logging.info("This is an info message.")
    logging.warning("This is a warning message.")
    logging.error("This is an error message.")
