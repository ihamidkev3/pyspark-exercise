import logging

class Logger:
    def __init__(self, log_file="assignment.log", log_level=logging.DEBUG):
        self.logger = logging.getLogger("brand_processing")
        self.logger.setLevel(log_level)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Console handler for debug and higher
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def debug(self, message):
        """Log debug level messages for detailed debugging information."""
        self.logger.debug(message)

    def info(self, message):
        """Log info level messages for general process information."""
        self.logger.info(message)

    def warning(self, message):
        """Log warning level messages for concerning but non-critical issues."""
        self.logger.warning(message)

    def error(self, message):
        """Log error level messages for issues that prevent normal operation."""
        self.logger.error(message)
        
    def critical(self, message):
        """Log critical level messages for severe errors that require immediate attention."""
        self.logger.critical(message)

# Create default logger instance with DEBUG level
LOGGER = Logger()