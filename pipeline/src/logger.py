"""
Tree11 Data Pipeline - Logger Module
Structured logging setup with performance metrics and notification system
"""

import logging
import logging.handlers
import os
import sys
import json
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, Any, Optional

import structlog


class NotificationHandler:
    """
    Handles email notification methods
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize notification handler
        
        Args:
            config: Notification configuration
        """
        self.config = config
        self.email_config = config.get('email', {})
        self.levels = config.get('notification_levels', {})
    
    def send_notification(self, level: str, title: str, message: str, 
                         details: Optional[Dict] = None):
        """
        Send notification through configured channels
        
        Args:
            level: Notification level (success, warning, error, info)
            title: Notification title
            message: Notification message
            details: Optional additional details
        """
        if not self.config.get('enabled', False):
            return
        
        if not self.levels.get(level, False):
            return
        
        # Send email notification
        if self.email_config.get('enabled', False):
            self._send_email(level, title, message, details)
    
    def _send_email(self, level: str, title: str, message: str, 
                   details: Optional[Dict] = None):
        """Send email notification"""
        try:
            smtp_server = self.email_config['smtp_server']
            smtp_port = self.email_config['smtp_port']
            username = self.email_config['username']
            password = self.email_config['password']
            from_address = self.email_config['from_address']
            to_addresses = self.email_config['to_addresses']
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_address
            msg['To'] = ', '.join(to_addresses)
            msg['Subject'] = f"[Tree11 Pipeline] {level.upper()}: {title}"
            
            # Create HTML body
            html_body = self._create_email_html(level, title, message, details)
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            if self.email_config.get('use_tls', True):
                server.starttls()
            server.login(username, password)
            server.send_message(msg)
            server.quit()
            
            structlog.get_logger().info("Email notification sent",
                                      level=level,
                                      title=title)
            
        except Exception as e:
            structlog.get_logger().error("Failed to send email notification",
                                       error=str(e))
    
    def _create_email_html(self, level: str, title: str, message: str,
                          details: Optional[Dict] = None) -> str:
        """Create HTML email body"""
        color_map = {
            'success': '#28a745',
            'warning': '#ffc107',
            'error': '#dc3545',
            'info': '#17a2b8'
        }
        
        color = color_map.get(level, '#17a2b8')
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .header {{ background-color: {color}; color: white; padding: 15px; border-radius: 5px; }}
                .content {{ padding: 20px; background-color: #f8f9fa; margin: 10px 0; border-radius: 5px; }}
                .details {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid {color}; }}
                .footer {{ color: #6c757d; font-size: 12px; margin-top: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Tree11 Data Pipeline - {level.upper()}</h2>
                <p>{title}</p>
            </div>
            
            <div class="content">
                <h3>Message:</h3>
                <p>{message}</p>
            </div>
        """
        
        if details:
            html += """
            <div class="details">
                <h3>Details:</h3>
                <table>
                    <tr><th>Property</th><th>Value</th></tr>
            """
            for key, value in details.items():
                html += f"<tr><td>{key}</td><td>{value}</td></tr>"
            html += "</table></div>"
        
        html += f"""
            <div class="footer">
                <p>Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Tree11 Data Pipeline - Automated Notification System</p>
            </div>
        </body>
        </html>
        """
        
        return html


class PerformanceLogger:
    """
    Performance metrics and monitoring logger
    """
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
        
    def start_timer(self, operation: str):
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str) -> float:
        """End timing an operation and return duration"""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            self.metrics[operation] = duration
            del self.start_times[operation]
            return duration
        return 0.0
    
    def record_metric(self, name: str, value: Any):
        """Record a custom metric"""
        self.metrics[name] = value
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all recorded metrics"""
        return self.metrics.copy()
    
    def log_metrics(self):
        """Log all metrics"""
        logger = structlog.get_logger()
        for name, value in self.metrics.items():
            logger.info("Performance metric - metric=%s, value=%s", name, value)


def setup_logging(log_level: int = logging.INFO, 
                 log_dir: Optional[str] = None,
                 enable_console: bool = True,
                 enable_file: bool = True,
                 enable_json: bool = True) -> structlog.stdlib.BoundLogger:
    """
    Setup structured logging for the Tree11 pipeline
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files (defaults to 'logs')
        enable_console: Enable console logging
        enable_file: Enable file logging
        enable_json: Enable JSON structured logging
        
    Returns:
        Configured structlog logger
    """
    # Create logs directory if needed
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.add_log_level,
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FUNC_NAME,
                           structlog.processors.CallsiteParameter.LINENO]
            ),
            structlog.processors.JSONRenderer() if enable_json else structlog.dev.ConsoleRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
    
    # File handlers
    if enable_file:
        # Main log file (rotating)
        main_log_file = os.path.join(log_dir, 'tree11_pipeline.log')
        file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(log_level)
        
        if enable_json:
            file_formatter = logging.Formatter('%(message)s')
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
        
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # Error log file (errors only)
        error_log_file = os.path.join(log_dir, 'tree11_errors.log')
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=10
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        root_logger.addHandler(error_handler)
    
    # Create and return structlog logger
    logger = structlog.get_logger("tree11.pipeline")
    logger.info("Logging system initialized",
               log_level=logging.getLevelName(log_level),
               log_dir=log_dir,
               console_enabled=enable_console,
               file_enabled=enable_file,
               json_enabled=enable_json)
    
    return logger


def setup_simple_logging(log_level: int = logging.INFO) -> logging.Logger:
    """
    Setup simple, readable Python logging only to console/terminal
    
    Args:
        log_level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create simple formatter with only datetime, level and message
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler - only output to terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Return a standard logger
    logger = logging.getLogger("tree11.pipeline")
    logger.info("Simple logging system initialized - log_level=%s", 
               logging.getLevelName(log_level))
    
    return logger


def send_notification(level: str, message: str, config: Dict[str, Any]):
    """
    Send notification using configured channels
    
    Args:
        level: Notification level (SUCCESS, WARNING, ERROR, INFO)
        message: Notification message
        config: Notification configuration
    """
    try:
        notification_handler = NotificationHandler(config)
        notification_handler.send_notification(
            level.lower(),
            "Tree11 Pipeline Notification",
            message
        )
    except Exception as e:
        structlog.get_logger().error("Failed to send notification",
                                   level=level,
                                   message=message,
                                   error=str(e))


def main():
    """Main function for testing logging setup"""
    # Test logging setup
    logger = setup_logging(logging.DEBUG)
    
    # Test different log levels
    logger.debug("Debug message")
    logger.info("Info message - extra_field=%s", "test_value")
    logger.warning("Warning message - warning_code=%d", 123)
    logger.error("Error message - error_details=%s", {"code": 500, "message": "Test error"})
    
    # Test performance logging
    perf_logger = PerformanceLogger()
    perf_logger.start_timer("test_operation")
    time.sleep(1)
    duration = perf_logger.end_timer("test_operation")
    perf_logger.record_metric("test_metric", 42)
    perf_logger.log_metrics()
    
    print(f"Test operation took {duration:.2f} seconds")
    print("Logging test completed successfully")


if __name__ == '__main__':
    main() 