"""
SMTP Email Agent using Ray for distributed email processing.

This module provides a Ray-based agent for sending emails via SMTP,
with support for credential loading from .env files and asynchronous processing.
"""

import os
import ray
import smtplib
import ssl
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class EmailMessage:
    """Email message data structure."""
    to: Union[str, List[str]]
    subject: str
    body: str
    body_type: str = "plain"  # "plain" or "html"
    cc: Optional[List[str]] = None
    bcc: Optional[List[str]] = None
    attachments: Optional[List[str]] = None
    timestamp: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if isinstance(self.to, str):
            self.to = [self.to]


@dataclass
class EmailResult:
    """Email sending result."""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None
    recipients: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


@ray.remote
class SMTPAgent:
    """
    Ray-based SMTP agent for distributed email processing.
    
    Features:
    - Credential management from .env
    - Connection pooling and reuse
    - Retry logic with exponential backoff
    - Rate limiting
    - Email queue management
    - Metrics tracking
    """
    
    def __init__(self):
        """Initialize the SMTP agent with credentials from environment."""
        # Load SMTP configuration
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_email = os.getenv("SMTP_EMAIL")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
        self.use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
        self.default_recipient = os.getenv("DEFAULT_RECIPIENT", "office@botworld.cloud")
        
        # Connection state
        self.connection = None
        self.last_activity = None
        self.connection_timeout = 300  # 5 minutes
        
        # Rate limiting
        self.rate_limit = 10  # emails per minute
        self.sent_timestamps = []
        
        # Metrics
        self.metrics = {
            "emails_sent": 0,
            "emails_failed": 0,
            "total_retry_attempts": 0,
            "connection_resets": 0,
            "start_time": datetime.now().isoformat()
        }
        
        # Validate credentials
        if not self.smtp_email or not self.smtp_password:
            raise ValueError(
                "SMTP credentials not found. Please set SMTP_EMAIL and SMTP_PASSWORD in .env file"
            )
        
        logger.info(f"SMTP Agent initialized with server: {self.smtp_server}:{self.smtp_port}")
    
    def _connect(self) -> bool:
        """Establish SMTP connection with the server."""
        try:
            if self.connection:
                # Try to use existing connection
                try:
                    self.connection.noop()
                    return True
                except:
                    self.connection = None
                    self.metrics["connection_resets"] += 1
            
            # Determine connection type based on port and settings
            if self.smtp_port == 465 or self.use_ssl:
                # SSL/TLS connection
                context = ssl.create_default_context()
                self.connection = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context)
                logger.info(f"Using SSL/TLS connection on port {self.smtp_port}")
            else:
                # Standard connection with optional STARTTLS
                self.connection = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.use_tls:
                    self.connection.starttls()
                    logger.info(f"Using STARTTLS on port {self.smtp_port}")
            
            # Enable debug if requested
            debug_mode = os.getenv("SMTP_DEBUG", "false").lower() == "true"
            if debug_mode:
                self.connection.set_debuglevel(1)
            
            # Login
            self.connection.login(self.smtp_email, self.smtp_password)
            self.last_activity = time.time()
            
            logger.info("SMTP connection established successfully")
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication failed: {e}")
            logger.error("Please verify your email and password are correct")
            self.connection = None
            return False
        except smtplib.SMTPServerDisconnected as e:
            logger.error(f"SMTP Server disconnected: {e}")
            logger.error("Try using port 465 with SSL or check server address")
            self.connection = None
            return False
        except Exception as e:
            logger.error(f"Failed to connect to SMTP server: {e}")
            logger.error(f"Server: {self.smtp_server}, Port: {self.smtp_port}")
            self.connection = None
            return False
    
    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits."""
        current_time = time.time()
        # Remove timestamps older than 1 minute
        self.sent_timestamps = [
            ts for ts in self.sent_timestamps 
            if current_time - ts < 60
        ]
        return len(self.sent_timestamps) < self.rate_limit
    
    def _create_message(self, email_msg: EmailMessage) -> MIMEMultipart:
        """Create MIME message from EmailMessage object."""
        msg = MIMEMultipart()
        msg['From'] = self.smtp_email
        msg['To'] = ', '.join(email_msg.to)
        msg['Subject'] = email_msg.subject
        msg['Date'] = formatdate(localtime=True)
        
        if email_msg.cc:
            msg['Cc'] = ', '.join(email_msg.cc)
        
        # Add body
        body = MIMEText(email_msg.body, email_msg.body_type)
        msg.attach(body)
        
        return msg
    
    def send_email(self, 
                   to: Optional[Union[str, List[str]]] = None,
                   subject: str = "Test Email from Ray SMTP Agent",
                   body: Optional[str] = None,
                   **kwargs) -> Dict:
        """
        Send an email with retry logic and rate limiting.
        
        Args:
            to: Recipient email(s). Defaults to DEFAULT_RECIPIENT from .env
            subject: Email subject
            body: Email body. If None, uses a dummy body
            **kwargs: Additional email parameters (cc, bcc, body_type, etc.)
        
        Returns:
            Dictionary with send result and metrics
        """
        # Use defaults if not provided
        if to is None:
            to = self.default_recipient
        
        if body is None:
            body = f"""
Hello,

This is a test email sent from the Ray SMTP Agent.

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Agent ID: {ray.get_runtime_context().actor_id if ray.is_initialized() else 'N/A'}
SMTP Server: {self.smtp_server}
From: {self.smtp_email}

This is an automated message demonstrating the distributed email capability
of the Ray framework with SMTP integration.

Best regards,
Ray SMTP Agent
"""
        
        # Create email message
        email_msg = EmailMessage(
            to=to,
            subject=subject,
            body=body,
            **kwargs
        )
        
        # Check rate limit
        if not self._check_rate_limit():
            return {
                "result": asdict(EmailResult(
                    success=False,
                    error="Rate limit exceeded. Please wait before sending more emails."
                )),
                "metrics": self.get_metrics()
            }
        
        # Retry logic
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Ensure connection
                if not self._connect():
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))
                        self.metrics["total_retry_attempts"] += 1
                        continue
                    else:
                        raise Exception("Failed to establish SMTP connection")
                
                # Create and send message
                mime_msg = self._create_message(email_msg)
                all_recipients = email_msg.to + (email_msg.cc or []) + (email_msg.bcc or [])
                
                # Send the email
                self.connection.send_message(mime_msg)
                
                # Update metrics
                self.sent_timestamps.append(time.time())
                self.metrics["emails_sent"] += 1
                self.last_activity = time.time()
                
                result = EmailResult(
                    success=True,
                    message_id=f"ray-smtp-{int(time.time()*1000)}",
                    recipients=all_recipients
                )
                
                logger.info(f"Email sent successfully to {all_recipients}")
                
                return {
                    "result": asdict(result),
                    "metrics": self.get_metrics()
                }
                
            except Exception as e:
                logger.error(f"Email send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                    self.metrics["total_retry_attempts"] += 1
                else:
                    self.metrics["emails_failed"] += 1
                    result = EmailResult(
                        success=False,
                        error=str(e),
                        recipients=email_msg.to
                    )
                    return {
                        "result": asdict(result),
                        "metrics": self.get_metrics()
                    }
    
    def get_metrics(self) -> Dict:
        """Get current agent metrics."""
        runtime = (
            datetime.now() - datetime.fromisoformat(self.metrics["start_time"])
        ).total_seconds()
        
        return {
            **self.metrics,
            "runtime_seconds": runtime,
            "emails_per_minute": (
                self.metrics["emails_sent"] / (runtime / 60) 
                if runtime > 0 else 0
            ),
            "success_rate": (
                self.metrics["emails_sent"] / 
                (self.metrics["emails_sent"] + self.metrics["emails_failed"])
                if (self.metrics["emails_sent"] + self.metrics["emails_failed"]) > 0
                else 0
            )
        }
    
    def close(self):
        """Close SMTP connection and cleanup."""
        if self.connection:
            try:
                self.connection.quit()
            except:
                pass
            self.connection = None
        logger.info("SMTP Agent connection closed")
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()


# Convenience functions for standalone usage
def create_smtp_agent():
    """Create and return an SMTP agent actor."""
    if not ray.is_initialized():
        ray.init()
    return SMTPAgent.remote()


def send_test_email():
    """Send a test email to the default recipient."""
    agent = create_smtp_agent()
    result = ray.get(agent.send_email.remote())
    ray.get(agent.close.remote())
    return result


if __name__ == "__main__":
    # Example usage
    print("Initializing Ray SMTP Agent...")
    print(f"Using SMTP server: {os.getenv('SMTP_SERVER')}")
    print(f"From: {os.getenv('SMTP_EMAIL')}")
    print(f"To: {os.getenv('DEFAULT_RECIPIENT')}")
    print("-" * 40)
    
    result = send_test_email()
    print(f"\nEmail Send Result: {json.dumps(result, indent=2)}")
    
    if ray.is_initialized():
        ray.shutdown()
