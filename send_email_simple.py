#!/usr/bin/env python
"""
Standalone SMTP Email Script for ud-mail.de
Works without Ray dependency
"""

import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SimpleEmailSender:
    """Simple email sender without Ray dependency."""
    
    def __init__(self):
        """Initialize with credentials from .env file."""
        self.smtp_server = os.getenv("SMTP_SERVER", "smtps.udag.de")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_email = os.getenv("SMTP_EMAIL")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
        self.default_recipient = os.getenv("DEFAULT_RECIPIENT", "office@botworld.cloud")
        
        if not self.smtp_email or not self.smtp_password:
            raise ValueError("Please set SMTP_EMAIL and SMTP_PASSWORD in .env file")
    
    def send_email(self, to=None, subject=None, body=None):
        """Send an email via SMTP."""
        # Use defaults
        if to is None:
            to = self.default_recipient
        
        if subject is None:
            subject = "Test Email from Python SMTP Script"
        
        if body is None:
            body = f"""
Hello,

This is a test email sent via SMTP from Python.

Configuration:
- Server: {self.smtp_server}:{self.smtp_port}
- From: {self.smtp_email}
- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This email confirms that your SMTP configuration is working correctly
with ud-mail.de (United Domains).

Best regards,
Python SMTP Script
"""
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.smtp_email
            msg['To'] = to
            msg['Subject'] = subject
            msg['Date'] = formatdate(localtime=True)
            msg.attach(MIMEText(body, 'plain'))
            
            # Connect and send
            print(f"Connecting to {self.smtp_server}:{self.smtp_port}...")
            
            if self.smtp_port == 465:
                # SSL connection
                context = ssl.create_default_context()
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context)
            else:
                # STARTTLS connection
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.use_tls:
                    server.starttls()
            
            print("Authenticating...")
            server.login(self.smtp_email, self.smtp_password)
            
            print("Sending email...")
            server.send_message(msg)
            server.quit()
            
            return True, "Email sent successfully!"
            
        except smtplib.SMTPAuthenticationError as e:
            return False, f"Authentication failed: {str(e)}"
        except smtplib.SMTPException as e:
            return False, f"SMTP error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"


def main():
    """Main function to test email sending."""
    print("="*60)
    print("SMTP EMAIL TEST (Standalone)")
    print("="*60)
    
    # First install python-dotenv if needed
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("\n⚠️  python-dotenv not installed. Installing...")
        import subprocess
        subprocess.check_call([os.sys.executable, "-m", "pip", "install", "python-dotenv"])
        from dotenv import load_dotenv
    
    # Load environment
    load_dotenv()
    
    print(f"\nConfiguration:")
    print(f"  Server: {os.getenv('SMTP_SERVER', 'Not set')}")
    print(f"  Port: {os.getenv('SMTP_PORT', 'Not set')}")
    print(f"  From: {os.getenv('SMTP_EMAIL', 'Not set')}")
    print(f"  To: {os.getenv('DEFAULT_RECIPIENT', 'Not set')}")
    print("="*60)
    
    response = input("\nSend test email? (y/n): ")
    if response.lower() != 'y':
        print("Test cancelled.")
        return
    
    print("\nSending test email...")
    
    try:
        sender = SimpleEmailSender()
        success, message = sender.send_email()
        
        if success:
            print(f"\n✅ SUCCESS! {message}")
            print(f"   Email sent to: {sender.default_recipient}")
            print(f"   From: {sender.smtp_email}")
        else:
            print(f"\n❌ FAILED! {message}")
            print("\nTroubleshooting tips:")
            print("1. Check your .env file has correct credentials")
            print("2. Verify SMTP_SERVER is 'smtps.udag.de'")
            print("3. Try port 465 if 587 doesn't work")
            print("4. Ensure your email account allows SMTP access")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease check:")
        print("1. .env file exists with SMTP credentials")
        print("2. Credentials are correct")


if __name__ == "__main__":
    main()
