#!/usr/bin/env python
"""
Simple test script for SMTP email functionality with ud-mail.de
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ray_agent_demo.smtp_agent import send_test_email
import ray

def main():
    print("="*60)
    print("RAY SMTP AGENT - EMAIL TEST")
    print("="*60)
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
        result = send_test_email()
        
        if result["result"]["success"]:
            print("\n✅ SUCCESS! Email sent successfully!")
            print(f"   Message ID: {result['result']['message_id']}")
            print(f"   Recipients: {result['result']['recipients']}")
            print(f"\n📊 Metrics:")
            print(f"   Emails sent: {result['metrics']['emails_sent']}")
            print(f"   Success rate: {result['metrics']['success_rate']*100:.0f}%")
        else:
            print(f"\n❌ FAILED! Error: {result['result']['error']}")
            print("\nTroubleshooting tips:")
            print("1. Check your .env file has correct credentials")
            print("2. Verify SMTP_SERVER is 'smtps.udag.de'")
            print("3. Try port 465 if 587 doesn't work")
            print("4. Ensure your email account allows SMTP access")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease check:")
        print("1. Python dependencies are installed (run: pip install -r requirements.txt)")
        print("2. .env file exists with SMTP credentials")
        print("3. Ray is properly installed")
    
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    main()
