# SMTP Email Setup Guide

## ✅ Configuration Complete!

Your `.env` file has been configured with:
- **Email**: ian.baumeister@botworld.cloud
- **Server**: smtps.udag.de (United Domains)
- **Port**: 587 (STARTTLS)
- **Default recipient**: office@botworld.cloud

## 🚀 Quick Test

1. **Install dependencies** (if not already done):
   ```bash
   pip install ray python-dotenv
   ```

2. **Run the email test**:
   ```bash
   python test_email.py
   ```

3. **Or test directly with the SMTP agent**:
   ```bash
   python -m ray_agent_demo.smtp_agent
   ```

## 📧 Usage Examples

### Send a simple test email:
```python
from ray_agent_demo import send_test_email

result = send_test_email()
if result["result"]["success"]:
    print("Email sent successfully!")
```

### Send a custom email:
```python
import ray
from ray_agent_demo import SMTPAgent

ray.init()
agent = SMTPAgent.remote()

result = ray.get(agent.send_email.remote(
    to="office@botworld.cloud",
    subject="Custom Subject",
    body="Your custom email body here"
))

ray.get(agent.close.remote())
ray.shutdown()
```

## 🔧 Troubleshooting

If you encounter issues:

1. **Connection errors**: 
   - Try port 465 instead of 587 by editing `.env`:
     ```
     SMTP_PORT=465
     SMTP_USE_TLS=false
     SMTP_USE_SSL=true
     ```

2. **Authentication errors**:
   - Verify your password is correct
   - Check if your account allows SMTP access
   - Some providers require app-specific passwords

3. **Debug mode**:
   - Set `SMTP_DEBUG=true` in `.env` to see detailed logs

## 📝 Features

- ✅ Connection pooling for efficiency
- ✅ Automatic retry with exponential backoff
- ✅ Rate limiting (10 emails/minute)
- ✅ Comprehensive metrics tracking
- ✅ Ray distributed processing
- ✅ Async email sending

## 🔒 Security Note

The `.env` file is already in `.gitignore` so your credentials won't be committed to git.

---

Ready to send emails! Run `python test_email.py` to start.
