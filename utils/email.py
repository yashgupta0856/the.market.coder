import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SMTP_EMAIL = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")


def send_password_reset_email(to_email: str, reset_link: str):
    """
    Sends a branded password reset email via Gmail SMTP.
    """
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        logger.warning("SMTP credentials not set. Skipping password reset email.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Reset Your Password — the.market.coder"
    msg["From"] = f"the.market.coder <{SMTP_EMAIL}>"
    msg["To"] = to_email

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;
                background: #0f172a; color: #f1f5f9; padding: 40px; border-radius: 16px;">

        <div style="font-size: 22px; font-weight: 800; margin-bottom: 8px;
                    background: linear-gradient(135deg, #60a5fa, #3b82f6, #10b981);
                    -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            the.market.coder
        </div>
        <div style="font-size: 13px; color: #94a3b8; margin-bottom: 32px;">
            AI-powered stock discovery for Indian markets
        </div>

        <h2 style="margin: 0 0 12px 0; font-size: 24px; color: #f1f5f9;">
            Password Reset Request
        </h2>
        <p style="color: #94a3b8; font-size: 15px; line-height: 1.6; margin-bottom: 28px;">
            We received a request to reset the password for your account.
            Click the button below to choose a new password.
            This link expires in <strong style="color: #f1f5f9;">1 hour</strong>.
        </p>

        <a href="{reset_link}"
           style="display: inline-block; background: linear-gradient(135deg, #3b82f6, #2563eb);
                  color: #fff; padding: 14px 32px; border-radius: 12px; font-size: 15px;
                  font-weight: 600; text-decoration: none;
                  box-shadow: 0 4px 14px rgba(59, 130, 246, 0.4);">
            Reset Password
        </a>

        <p style="color: #64748b; font-size: 13px; margin-top: 32px; line-height: 1.5;">
            If you didn't request this, you can safely ignore this email.
            Your password will remain unchanged.
        </p>

        <div style="border-top: 1px solid rgba(255,255,255,0.08); margin-top: 32px; padding-top: 20px;
                    font-size: 12px; color: #475569;">
            © the.market.coder · AI Market Intelligence
        </div>
    </div>
    """

    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())

        logger.info(f"Password reset email sent to {to_email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send password reset email: {e}")
        return False
