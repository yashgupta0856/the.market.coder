import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_community_alert(symbol: str, entry: str, stop_loss: str, target: str, commentary: str, image_url: str):
    """
    Sends a formatted message to the Telegram channel when a new community post is made.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram Bot Token or Chat ID is missing. Skipping alert.")
        return

    # Check if chat id needs the @ prefix for public channels
    chat_id = TELEGRAM_CHAT_ID
    if not chat_id.startswith("@") and not chat_id.startswith("-100"):
        # Assume it's a public channel handle if no prefix
        chat_id = f"@{chat_id}"

    caption = f"🚀 <b>NEW STOCK ALERT: {symbol}</b>\n\n"
    
    if entry:
        caption += f"🟢 <b>Entry:</b> {entry}\n"
    if target:
        caption += f"🎯 <b>Target:</b> {target}\n"
    if stop_loss:
        caption += f"🛑 <b>Stop Loss:</b> {stop_loss}\n"
        
    if commentary:
        caption += f"\n📝 <b>Commentary:</b>\n{commentary}\n"

    try:
        if image_url:
            # If there's an image, send as a photo payload
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            payload = {
                "chat_id": chat_id,
                "photo": image_url,
                "caption": caption,
                "parse_mode": "HTML"
            }
        else:
            # If no image, send as normal text message
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": caption,
                "parse_mode": "HTML"
            }

        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code != 200:
            logging.error(f"Failed to send Telegram alert: {response.text}")
        else:
            logging.info(f"Telegram alert sent successfully for {symbol}")
            
    except Exception as e:
        logging.error(f"Exception while sending telegram alert: {e}")
