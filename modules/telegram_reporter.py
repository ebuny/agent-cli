import os
import requests
import logging

log = logging.getLogger("telegram_reporter")

class TelegramReporter:
    """Simple reporter to push trade events to a Telegram channel/chat."""
    
    def __init__(self):
        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.bot_token and self.chat_id)
        if not self.enabled:
            log.info("TelegramReporter disabled (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")

    def send_message(self, text: str) -> None:
        """Send a markdown message to the configured chat."""
        if not self.enabled:
            return
            
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code != 200:
                log.warning("Telegram send failed: %s", resp.text)
        except Exception as e:
            log.warning("Telegram connection error: %s", e)
