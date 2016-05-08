import telepot
from config import config

class Notifier(object):
    def __init__(self, receivers=None):
        self.bot = telepot.Bot(config.telegram_bot_id)
        if receivers:
            self.receivers = receivers
        else:
            self.receivers = config.notification.receivers

    def send_message(self, msg, receivers=None):
        if receivers is None:
            receivers = self.receivers
        for name, no in receivers.items():
            self.bot.sendMessage(no['telegram'], msg)
