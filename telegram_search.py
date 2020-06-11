from functools import partial

from telegram import Update, Message
from telegram.ext import Updater, Dispatcher, MessageHandler, Filters, CommandHandler, CallbackContext

from lib.config import parse, TelegramConfig
from lib.db import UDB
from lib.utils import TargetType


def handle_start(update: Update, context: CallbackContext):
    message: Message = update.message
    message.reply_text("""你好！欢迎使用本bot
直接转发消息可以搜索图片来源，只能搜索本bot发送的图片，太旧的图片可能也会搜不出来，还请谅解
回复/help 可以再看一次! """)


def handle_forward(db: UDB, update: Update, context: CallbackContext):
    message: Message = update.message
    print(message)
    chat_name = message.forward_from_chat.username
    message_id = message.forward_from_message_id
    tid = f'{chat_name}/{message_id}'
    result = db.reversed_index_get(TargetType.Telegram, tid)
    if result is None:
        reply = "抱歉，我不记得我发过这张图了"
    else:
        reply = f"由 {result.type.name} 的 {result.author} 创作:\n" + \
                f"正文: {result.content}\n" + \
                f"来源: {result.source}"
    message.reply_text(reply)


def get_updater(config: TelegramConfig) -> Updater:
    return Updater(config.token, use_context=True)


def main():
    with open('config.toml') as cf:
        config = parse(cf)
    updater = get_updater(config.telegram)
    with UDB(config.redis) as db:
        dp: Dispatcher = updater.dispatcher
        dp.add_handler(CommandHandler("start", handle_start))
        dp.add_handler(CommandHandler("help", handle_start))
        dp.add_handler(MessageHandler(Filters.forwarded, partial(handle_forward, db)))
        updater.start_polling()
        print("Bot Started")
        updater.idle()


if __name__ == '__main__':
    main()
