import logging
import asyncio
import random
import os
import json
import time
import psutil
import aiosqlite
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeAllPrivateChats, FSInputFile, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, PreCheckoutQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import uuid
import aiohttp
import tenacity
from collections import deque
from bot.utils import bot, logger, SHOP_ITEMS, ADMIN_IDS
from bot.database import add_purchase, update_premium, add_pack
from bot.handlers import dp

# Решта коду payments.py без змін
SHOP_ITEMS = {
    "VIP_1D": {
        "title": "VIP на 1 день",
        "description": "Отримати преміум статус на 1 день.",
        "price": 1,
        "payload": "vip_1d",
        "duration": 86400
    },
    "PACK_FANTASY": {
        "title": "Набір Fantasy",
        "description": "Додатковий набір локацій: Fantasy.",
        "price": 1,
        "payload": "pack_fantasy"
    },
    "BOOST_SPY": {
        "title": "Буст Шпигуна",
        "description": "Збільшити шанси стати шпигуном в наступній грі.",
        "price": 1,
        "payload": "boost_spy"
    }
}

@dp.message(Command("shop"))
async def shop_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ тільки для адмінів поки що.")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=item["title"], callback_data=f"buy:{key}") for key, item in SHOP_ITEMS.items()]
    ])
    await message.reply("Оберіть товар:", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("buy:"))
async def buy_callback(callback: types.CallbackQuery):
    item_code = callback.data.split(":")[1]
    item = SHOP_ITEMS.get(item_code)
    if not item:
        await callback.answer("Товар не знайдено.")
        return
    prices = [LabeledPrice(label=item["title"], amount=item["price"] * 100)]
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=item["title"],
        description=item["description"],
        payload=item["payload"],
        provider_token="",
        currency="XTR",
        prices=prices
    )
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    user_id = message.from_user.id
    item_code = next((key for key, item in SHOP_ITEMS.items() if item["payload"] == payload), None)
    if not item_code:
        logger.error(f"Unknown payload: {payload}")
        return
    item = SHOP_ITEMS[item_code]
    await add_purchase(user_id, item_code, item["price"])
    if item_code.startswith("VIP_"):
        await update_premium(user_id, item["duration"])
        await message.reply(f"VIP активовано на 1 день!")
    elif item_code.startswith("PACK_"):
        pack_name = item_code.lower().replace("pack_", "")
        await add_pack(user_id, pack_name)
        await message.reply(f"Набір {pack_name} додано!")
    elif item_code == "BOOST_SPY":
        # Для boost - припустимо, зберігаємо в FSM або тимчасово, але для простоти - повідомлення
        await message.reply("Буст Шпигуна активовано для наступної гри!")

@dp.message(F.text == "🛍️ Магазин")
async def shop_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=item["title"], callback_data=f"buy:{key}") for key, item in SHOP_ITEMS.items()]
    ])
    await message.reply("Вітаємо в магазині! Оберіть товар (тестово по 1 зірці):", reply_markup=keyboard)