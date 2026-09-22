# ====================================================================
# Central Bank Bot - Version 1.1
# ====================================================================
import os
import io
import re
import sys
import time
import asyncio
import logging
import signal
import threading
import urllib.parse
from datetime import datetime, timezone, timedelta, time as dtime

import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flask import Flask
from waitress import serve
from motor.motor_asyncio import AsyncIOMotorClient
from huggingface_hub import InferenceClient

import discord
from discord.ext import commands, tasks
from unbelievaboat import Client as UBClient


# ====================================================================
# КОНФИГУРАЦИЯ
# ====================================================================

BOT_VERSION = "1.1.0"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash-0731"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("CentralBank")

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
UB_TOKEN = os.environ.get("UB_TOKEN")
HF_TOKEN = os.environ.get("HF_TOKEN")

MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "central_bank")

GUILD_ID = int(os.environ.get("GUILD_ID", "0"))
REPORT_CHANNEL_ID = int(os.environ.get("REPORT_CHANNEL_ID", "0"))
PORT = int(os.environ.get("PORT", "10000"))

SALARY_AMOUNT = int(os.environ.get("SALARY_AMOUNT", "500"))
SALARY_COOLDOWN_HOURS = int(os.environ.get("SALARY_COOLDOWN_HOURS", "24"))

COOLDOWN_CACHE_TTL = 300
RATE_CACHE_TTL = 3600

USD_CODE = "R01235"
EUR_CODE = "R01239"
CNY_CODE = "R01375"

# Время отчёта (МСК = UTC+3)
REPORT_HOUR_MSK = 21
REPORT_HOUR_UTC = REPORT_HOUR_MSK - 3


def build_mongo_uri() -> str:
    username = urllib.parse.quote_plus(MONGO_USER)
    password = urllib.parse.quote_plus(MONGO_PASSWORD)
    return (
        f"mongodb+srv://{username}:{password}"
        f"@cluster0.u62aem5.mongodb.net/?appName=CentralBank"
    )


def validate_config():
    missing = []
    if not DISCORD_TOKEN:
        missing.append("DISCORD_TOKEN")
    if not UB_TOKEN:
        missing.append("UB_TOKEN")
    if not MONGO_USER:
        missing.append("MONGO_USER")
    if not MONGO_PASSWORD:
        missing.append("MONGO_PASSWORD")
    if GUILD_ID == 0:
        missing.append("GUILD_ID")

    if missing:
        logger.critical(f"❌ Не заданы: {', '.join(missing)}")
        while True:
            time.sleep(60)

    if not HF_TOKEN:
        logger.warning("⚠️ HF_TOKEN не задан — отчёты будут без нейросети")


# ====================================================================
# FLASK ДЛЯ RENDER
# ====================================================================

web_app = Flask(__name__)


@web_app.route('/')
def home():
    return f"Online 🏦 Central Bank {BOT_VERSION}"


@web_app.route('/health')
def health():
    return {"status": "ok", "version": BOT_VERSION}, 200


def run_flask():
    serve(web_app, host='0.0.0.0', port=PORT, threads=4)


# ====================================================================
# КЭШ КУРСОВ ВАЛЮТ
# ====================================================================

class CurrencyCache:
    def __init__(self, ttl: int = RATE_CACHE_TTL):
        self.ttl = ttl
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get_rate(self, code: str) -> float:
        now = time.time()
        async with self._lock:
            cached = self._cache.get(code)
            if cached and now - cached[1] < self.ttl:
                return cached[0]

        rate = await asyncio.get_event_loop().run_in_executor(
            None, self._fetch_rate_sync, code
        )

        async with self._lock:
            self._cache[code] = (rate, now)
        return rate

    @staticmethod
    def _fetch_rate_sync(code: str) -> float:
        try:
            r = requests.get("https://www.cbr.ru/scripts/XML_daily.asp", timeout=10)
            pattern = rf'<Valute ID="{code}">.*?<Value>(.*?)</Value>'
            match = re.search(pattern, r.text, re.DOTALL)
            if match:
                return float(match.group(1).replace(",", "."))
        except Exception as e:
            logger.error(f"Ошибка курса {code}: {e}")
        return 0.0

    async def cleanup_loop(self):
        while True:
            await asyncio.sleep(RATE_CACHE_TTL)
            now = time.time()
            async with self._lock:
                expired = [k for k, (_, t) in self._cache.items() if now - t > RATE_CACHE_TTL * 2]
                for k in expired:
                    del self._cache[k]


currency_cache = CurrencyCache()


# ====================================================================
# МЕНЕДЖЕР КУЛДАУНОВ (атомарный)
# ====================================================================

class CooldownManager:
    def __init__(self, db):
        self.db = db

    async def check_and_set(self, user_id: str, command: str, hours: int):
        """
        Атомарный кулдаун через find_one_and_update.
        Возвращает (allowed, message).
        """
        key = f"{user_id}_{command}"
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(hours=hours)

        # Атомарно: обновляем только если last_used < threshold
        result = await self.db.find_one_and_update(
            {
                "_id": key,
                "last_used": {"$lt": threshold}
            },
            {"$set": {"last_used": now}},
            upsert=False,
            return_document=False
        )

        if result:
            # Кулдаун был истёк → обновили → можно
            return True, ""

        # Либо документа нет (первый раз), либо кулдаун активен
        existing = await self.db.find_one({"_id": key})
        if existing is None:
            # Первый раз — создаём
            try:
                await self.db.insert_one({"_id": key, "last_used": now})
                return True, ""
            except Exception:
                # Гонка: кто-то успел вставить — значит кулдаун активен
                existing = await self.db.find_one({"_id": key})

        # Кулдаун активен
        last_used = existing["last_used"]
        left = timedelta(hours=hours) - (now - last_used)
        h = int(left.total_seconds() // 3600)
        m = int((left.total_seconds() % 3600) // 60)
        return False, f"{h}ч {m}м"


# ====================================================================
# ОСНОВНОЙ БОТ
# ====================================================================

class CentralBankBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            activity=discord.Game(name="Центробанк"),
            status=discord.Status.online
        )

        self.mongo_client = None
        self.db = None
        self.economy_collection = None
        self.transactions_collection = None
        self.cooldowns = None
        self.ub = None
        self.hf_client = None
        self._currency_cleanup_task = None

    async def setup_hook(self):
        # MongoDB
        try:
            self.mongo_client = AsyncIOMotorClient(
                build_mongo_uri(),
                serverSelectionTimeoutMS=10000,
                maxPoolSize=10,
                minPoolSize=2
            )
            await self.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB подключена")
        except Exception as e:
            logger.critical(f"❌ Ошибка MongoDB: {e}")
            while True:
                await asyncio.sleep(60)

        self.db = self.mongo_client[MONGO_DB_NAME]
        self.economy_collection = self.db["economy"]
        self.transactions_collection = self.db["transactions"]
        self.cooldowns = CooldownManager(self.db["cooldowns"])

        if await self.economy_collection.find_one({"_id": "central_bank"}) is None:
            await self.economy_collection.insert_one({
                "_id": "central_bank",
                "reserve": 0,
                "printed": 0,
                "welfare_fund": 0,
                "event_fund": 0,
                "reward_fund": 0
            })
            logger.info("✅ Центробанк инициализирован")

        self.ub = UBClient(UB_TOKEN)
        logger.info("✅ UnbelievaBoat подключён")

        if HF_TOKEN:
            self.hf_client = InferenceClient(token=HF_TOKEN)
            logger.info("✅ Hugging Face подключён")

        self._currency_cleanup_task = asyncio.create_task(currency_cache.cleanup_loop())

        try:
            signal.signal(signal.SIGTERM, lambda s, f: asyncio.create_task(self._emergency_shutdown()))
            signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(self._emergency_shutdown()))
        except ValueError:
            pass

        logger.info(f"✅ Бот готов, версия {BOT_VERSION}")

    async def _emergency_shutdown(self):
        logger.info("⚠️ Завершение...")
        if self.mongo_client:
            self.mongo_client.close()

    # ================================================================
    # ФУНКЦИИ ЦБ
    # ================================================================

    async def get_central_bank(self) -> dict:
        return await self.economy_collection.find_one({"_id": "central_bank"}) or {}

    async def change_reserve(self, amount: int) -> bool:
        if amount == 0:
            return True
        if amount > 0:
            r = await self.economy_collection.update_one(
                {"_id": "central_bank"},
                {"$inc": {"reserve": amount}}
            )
            return r.modified_count == 1
        r = await self.economy_collection.update_one(
            {"_id": "central_bank", "reserve": {"$gte": abs(amount)}},
            {"$inc": {"reserve": amount}}
        )
        return r.modified_count == 1

    async def add_transaction(self, source: str, destination: str, amount: int, reason: str):
        await self.transactions_collection.insert_one({
            "time": datetime.now(timezone.utc),
            "source": source,
            "destination": destination,
            "amount": amount,
            "reason": reason
        })

    async def income_to_reserve(self, amount: int, source: str, reason: str) -> bool:
        if amount <= 0:
            return False
        if not await self.change_reserve(amount):
            return False
        await self.add_transaction(source, "central_bank", amount, reason)
        return True

    async def spend_from_reserve(self, amount: int, destination: str, reason: str) -> bool:
        if amount <= 0:
            return False
        if not await self.change_reserve(-amount):
            return False
        await self.add_transaction("central_bank", destination, amount, reason)
        return True

    async def get_total_balance(self) -> int:
        if self.ub is None:
            return 0
        leaderboard = await self.ub.get_guild_leaderboard(str(GUILD_ID), limit=1000)
        return sum(u.get("total", 0) for u in leaderboard)

    async def calculate_rate(self, total_balance: int) -> float:
        if total_balance <= 0:
            return 0
        usd = await currency_cache.get_rate(USD_CODE)
        return (1_000_000 / total_balance) * usd * 0.01

    # ================================================================
    # СБОР СТАТИСТИКИ
    # ================================================================

    async def collect_stats(self) -> dict:
        cb = await self.get_central_bank()
        reserve = cb.get("reserve", 0)
        printed = cb.get("printed", 0)
        funds = cb.get("welfare_fund", 0) + cb.get("event_fund", 0) + cb.get("reward_fund", 0)
        total_balance = await self.get_total_balance()
        rate = await self.calculate_rate(total_balance)

        day_ago = datetime.now(timezone.utc) - timedelta(hours=24)
        pipeline = [
            {"$match": {"time": {"$gte": day_ago}}},
            {"$group": {
                "_id": "$reason",
                "income": {"$sum": {"$cond": [{"$eq": ["$destination", "central_bank"]}, "$amount", 0]}},
                "outcome": {"$sum": {"$cond": [{"$eq": ["$source", "central_bank"]}, "$amount", 0]}}
            }}
        ]
        by_reason = await self.transactions_collection.aggregate(pipeline).to_list(100)

        income_total = sum(r["income"] for r in by_reason)
        outcome_total = sum(r["outcome"] for r in by_reason)

        return {
            "reserve": reserve,
            "printed": printed,
            "funds": funds,
            "total_balance": total_balance,
            "total_economy": reserve + funds + total_balance,
            "rate": rate,
            "reserve_change": income_total - outcome_total,
            "income_by_reason": {r["_id"]: r["income"] for r in by_reason if r["income"] > 0},
            "outcome_by_reason": {r["_id"]: r["outcome"] for r in by_reason if r["outcome"] > 0},
        }

    # ================================================================
    # ОТЧЁТЫ
    # ================================================================

    def _fallback_report(self, stats: dict) -> str:
        change = stats['reserve_change']
        sign = "+" if change >= 0 else ""
        return (
            f"🏦 Резерв ЦБ: **{stats['reserve']:,}**\n"
            f"🏛️ Фонды: **{stats['funds']:,}**\n"
            f"👥 У игроков: **{stats['total_balance']:,}**\n"
            f"💰 Всего: **{stats['total_economy']:,}**\n"
            f"📈 Курс: **{stats['rate']:.4f}** ₽\n"
            f"📊 Изменение: **{sign}{change:,}**"
        )

    async def generate_report_text(self, stats: dict) -> str:
        if self.hf_client is None:
            return self._fallback_report(stats)

        income_lines = "\n".join([f"- {k}: +{v:,}" for k, v in stats['income_by_reason'].items()])
        outcome_lines = "\n".join([f"- {k}: -{v:,}" for k, v in stats['outcome_by_reason'].items()])

        prompt = f"""Ты — аналитик Центрального банка Discord-сервера.
Напиши краткий отчёт (3-5 предложений) на русском.

ЦИФРЫ:
- Резерв ЦБ: {stats['reserve']:,}
- Фонды: {stats['funds']:,}
- У игроков: {stats['total_balance']:,}
- Всего в экономике: {stats['total_economy']:,}
- Напечатано: {stats['printed']:,}
- Курс: {stats['rate']:.4f} ₽
- Изменение резерва: {stats['reserve_change']:+,}

Доходы за сутки:
{income_lines or "нет данных"}

Расходы за сутки:
{outcome_lines or "нет данных"}

Не выдумывай цифры. Пиши официально, но живо."""

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.hf_client.chat_completion,
                    model=MODEL_NAME,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=250,
                    temperature=0.7
                ),
                timeout=20
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Ошибка HF: {e}")
            return self._fallback_report(stats)

    # ================================================================
    # ЕЖЕДНЕВНЫЙ ОТЧЁТ В 21:00 МСК
    # ================================================================

    @tasks.loop(time=dtime(hour=REPORT_HOUR_UTC, minute=0, tzinfo=timezone.utc))
    async def daily_report(self):
        if REPORT_CHANNEL_ID == 0:
            return
        channel = self.get_channel(REPORT_CHANNEL_ID)
        if not channel:
            return

        stats = await self.collect_stats()
        text = await self.generate_report_text(stats)

        embed = discord.Embed(
            title="📊 Ежедневный отчёт Центробанка",
            description=text,
            color=0x00BFFF,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text="Данные из MongoDB")
        await channel.send(embed=embed)

    @daily_report.before_loop
    async def before_daily_report(self):
        await self.wait_until_ready()

    # ================================================================
    # ON READY
    # ================================================================

    async def on_ready(self):
        logger.info(f"✅ Бот запущен: {self.user}")
        if REPORT_CHANNEL_ID != 0 and not self.daily_report.is_running():
            self.daily_report.start()

    # ================================================================
    # ON MESSAGE
    # ================================================================

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        await self.process_commands(message)

    # ================================================================
    # КОМАНДЫ
    # ================================================================

    # ---------- PRINT MONEY ----------

    @commands.command(name="print_money")
    @commands.has_permissions(administrator=True)
    async def print_money(self, ctx, amount: int):
        if amount <= 0:
            await ctx.send("❌ Сумма должна быть положительной.")
            return

        if not await self.income_to_reserve(amount, "money_printer", "Печать денег"):
            await ctx.send("❌ Не удалось.")
            return

        await self.economy_collection.update_one(
            {"_id": "central_bank"},
            {"$inc": {"printed": amount}}
        )

        cb = await self.get_central_bank()
        await ctx.send(
            f"🏦 Напечатано: **{amount:,}**\n"
            f"💰 Резерв: **{cb.get('reserve', 0):,}**\n"
            f"🖨️ Всего: **{cb.get('printed', 0):,}**"
        )

    # ---------- CB ----------

    @commands.command(name="cb")
    async def cb(self, ctx):
        cb = await self.get_central_bank()
        total = await self.get_total_balance()
        funds = cb.get("welfare_fund", 0) + cb.get("event_fund", 0) + cb.get("reward_fund", 0)

        embed = discord.Embed(title="🏦 Центральный банк", color=0x00BFFF)
        embed.add_field(name="🏦 Резерв", value=f"{cb.get('reserve', 0):,}", inline=True)
        embed.add_field(name="🏛️ Фонды", value=f"{funds:,}", inline=True)
        embed.add_field(name="👥 У игроков", value=f"{total:,}", inline=True)
        embed.add_field(name="💰 Всего", value=f"{cb.get('reserve', 0) + funds + total:,}", inline=False)
        embed.add_field(name="🖨️ Напечатано", value=f"{cb.get('printed', 0):,}", inline=False)
        embed.add_field(name="🤝 Соцфонд", value=f"{cb.get('welfare_fund', 0):,}", inline=True)
        embed.add_field(name="🎉 Ивенты", value=f"{cb.get('event_fund', 0):,}", inline=True)
        embed.add_field(name="🎁 Награды", value=f"{cb.get('reward_fund', 0):,}", inline=True)
        await ctx.send(embed=embed)

    # ---------- ECONOMY STATS ----------

    @commands.command(name="economy_stats")
    async def economy_stats(self, ctx):
        stats = await self.collect_stats()

        embed = discord.Embed(title="📊 Статистика экономики", color=0x00BFFF)
        embed.add_field(name="🏦 Резерв", value=f"{stats['reserve']:,}", inline=True)
        embed.add_field(name="🏛️ Фонды", value=f"{stats['funds']:,}", inline=True)
        embed.add_field(name="👥 У игроков", value=f"{stats['total_balance']:,}", inline=True)
        embed.add_field(name="💰 Всего", value=f"{stats['total_economy']:,}", inline=False)

        if stats['income_by_reason']:
            income = "\n".join([f"**{k}**: +{v:,}" for k, v in stats['income_by_reason'].items()])
            embed.add_field(name="📥 Доходы (24ч)", value=income, inline=False)

        if stats['outcome_by_reason']:
         