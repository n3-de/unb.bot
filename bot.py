# ====================================================================
# Central Bank Bot - Version 1.3
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
from discord import app_commands
from discord.ext import commands, tasks
from unbelievaboat import Client as UBClient


# ====================================================================
# КОНФИГУРАЦИЯ
# ====================================================================

BOT_VERSION = "1.3.0"
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

COOLDOWN_CACHE_TTL = 300
RATE_CACHE_TTL = 3600

USD_CODE = "R01235"
EUR_CODE = "R01239"
CNY_CODE = "R01375"

REPORT_HOUR_UTC = 18  # 21:00 МСК


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
        logger.warning("⚠️ HF_TOKEN не задан — отчёты без нейросети")


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
        self.ub = None
        self.ub_guild = None
        self.hf_client = None
        self._currency_cleanup_task = None
        self._synced = False

    async def setup_hook(self):
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
        try:
            self.ub_guild = await self.ub.get_guild(GUILD_ID)
            logger.info("✅ UnbelievaBoat подключён")
        except Exception as e:
            logger.critical(f"❌ Ошибка UnbelievaBoat: {e}")

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
        welfare = cb.get("welfare_fund", 0)
        events = cb.get("event_fund", 0)
        rewards = cb.get("reward_fund", 0)
        funds = welfare + events + rewards

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
            "welfare": welfare,
            "events": events,
            "rewards": rewards,
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

Доходы:
{income_lines or "нет"}

Расходы:
{outcome_lines or "нет"}

Не выдумывай цифры."""

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

    async def on_ready(self):
        logger.info(f"✅ Бот запущен: {self.user}")

        if not self._synced:
            try:
                synced = await self.tree.sync(guild=discord.Object(id=GUILD_ID))
                logger.info(f"✅ Синхронизировано {len(synced)} slash-команд")
            except Exception as e:
                logger.error(f"❌ Ошибка синхронизации slash-команд: {e}")
            self._synced = True

        if REPORT_CHANNEL_ID != 0 and not self.daily_report.is_running():
            self.daily_report.start()

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        await self.process_commands(message)

    async def on_command_error(self, ctx: commands.Context, error):
        if isinstance(error, commands.CommandNotFound):
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ У тебя нет прав на эту команду.")
            return
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"❌ Не хватает аргумента: `{error.param.name}`")
            return
        if isinstance(error, commands.BadArgument):
            await ctx.send("❌ Неверный формат аргумента.")
            return
        logger.error(f"Необработанная ошибка команды: {error}")
        await ctx.send("❌ Произошла ошибка при выполнении команды.")

    # ================================================================
    # КОМАНДЫ
    # ================================================================

    @commands.hybrid_command(name="print_money", description="Напечатать деньги в резерв ЦБ")
    @app_commands.describe(amount="Сколько напечатать")
    @commands.has_permissions(administrator=True)
    async def print_money(self, ctx: commands.Context, amount: int):
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

    @commands.hybrid_command(name="cb", description="Показать состояние Центробанка")
    async def cb(self, ctx: commands.Context):
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

    @commands.hybrid_command(name="economy", description="Полная экономика: резерв, фонды, доходы/расходы")
    async def economy(self, ctx: commands.Context):
        stats = await self.collect_stats()

        embed = discord.Embed(
            title="🏦 Экономика Центробанка",
            color=0x00BFFF
        )

        embed.add_field(name="🏦 Резерв ЦБ", value=f"**{stats['reserve']:,}**", inline=True)
        embed.add_field(name="🏛️ Фонды", value=f"**{stats['funds']:,}**", inline=True)
        embed.add_field(name="👥 У игроков", value=f"**{stats['total_balance']:,}**", inline=True)
        embed.add_field(name="💰 Всего в экономике", value=f"**{stats['total_economy']:,}**", inline=False)

        if stats['income_by_reason']:
            income = "\n".join([f"• {k}: **+{v:,}**" for k, v in stats['income_by_reason'].items()])
            embed.add_field(name="📥 Доходы (24ч)", value=income, inline=False)

        if stats['outcome_by_reason']:
            outcome = "\n".join([f"• {k}: **-{v:,}**" for k, v in stats['outcome_by_reason'].items()])
            embed.add_field(name="📤 Расходы (24ч)", value=outcome, inline=False)

        change = stats['reserve_change']
        sign = "+" if change >= 0 else ""
        embed.add_field(name="📊 Изменение резерва", value=f"**{sign}{change:,}**", inline=False)

        embed.set_footer(text="Центробанк • данные из MongoDB")
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="tax", description="Списать налог у игрока в резерв ЦБ")
    @app_commands.describe(member="Кого облагаем налогом", amount="Сумма налога")
    @commands.has_permissions(administrator=True)
    async def tax(self, ctx: commands.Context, member: discord.Member, amount: int):
        if amount <= 0:
            await ctx.send("❌ Сумма должна быть положительной.")
            return

        user = await self.ub_guild.get_user_balance(member.id)
        if user.cash < amount:
            await ctx.send(f"❌ У {member.mention} недостаточно денег.")
            return

        await user.update(cash=-amount)

        if not await self.income_to_reserve(amount, f"user_{member.id}", "Налог"):
            await user.update(cash=amount)
            await ctx.send("❌ Откат.")
            return

        await ctx.send(f"💰 Налог **{amount:,}** списан у {member.mention}.")

    @commands.hybrid_command(name="fund", description="Показать состояние фондов")
    @app_commands.describe(fund_name="welfare, event, reward или all")
    async def fund(self, ctx: commands.Context, fund_name: str = "all"):
        cb = await self.get_central_bank()
        if fund_name == "all":
            embed = discord.Embed(title="🏛️ Фонды", color=0x00BFFF)
            embed.add_field(name="🤝 Соцфонд", value=f"{cb.get('welfare_fund', 0):,}", inline=True)
            embed.add_field(name="🎉 Ивенты", value=f"{cb.get('event_fund', 0):,}", inline=True)
            embed.add_field(name="🎁 Награды", value=f"{cb.get('reward_fund', 0):,}", inline=True)
            await ctx.send(embed=embed)
            return
        fmap = {"welfare": "welfare_fund", "event": "event_fund", "reward": "reward_fund"}
        if fund_name not in fmap:
            await ctx.send("❌ Доступно: welfare, event, reward")
            return
        await ctx.send(f"🏛️ {fund_name}: **{cb.get(fmap[fund_name], 0):,}**")

    @commands.hybrid_command(name="fund_add", description="Пополнить фонд из резерва ЦБ")
    @app_commands.describe(fund_name="welfare, event или reward", amount="Сумма пополнения")
    @commands.has_permissions(administrator=True)
    async def fund_add(self, ctx: commands.Context, fund_name: str, amount: int):
        if amount <= 0:
            await ctx.send("❌ Сумма должна быть положительной.")
            return
        fmap = {"welfare": "welfare_fund", "event": "event_fund", "reward": "reward_fund"}
        if fund_name not in fmap:
            await ctx.send("❌ Доступно: welfare, event, reward")
            return
        if not await self.spend_from_reserve(amount, f"fund_{fund_name}", f"Пополнение {fund_name}"):
            await ctx.send("❌ В ЦБ недостаточно средств.")
            return
        await self.economy_collection.update_one(
            {"_id": "central_bank"},
            {"$inc": {fmap[fund_name]: amount}}
        )
        await ctx.send(f"✅ +{amount:,} в фонд **{fund_name}**")

    @commands.hybrid_command(name="fund_take", description="Выдать деньги игроку из фонда")
    @app_commands.describe(fund_name="welfare, event или reward", amount="Сумма выдачи", member="Кому выдать")
    @commands.has_permissions(administrator=True)
    async def fund_take(self, ctx: commands.Context, fund_name: str, amount: int, member: discord.Member):
        if amount <= 0:
            await ctx.send("❌ Сумма должна быть положительной.")
            return
        fmap = {"welfare": "welfare_fund", "event": "event_fund", "reward": "reward_fund"}
        if fund_name not in fmap:
            await ctx.send("❌ Доступно: welfare, event, reward")
            return
        cb = await self.get_central_bank()
        if cb.get(fmap[fund_name], 0) < amount:
            await ctx.send("❌ В фонде недостаточно.")
            return
        await self.economy_collection.update_one(
            {"_id": "central_bank"},
            {"$inc": {fmap[fund_name]: -amount}}
        )
        user = await self.ub_guild.get_user_balance(member.id)
        await user.update(cash=amount)
        await self.add_transaction(f"fund_{fund_name}", f"user_{member.id}", amount, f"Выдача из {fund_name}")
        await ctx.send(f"✅ {member.mention} получил **{amount:,}** из фонда {fund_name}.")

    @commands.hybrid_command(name="rate", description="Курс монеты к ₽, $, €, ¥")
    async def rate(self, ctx: commands.Context):
        total = await self.get_total_balance()
        coin_rub = await self.calculate_rate(total)
        usd = await currency_cache.get_rate(USD_CODE)
        eur = await currency_cache.get_rate(EUR_CODE)
        cny = await currency_cache.get_rate(CNY_CODE)

        coin_usd = coin_rub / usd if usd > 0 else 0
        coin_eur = coin_rub / eur if eur > 0 else 0
        coin_cny = coin_rub / cny if cny > 0 else 0

        embed = discord.Embed(title="📈 Курс валюты", color=0x00FF00)
        embed.add_field(
            name="1 монета",
            value=(
                f"≈ **{coin_rub:.4f}** ₽\n"
                f"≈ **{coin_usd:.4f}** $\n"
                f"≈ **{coin_eur:.4f}** €\n"
                f"≈ **{coin_cny:.4f}** ¥"
            ),
            inline=False
        )
        embed.add_field(
            name="Курсы ЦБ РФ",
            value=f"USD: {usd:.2f} ₽\nEUR: {eur:.2f} ₽\nCNY: {cny:.2f} ₽",
            inline=False
        )
        await ctx.send(embed=embed)

        await self.db["stats"].update_one(
            {"_id": "rate_history"},
            {"$push": {"history": {"date": datetime.now().strftime("%d.%m"), "rate": round(coin_rub, 4)}}},
            upsert=True
        )

    @commands.hybrid_command(name="chart", description="График курса монеты")
    async def chart(self, ctx: commands.Context):
        doc = await self.db["stats"].find_one({"_id": "rate_history"})
        history = doc["history"] if doc else []

        if len(history) < 2:
            await ctx.send("📉 Мало данных. Повтори `!rate`.")
            return

        dates = [h["date"] for h in history[-30:]]
        rates = [h["rate"] for h in history[-30:]]

        plt.figure(figsize=(8, 5))
        plt.plot(dates, rates, marker="o", color="#00BFFF")
        plt.title("Курс валюты")
        plt.xlabel("Дата")
        plt.ylabel("₽")
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight", dpi=80)
        buf.seek(0)
        plt.close()

        await ctx.send(file=discord.File(buf, filename="chart.png"))

    @commands.hybrid_command(name="history", description="Последние транзакции Центробанка")
    @app_commands.describe(limit="Сколько последних записей показать (1-25)")
    async def history(self, ctx: commands.Context, limit: int = 10):
        limit = max(1, min(25, limit))
        docs = await self.transactions_collection.find().sort("time", -1).limit(limit).to_list(limit)

        if not docs:
            await ctx.send("📭 Транзакций пока нет.")
            return

        lines = []
        for d in docs:
            t = d["time"].strftime("%d.%m %H:%M")
            sign = "+" if d["destination"] == "central_bank" else "-"
            lines.append(f"`{t}` {sign}{d['amount']:,} — {d['reason']}")

        embed = discord.Embed(
            title="📜 История транзакций",
            description="\n".join(lines),
            color=0x00BFFF
        )
        embed.set_footer(text=f"Последние {len(docs)} записей")
        await ctx.send(embed=embed)


# ====================================================================
# ТОЧКА ВХОДА
# ====================================================================

def main():
    validate_config()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("🌐 Веб-сервер запущен")

    bot = CentralBankBot()

    try:
        bot.run(DISCORD_TOKEN)
    except discord.LoginFailure:
        logger.critical("❌ Неверный токен Discord!")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {type(e).__name__}: {e}")
    finally:
        logger.info("Процесс завершён")


if __name__ == "__main__":
    main()
