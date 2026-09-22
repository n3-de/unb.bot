# ====================================================================
# Central Bank Bot - Version 1.5.0
# Commands are registered through a Cog so both ! and / commands work.
# ====================================================================

import os
import io
import re
import sys
import time
import asyncio
import logging
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
# CONFIG
# ====================================================================

BOT_VERSION = "1.5.0"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("CentralBank")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
UB_TOKEN = os.getenv("UB_TOKEN")
HF_TOKEN = os.getenv("HF_TOKEN")

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "cb")

GUILD_ID = int(os.getenv("GUILD_ID", "0"))
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))

USD_CODE = "R01235"
EUR_CODE = "R01239"
CNY_CODE = "R01375"

REPORT_HOUR_UTC = 18


# ====================================================================
# MONGODB
# ====================================================================

def build_mongo_uri() -> str:
    username = urllib.parse.quote_plus(MONGO_USER or "")
    password = urllib.parse.quote_plus(MONGO_PASSWORD or "")

    return (
        f"mongodb+srv://{username}:{password}"
        f"@cluster0.u62aem5.mongodb.net/?appName=CentralBank"
    )


def validate_config():
    missing = []

    for name, value in (
        ("DISCORD_TOKEN", DISCORD_TOKEN),
        ("UB_TOKEN", UB_TOKEN),
        ("MONGO_USER", MONGO_USER),
        ("MONGO_PASSWORD", MONGO_PASSWORD),
    ):
        if not value:
            missing.append(name)

    if GUILD_ID == 0:
        missing.append("GUILD_ID")

    if missing:
        raise RuntimeError(
            "Не заданы переменные окружения: "
            + ", ".join(missing)
        )

    if not HF_TOKEN:
        logger.warning(
            "⚠️ HF_TOKEN не задан — отчёты будут без нейросети"
        )


# ====================================================================
# FLASK / RENDER
# ====================================================================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    return f"Online 🏦 Central Bank {BOT_VERSION}"


@web_app.route("/health")
def health():
    return {"status": "ok", "version": BOT_VERSION}, 200


def run_flask():
    serve(
        web_app,
        host="0.0.0.0",
        port=PORT,
        threads=4,
    )


# ====================================================================
# CURRENCY CACHE
# ====================================================================

class CurrencyCache:
    def __init__(self, ttl=3600):
        self.ttl = ttl
        self._cache = {}
        self._lock = None

    def _lock_obj(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_rate(self, code: str) -> float:
        now = time.time()

        async with self._lock_obj():
            cached = self._cache.get(code)
            if cached and now - cached[1] < self.ttl:
                return cached[0]

        loop = asyncio.get_running_loop()
        rate = await loop.run_in_executor(
            None,
            self._fetch_rate_sync,
            code,
        )

        async with self._lock_obj():
            self._cache[code] = (rate, time.time())

        return rate

    @staticmethod
    def _fetch_rate_sync(code: str) -> float:
        try:
            response = requests.get(
                "https://www.cbr.ru/scripts/XML_daily.asp",
                timeout=10,
            )
            response.raise_for_status()

            pattern = (
                rf'<Valute ID="{re.escape(code)}">'
                rf".*?<Value>(.*?)</Value>"
            )

            match = re.search(
                pattern,
                response.text,
                re.DOTALL,
            )

            if match:
                return float(
                    match.group(1).replace(",", ".")
                )

        except Exception as exc:
            logger.error(
                "Ошибка курса %s: %s",
                code,
                exc,
            )

        return 0.0

    async def cleanup_loop(self):
        while True:
            await asyncio.sleep(self.ttl)

            now = time.time()

            async with self._lock_obj():
                expired = [
                    key
                    for key, (_, timestamp) in self._cache.items()
                    if now - timestamp > self.ttl * 2
                ]

                for key in expired:
                    self._cache.pop(key, None)


# ====================================================================
# BOT
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
            status=discord.Status.online,
        )

        self.mongo_client = None
        self.db = None
        self.economy_collection = None
        self.transactions_collection = None

        self.ub = None
        self.ub_guild = None
        self.hf_client = None

        self.currency_cache = CurrencyCache()
        self._currency_cleanup_task = None
        self._synced = False

    async def setup_hook(self):
        # ------------------------------------------------------------
        # MongoDB
        # ------------------------------------------------------------
        try:
            self.mongo_client = AsyncIOMotorClient(
                build_mongo_uri(),
                serverSelectionTimeoutMS=10000,
                maxPoolSize=10,
                minPoolSize=2,
            )

            await self.mongo_client.admin.command("ping")

            self.db = self.mongo_client[MONGO_DB_NAME]
            self.economy_collection = self.db["economy"]
            self.transactions_collection = self.db["transactions"]

            logger.info(
                "✅ MongoDB подключена | база: %s",
                MONGO_DB_NAME,
            )

        except Exception as exc:
            logger.critical(
                "❌ Ошибка MongoDB: %s",
                exc,
            )
            raise

        # ------------------------------------------------------------
        # Central Bank document
        # ------------------------------------------------------------
        existing = await self.economy_collection.find_one(
            {"_id": "central_bank"}
        )

        if existing is None:
            await self.economy_collection.insert_one(
                {
                    "_id": "central_bank",
                    "reserve": 0,
                    "printed": 0,
                    "welfare_fund": 0,
                    "event_fund": 0,
                    "reward_fund": 0,
                }
            )
            logger.info("✅ Центральный банк создан")

        # ------------------------------------------------------------
        # UnbelievaBoat
        # ------------------------------------------------------------
        try:
            self.ub = UBClient(UB_TOKEN)
            self.ub_guild = await self.ub.get_guild(GUILD_ID)
            logger.info("✅ UnbelievaBoat подключён")
        except Exception as exc:
            logger.critical(
                "❌ Ошибка UnbelievaBoat: %s",
                exc,
            )
            raise

        # ------------------------------------------------------------
        # Hugging Face
        # ------------------------------------------------------------
        if HF_TOKEN:
            self.hf_client = InferenceClient(token=HF_TOKEN)
            logger.info("✅ Hugging Face подключён")

        # ------------------------------------------------------------
        # Currency cleanup
        # ------------------------------------------------------------
        self._currency_cleanup_task = asyncio.create_task(
            self.currency_cache.cleanup_loop()
        )

        # ------------------------------------------------------------
        # IMPORTANT:
        # Commands are inside a Cog and are explicitly added here.
        # This is what makes !commands and hybrid slash commands work.
        # ------------------------------------------------------------
        await self.add_cog(CentralBankCog(self))

        logger.info(
            "✅ Cog команд загружен: %s команд",
            len(self.commands),
        )

    async def close(self):
        logger.info("⚠️ Завершение работы...")

        if self._currency_cleanup_task:
            self._currency_cleanup_task.cancel()

        if self.mongo_client:
            self.mongo_client.close()

        await super().close()

    # ----------------------------------------------------------------
    # MESSAGE / PING HELP
    # ----------------------------------------------------------------

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        if self.user and self.user in message.mentions:
            content = message.content

            content = content.replace(
                f"<@{self.user.id}>",
                "",
            )
            content = content.replace(
                f"<@!{self.user.id}>",
                "",
            )

            if not content.strip():
                await self.send_help_message(message)
                return

        await self.process_commands(message)

    async def send_help_message(self, message):
        await message.channel.send(
            embed=CentralBankCog.create_help_embed()
        )

    # ----------------------------------------------------------------
    # READY / SLASH SYNC
    # ----------------------------------------------------------------

    async def on_ready(self):
        logger.info(
            "✅ Бот запущен: %s | ID: %s",
            self.user,
            self.user.id if self.user else "unknown",
        )

        if not self._synced:
            try:
                guild = discord.Object(id=GUILD_ID)

                # Копируем hybrid-команды в конкретный сервер.
                self.tree.copy_global_to(guild=guild)

                synced = await self.tree.sync(guild=guild)

                logger.info(
                    "✅ Slash-команд синхронизировано: %s",
                    len(synced),
                )

                logger.info(
                    "📋 Команды: %s",
                    ", ".join(command.name for command in synced),
                )

                self._synced = True

            except Exception as exc:
                logger.exception(
                    "❌ Ошибка синхронизации slash-команд: %s",
                    exc,
                )

        # Запускаем ежедневный отчёт после готовности.
        cog = self.get_cog("CentralBankCog")

        if (
            cog
            and REPORT_CHANNEL_ID != 0
            and not cog.daily_report.is_running()
        ):
            cog.daily_report.start()

    # ----------------------------------------------------------------
    # COMMAND ERRORS
    # ----------------------------------------------------------------

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandNotFound):
            return

        if isinstance(error, commands.MissingPermissions):
            await ctx.send(
                "❌ У тебя нет прав на эту команду."
            )
            return

        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                f"❌ Не хватает аргумента: "
                f"`{error.param.name}`"
            )
            return

        if isinstance(error, commands.BadArgument):
            await ctx.send(
                "❌ Неверный формат аргумента."
            )
            return

        if isinstance(error, commands.CommandInvokeError):
            original = error.original
            logger.exception(
                "❌ Ошибка команды %s: %s",
                getattr(ctx.command, "qualified_name", "?"),
                original,
            )
        else:
            logger.exception(
                "❌ Ошибка команды: %s",
                error,
            )

        try:
            await ctx.send(
                "❌ Произошла ошибка при выполнении команды."
            )
        except Exception:
            pass


# ====================================================================
# COMMAND COG
# ====================================================================

class CentralBankCog(commands.Cog):
    def __init__(self, bot: CentralBankBot):
        self.bot = bot

    # ----------------------------------------------------------------
    # HELP
    # ----------------------------------------------------------------

    @staticmethod
    def create_help_embed():
        embed = discord.Embed(
            title="🏦 Центральный банк — помощь",
            description=(
                "Команды работают через `!` и `/`.\n"
                "Можно также просто пингануть бота."
            ),
            color=0x00BFFF,
        )

        embed.add_field(
            name="💰 Экономика",
            value=(
                "`!cb` • `/cb` — состояние ЦБ\n"
                "`!economy` • `/economy` — экономика\n"
                "`!rate` • `/rate` — курс\n"
                "`!chart` • `/chart` — график\n"
                "`!history` • `/history` — транзакции"
            ),
            inline=False,
        )

        embed.add_field(
            name="🏛️ Управление",
            value=(
                "`!print_money` • `/print_money` — печать\n"
                "`!tax` • `/tax` — налог\n"
                "`!fund` • `/fund` — фонды\n"
                "`!fund_add` • `/fund_add` — пополнить фонд\n"
                "`!fund_take` • `/fund_take` — выдать из фонда"
            ),
            inline=False,
        )

        embed.add_field(
            name="ℹ️ Помощь",
            value="`!help` • `/help`",
            inline=False,
        )

        embed.set_footer(
            text=f"Central Bank • версия {BOT_VERSION}"
        )

        return embed

    @commands.hybrid_command(
        name="help",
        description="Показать список команд Центрального банка",
    )
    async def help(self, ctx: commands.Context):
        await ctx.send(embed=self.create_help_embed())

    # ----------------------------------------------------------------
    # DATABASE HELPERS
    # ----------------------------------------------------------------

    async def get_central_bank(self):
        return (
            await self.bot.economy_collection.find_one(
                {"_id": "central_bank"}
            )
            or {}
        )

    async def change_reserve(self, amount: int):
        if amount == 0:
            return True

        if amount > 0:
            result = await self.bot.economy_collection.update_one(
                {"_id": "central_bank"},
                {"$inc": {"reserve": amount}},
            )
            return result.modified_count == 1

        result = await self.bot.economy_collection.update_one(
            {
                "_id": "central_bank",
                "reserve": {"$gte": abs(amount)},
            },
            {"$inc": {"reserve": amount}},
        )

        return result.modified_count == 1

    async def add_transaction(
        self,
        source,
        destination,
        amount,
        reason,
    ):
        await self.bot.transactions_collection.insert_one(
            {
                "time": datetime.now(timezone.utc),
                "source": source,
                "destination": destination,
                "amount": int(amount),
                "reason": reason,
            }
        )

    async def income_to_reserve(
        self,
        amount,
        source,
        reason,
    ):
        if amount <= 0:
            return False

        if not await self.change_reserve(amount):
            return False

        await self.add_transaction(
            source,
            "central_bank",
            amount,
            reason,
        )

        return True

    async def spend_from_reserve(
        self,
        amount,
        destination,
        reason,
    ):
        if amount <= 0:
            return False

        if not await self.change_reserve(-amount):
            return False

        await self.add_transaction(
            "central_bank",
            destination,
            amount,
            reason,
        )

        return True

    async def get_total_balance(self):
        if self.bot.ub_guild is None:
            return 0

        try:
            leaderboard = await self.bot.ub_guild.get_leaderboard(
                limit=1000
            )
        except Exception:
            # Некоторые версии wrapper используют guild-level
            # leaderboard API иначе; возвращаем ошибку наверх.
            raise

        total = 0

        for user in leaderboard:
            if isinstance(user, dict):
                total += int(
                    user.get("total", 0) or 0
                )
            else:
                total += int(
                    getattr(user, "total", 0) or 0
                )

        return total

    async def calculate_rate(self, total_balance):
        if total_balance <= 0:
            return 0.0

        usd = await self.bot.currency_cache.get_rate(
            USD_CODE
        )

        if usd <= 0:
            return 0.0

        return (
            (1_000_000 / total_balance)
            * usd
            * 0.01
        )

    # ----------------------------------------------------------------
    # STATS
    # ----------------------------------------------------------------

    async def collect_stats(self):
        cb = await self.get_central_bank()

        reserve = int(cb.get("reserve", 0) or 0)
        printed = int(cb.get("printed", 0) or 0)
        welfare = int(cb.get("welfare_fund", 0) or 0)
        events = int(cb.get("event_fund", 0) or 0)
        rewards = int(cb.get("reward_fund", 0) or 0)

        funds = welfare + events + rewards

        total_balance = await self.get_total_balance()
        rate = await self.calculate_rate(total_balance)

        day_ago = (
            datetime.now(timezone.utc)
            - timedelta(hours=24)
        )

        pipeline = [
            {
                "$match": {
                    "time": {"$gte": day_ago}
                }
            },
            {
                "$group": {
                    "_id": "$reason",
                    "income": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$destination",
                                        "central_bank",
                                    ]
                                },
                                "$amount",
                                0,
                            ]
                        }
                    },
                    "outcome": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$source",
                                        "central_bank",
                                    ]
                                },
                                "$amount",
                                0,
                            ]
                        }
                    },
                }
            },
        ]

        by_reason = await (
            self.bot.transactions_collection
            .aggregate(pipeline)
            .to_list(100)
        )

        income_total = sum(
            int(item.get("income", 0) or 0)
            for item in by_reason
        )

        outcome_total = sum(
            int(item.get("outcome", 0) or 0)
            for item in by_reason
        )

        return {
            "reserve": reserve,
            "printed": printed,
            "funds": funds,
            "welfare": welfare,
            "events": events,
            "rewards": rewards,
            "total_balance": total_balance,
            "total_economy": (
                reserve + funds + total_balance
            ),
            "rate": rate,
            "reserve_change": (
                income_total - outcome_total
            ),
            "income_by_reason": {
                item["_id"]: item["income"]
                for item in by_reason
                if item.get("income", 0) > 0
            },
            "outcome_by_reason": {
                item["_id"]: item["outcome"]
                for item in by_reason
                if item.get("outcome", 0) > 0
            },
        }

    # ----------------------------------------------------------------
    # PRINT MONEY
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="print_money",
        description="Напечатать деньги в резерв ЦБ",
    )
    @app_commands.describe(
        amount="Сколько напечатать"
    )
    @commands.has_permissions(administrator=True)
    async def print_money(
        self,
        ctx,
        amount: int,
    ):
        if amount <= 0:
            await ctx.send(
                "❌ Сумма должна быть положительной."
            )
            return

        ok = await self.income_to_reserve(
            amount,
            "money_printer",
            "Печать денег",
        )

        if not ok:
            await ctx.send(
                "❌ Не удалось провести операцию."
            )
            return

        await self.bot.economy_collection.update_one(
            {"_id": "central_bank"},
            {"$inc": {"printed": amount}},
        )

        cb = await self.get_central_bank()

        await ctx.send(
            f"🏦 Напечатано: **{amount:,}**\n"
            f"💰 Резерв: **{cb.get('reserve', 0):,}**\n"
            f"🖨️ Всего напечатано: "
            f"**{cb.get('printed', 0):,}**"
        )

    # ----------------------------------------------------------------
    # CB
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="cb",
        description="Показать состояние Центробанка",
    )
    async def cb(self, ctx):
        cb = await self.get_central_bank()

        try:
            total = await self.get_total_balance()
        except Exception as exc:
            logger.exception("Ошибка получения баланса UB: %s", exc)
            await ctx.send(
                "❌ Не удалось получить баланс игроков из UnbelievaBoat."
            )
            return

        reserve = int(cb.get("reserve", 0) or 0)

        funds = (
            int(cb.get("welfare_fund", 0) or 0)
            + int(cb.get("event_fund", 0) or 0)
            + int(cb.get("reward_fund", 0) or 0)
        )

        total_economy = reserve + funds + total

        embed = discord.Embed(
            title="🏦 Центральный банк",
            color=0x00BFFF,
        )

        embed.add_field(
            name="🏦 Резерв",
            value=f"{reserve:,}",
            inline=True,
        )
        embed.add_field(
            name="🏛️ Фонды",
            value=f"{funds:,}",
            inline=True,
        )
        embed.add_field(
            name="👥 У игроков",
            value=f"{total:,}",
            inline=True,
        )
        embed.add_field(
            name="💰 Всего",
            value=f"{total_economy:,}",
            inline=False,
        )
        embed.add_field(
            name="🖨️ Напечатано",
            value=f"{cb.get('printed', 0):,}",
            inline=False,
        )
        embed.add_field(
            name="🤝 Соцфонд",
            value=f"{cb.get('welfare_fund', 0):,}",
            inline=True,
        )
        embed.add_field(
            name="🎉 Ивенты",
            value=f"{cb.get('event_fund', 0):,}",
            inline=True,
        )
        embed.add_field(
            name="🎁 Награды",
            value=f"{cb.get('reward_fund', 0):,}",
            inline=True,
        )

        await ctx.send(embed=embed)

    # ----------------------------------------------------------------
    # ECONOMY
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="economy",
        description="Полная статистика экономики",
    )
    async def economy(self, ctx):
        try:
            stats = await self.collect_stats()
        except Exception as exc:
            logger.exception("Ошибка economy: %s", exc)
            await ctx.send(
                "❌ Не удалось получить статистику экономики."
            )
            return

        embed = discord.Embed(
            title="🏦 Экономика Центробанка",
            color=0x00BFFF,
        )

        embed.add_field(
            name="🏦 Резерв ЦБ",
            value=f"**{stats['reserve']:,}**",
            inline=True,
        )
        embed.add_field(
            name="🏛️ Фонды",
            value=f"**{stats['funds']:,}**",
            inline=True,
        )
        embed.add_field(
            name="👥 У игроков",
            value=f"**{stats['total_balance']:,}**",
            inline=True,
        )
        embed.add_field(
            name="💰 Всего в экономике",
            value=f"**{stats['total_economy']:,}**",
            inline=False,
        )

        if stats["income_by_reason"]:
            income = "\n".join(
                f"• {key}: **+{value:,}**"
                for key, value
                in stats["income_by_reason"].items()
            )

            embed.add_field(
                name="📥 Доходы за 24ч",
                value=income[:1024],
                inline=False,
            )

        if stats["outcome_by_reason"]:
            outcome = "\n".join(
                f"• {key}: **-{value:,}**"
                for key, value
                in stats["outcome_by_reason"].items()
            )

            embed.add_field(
                name="📤 Расходы за 24ч",
                value=outcome[:1024],
                inline=False,
            )

        change = stats["reserve_change"]
        sign = "+" if change >= 0 else ""

        embed.add_field(
            name="📊 Изменение резерва",
            value=f"**{sign}{change:,}**",
            inline=False,
        )

        await ctx.send(embed=embed)

    # ----------------------------------------------------------------
    # TAX
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="tax",
        description="Списать налог у игрока в резерв ЦБ",
    )
    @app_commands.describe(
        member="Кого облагаем налогом",
        amount="Сумма налога",
    )
    @commands.has_permissions(administrator=True)
    async def tax(
        self,
        ctx,
        member: discord.Member,
        amount: int,
    ):
        if amount <= 0:
            await ctx.send(
                "❌ Сумма должна быть положительной."
            )
            return

        if self.bot.ub_guild is None:
            await ctx.send(
                "❌ UnbelievaBoat недоступен."
            )
            return

        try:
            user = await self.bot.ub_guild.get_user_balance(
                member.id
            )

            if user.cash < amount:
                await ctx.send(
                    f"❌ У {member.mention} недостаточно денег."
                )
                return

            await user.update(cash=-amount)

            try:
                ok = await self.income_to_reserve(
                    amount,
                    f"user_{member.id}",
                    "Налог",
                )
            except Exception:
                await user.update(cash=amount)
                raise

            if not ok:
                await user.update(cash=amount)
                await ctx.send(
                    "❌ Не удалось провести операцию. "
                    "Средства возвращены."
                )
                return

        except Exception as exc:
            logger.exception("Ошибка tax: %s", exc)
            await ctx.send(
                "❌ Ошибка при работе с UnbelievaBoat."
            )
            return

        await ctx.send(
            f"💰 Налог **{amount:,}** списан у "
            f"{member.mention}."
        )

    # ----------------------------------------------------------------
    # FUND
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="fund",
        description="Показать состояние фондов",
    )
    @app_commands.describe(
        fund_name="welfare, event, reward или all"
    )
    async def fund(
        self,
        ctx,
        fund_name: str = "all",
    ):
        fund_name = fund_name.lower().strip()
        cb = await self.get_central_bank()

        if fund_name == "all":
            embed = discord.Embed(
                title="🏛️ Фонды",
                color=0x00BFFF,
            )

            embed.add_field(
                name="🤝 Соцфонд",
                value=f"{cb.get('welfare_fund', 0):,}",
                inline=True,
            )
            embed.add_field(
                name="🎉 Ивенты",
                value=f"{cb.get('event_fund', 0):,}",
                inline=True,
            )
            embed.add_field(
                name="🎁 Награды",
                value=f"{cb.get('reward_fund', 0):,}",
                inline=True,
            )

            await ctx.send(embed=embed)
            return

        fmap = {
            "welfare": "welfare_fund",
            "event": "event_fund",
            "reward": "reward_fund",
        }

        if fund_name not in fmap:
            await ctx.send(
                "❌ Доступно: welfare, event, reward"
            )
            return

        await ctx.send(
            f"🏛️ {fund_name}: "
            f"**{cb.get(fmap[fund_name], 0):,}**"
        )

    # ----------------------------------------------------------------
    # FUND ADD
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="fund_add",
        description="Пополнить фонд из резерва ЦБ",
    )
    @app_commands.describe(
        fund_name="welfare, event или reward",
        amount="Сумма пополнения",
    )
    @commands.has_permissions(administrator=True)
    async def fund_add(
        self,
        ctx,
        fund_name: str,
        amount: int,
    ):
        fund_name = fund_name.lower().strip()

        if amount <= 0:
            await ctx.send(
                "❌ Сумма должна быть положительной."
            )
            return

        fmap = {
            "welfare": "welfare_fund",
            "event": "event_fund",
            "reward": "reward_fund",
        }

        if fund_name not in fmap:
            await ctx.send(
                "❌ Доступно: welfare, event, reward"
            )
            return

        if not await self.spend_from_reserve(
            amount,
            f"fund_{fund_name}",
            f"Пополнение {fund_name}",
        ):
            await ctx.send(
                "❌ В ЦБ недостаточно средств."
            )
            return

        try:
            await self.bot.economy_collection.update_one(
                {"_id": "central_bank"},
                {"$inc": {fmap[fund_name]: amount}},
            )
        except Exception as exc:
            await self.change_reserve(amount)
            logger.exception(
                "Ошибка пополнения фонда: %s",
                exc,
            )
            await ctx.send(
                "❌ Ошибка MongoDB. Средства возвращены в резерв."
            )
            return

        await ctx.send(
            f"✅ +{amount:,} в фонд **{fund_name}**"
        )

    # ----------------------------------------------------------------
    # FUND TAKE
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="fund_take",
        description="Выдать деньги игроку из фонда",
    )
    @app_commands.describe(
        fund_name="welfare, event или reward",
        amount="Сумма выдачи",
        member="Кому выдать",
    )
    @commands.has_permissions(administrator=True)
    async def fund_take(
        self,
        ctx,
        fund_name: str,
        amount: int,
        member: discord.Member,
    ):
        fund_name = fund_name.lower().strip()

        if amount <= 0:
            await ctx.send(
                "❌ Сумма должна быть положительной."
            )
            return

        fmap = {
            "welfare": "welfare_fund",
            "event": "event_fund",
            "reward": "reward_fund",
        }

        if fund_name not in fmap:
            await ctx.send(
                "❌ Доступно: welfare, event, reward"
            )
            return

        result = await self.bot.economy_collection.update_one(
            {
                "_id": "central_bank",
                fmap[fund_name]: {"$gte": amount},
            },
            {
                "$inc": {
                    fmap[fund_name]: -amount
                }
            },
        )

        if result.modified_count != 1:
            await ctx.send(
                "❌ В фонде недостаточно средств."
            )
            return

        try:
            user = await self.bot.ub_guild.get_user_balance(
                member.id
            )
            await user.update(cash=amount)

        except Exception as exc:
            await self.bot.economy_collection.update_one(
                {"_id": "central_bank"},
                {"$inc": {fmap[fund_name]: amount}},
            )

            logger.exception(
                "Ошибка fund_take: %s",
                exc,
            )

            await ctx.send(
                "❌ Не удалось выдать деньги. "
                "Средства возвращены в фонд."
            )
            return

        await self.add_transaction(
            f"fund_{fund_name}",
            f"user_{member.id}",
            amount,
            f"Выдача из {fund_name}",
        )

        await ctx.send(
            f"✅ {member.mention} получил **{amount:,}** "
            f"из фонда {fund_name}."
        )

    # ----------------------------------------------------------------
    # RATE
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="rate",
        description="Курс монеты к ₽, $, €, ¥",
    )
    async def rate(self, ctx):
        try:
            total = await self.get_total_balance()

            coin_rub = await self.calculate_rate(total)

            usd = await self.bot.currency_cache.get_rate(
                USD_CODE
            )
            eur = await self.bot.currency_cache.get_rate(
                EUR_CODE
            )
            cny = await self.bot.currency_cache.get_rate(
                CNY_CODE
            )

        except Exception as exc:
            logger.exception("Ошибка rate: %s", exc)
            await ctx.send(
                "❌ Не удалось получить курс."
            )
            return

        coin_usd = coin_rub / usd if usd > 0 else 0
        coin_eur = coin_rub / eur if eur > 0 else 0
        coin_cny = coin_rub / cny if cny > 0 else 0

        embed = discord.Embed(
            title="📈 Курс валюты",
            color=0x00FF00,
        )

        embed.add_field(
            name="1 монета",
            value=(
                f"≈ **{coin_rub:.4f}** ₽\n"
                f"≈ **{coin_usd:.4f}** $\n"
                f"≈ **{coin_eur:.4f}** €\n"
                f"≈ **{coin_cny:.4f}** ¥"
            ),
            inline=False,
        )

        embed.add_field(
            name="Курсы ЦБ РФ",
            value=(
                f"USD: {usd:.2f} ₽\n"
                f"EUR: {eur:.2f} ₽\n"
                f"CNY: {cny:.2f} ₽"
            ),
            inline=False,
        )

        await ctx.send(embed=embed)

        await self.bot.db["stats"].update_one(
            {"_id": "rate_history"},
            {
                "$push": {
                    "history": {
                        "date": datetime.now(
                            timezone.utc
                        ).strftime("%d.%m"),
                        "rate": round(coin_rub, 4),
                    }
                }
            },
            upsert=True,
        )

    # ----------------------------------------------------------------
    # CHART
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="chart",
        description="График курса монеты",
    )
    async def chart(self, ctx):
        doc = await self.bot.db["stats"].find_one(
            {"_id": "rate_history"}
        )

        history = doc.get("history", []) if doc else []

        if len(history) < 2:
            await ctx.send(
                "📉 Мало данных. Выполни `!rate` несколько раз."
            )
            return

        history = history[-30:]

        dates = [item["date"] for item in history]
        rates = [item["rate"] for item in history]

        plt.figure(figsize=(8, 5))
        plt.plot(
            dates,
            rates,
            marker="o",
            color="#00BFFF",
        )
        plt.title("Курс валюты")
        plt.xlabel("Дата")
        plt.ylabel("₽")
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)

        buffer = io.BytesIO()

        plt.savefig(
            buffer,
            format="png",
            bbox_inches="tight",
            dpi=80,
        )

        buffer.seek(0)
        plt.close()

        await ctx.send(
            file=discord.File(
                buffer,
                filename="chart.png",
            )
        )

    # ----------------------------------------------------------------
    # HISTORY
    # ----------------------------------------------------------------

    @commands.hybrid_command(
        name="history",
        description="Последние транзакции Центробанка",
    )
    @app_commands.describe(
        limit="Сколько записей показать (1-25)"
    )
    async def history(
        self,
        ctx,
        limit: int = 10,
    ):
        limit = max(1, min(25, limit))

        docs = await (
            self.bot.transactions_collection
            .find()
            .sort("time", -1)
            .limit(limit)
            .to_list(limit)
        )

        if not docs:
            await ctx.send(
                "📭 Транзакций пока нет."
            )
            return

        lines = []

        for document in docs:
            timestamp = document["time"].strftime(
                "%d.%m %H:%M"
            )

            sign = (
                "+"
                if document["destination"] == "central_bank"
                else "-"
            )

            lines.append(
                f"`{timestamp}` "
                f"{sign}{document['amount']:,} "
                f"— {document['reason']}"
            )

        embed = discord.Embed(
            title="📜 История транзакций",
            description="\n".join(lines),
            color=0x00BFFF,
        )

        embed.set_footer(
            text=f"Последние {len(docs)} записей"
        )

        await ctx.send(embed=embed)

    # ----------------------------------------------------------------
    # DAILY REPORT
    # ----------------------------------------------------------------

    def fallback_report(self, stats):
        change = stats["reserve_change"]
        sign = "+" if change >= 0 else ""

        return (
            f"🏦 Резерв ЦБ: **{stats['reserve']:,}**\n"
            f"🏛️ Фонды: **{stats['funds']:,}**\n"
            f"👥 У игроков: **{stats['total_balance']:,}**\n"
            f"💰 Всего: **{stats['total_economy']:,}**\n"
            f"📈 Курс: **{stats['rate']:.4f}** ₽\n"
            f"📊 Изменение: **{sign}{change:,}**"
        )

    async def generate_report_text(self, stats):
        if self.bot.hf_client is None:
            return self.fallback_report(stats)

        income_lines = "\n".join(
            f"- {key}: +{value:,}"
            for key, value
            in stats["income_by_reason"].items()
        )

        outcome_lines = "\n".join(
            f"- {key}: -{value:,}"
            for key, value
            in stats["outcome_by_reason"].items()
        )

        prompt = f"""
Ты — аналитик Центрального банка Discord-сервера.

Напиши краткий отчёт на русском языке, 3-5 предложений.
Не выдумывай цифры.

Резерв ЦБ: {stats['reserve']:,}
Фонды: {stats['funds']:,}
У игроков: {stats['total_balance']:,}
Всего в экономике: {stats['total_economy']:,}
Напечатано: {stats['printed']:,}
Курс: {stats['rate']:.4f} ₽
Изменение резерва: {stats['reserve_change']:+,}

Доходы:
{income_lines or "нет"}

Расходы:
{outcome_lines or "нет"}
"""

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.bot.hf_client.chat_completion,
                    model=MODEL_NAME,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt,
                        }
                    ],
                    max_tokens=250,
                    temperature=0.7,
                ),
                timeout=20,
            )

            return response.choices[0].message.content

        except Exception as exc:
            logger.error("Ошибка HF: %s", exc)
            return self.fallback_report(stats)

    @tasks.loop(
        time=dtime(
            hour=REPORT_HOUR_UTC,
            minute=0,
            tzinfo=timezone.utc,
        )
    )
    async def daily_report(self):
        if REPORT_CHANNEL_ID == 0:
            return

        channel = self.bot.get_channel(
            REPORT_CHANNEL_ID
        )

        if channel is None:
            try:
                channel = await self.bot.fetch_channel(
                    REPORT_CHANNEL_ID
                )
            except Exception as exc:
                logger.error(
                    "Ошибка получения канала отчёта: %s",
                    exc,
                )
                return

        try:
            stats = await self.collect_stats()
            text = await self.generate_report_text(stats)

            embed = discord.Embed(
                title="📊 Ежедневный отчёт Центробанка",
                description=text,
                color=0x00BFFF,
                timestamp=datetime.now(timezone.utc),
            )

            embed.set_footer(
                text="Данные из MongoDB"
            )

            await channel.send(embed=embed)

        except Exception as exc:
            logger.exception(
                "Ошибка ежедневного отчёта: %s",
                exc,
            )

    @daily_report.before_loop
    async def before_daily_report(self):
        await self.bot.wait_until_ready()


# ====================================================================
# MAIN
# ====================================================================

def main():
    validate_config()

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True,
    )
    flask_thread.start()

    logger.info(
        "🌐 Web-сервер запущен на порту %s",
        PORT,
    )

    bot = CentralBankBot()

    try:
        bot.run(DISCORD_TOKEN)

    except discord.LoginFailure:
        logger.critical(
            "❌ Неверный Discord токен!"
        )

    except Exception as exc:
        logger.exception(
            "❌ Критическая ошибка: %s",
            exc,
        )

    finally:
        logger.info("Процесс завершён")


if __name__ == "__main__":
    main()
