# ====================================================================
# Central Bank Bot - Version 1.4
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

BOT_VERSION = "1.4.0"

MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash-0731"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("CentralBank")


# ====================================================================
# ENV
# ====================================================================

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
UB_TOKEN = os.environ.get("UB_TOKEN")
HF_TOKEN = os.environ.get("HF_TOKEN")

MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB_NAME = os.environ.get(
    "MONGO_DB_NAME",
    "central_bank"
)

GUILD_ID = int(
    os.environ.get("GUILD_ID", "0")
)

REPORT_CHANNEL_ID = int(
    os.environ.get("REPORT_CHANNEL_ID", "0")
)

PORT = int(
    os.environ.get("PORT", "10000")
)

COOLDOWN_CACHE_TTL = 300
RATE_CACHE_TTL = 3600

USD_CODE = "R01235"
EUR_CODE = "R01239"
CNY_CODE = "R01375"

# 18:00 UTC = 21:00 МСК
REPORT_HOUR_UTC = 18


# ====================================================================
# MONGODB
# ====================================================================

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
        logger.critical(
            f"❌ Не заданы переменные: {', '.join(missing)}"
        )

        while True:
            time.sleep(60)

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
    return {
        "status": "ok",
        "version": BOT_VERSION
    }, 200


def run_flask():
    serve(
        web_app,
        host="0.0.0.0",
        port=PORT,
        threads=4
    )


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
            None,
            self._fetch_rate_sync,
            code
        )

        async with self._lock:
            self._cache[code] = (rate, now)

        return rate

    @staticmethod
    def _fetch_rate_sync(code: str) -> float:

        try:

            response = requests.get(
                "https://www.cbr.ru/scripts/XML_daily.asp",
                timeout=10
            )

            pattern = (
                rf'<Valute ID="{code}">'
                rf'.*?<Value>(.*?)</Value>'
            )

            match = re.search(
                pattern,
                response.text,
                re.DOTALL
            )

            if match:
                return float(
                    match.group(1).replace(",", ".")
                )

        except Exception as e:

            logger.error(
                f"Ошибка курса {code}: {e}"
            )

        return 0.0

    async def cleanup_loop(self):

        while True:

            await asyncio.sleep(
                RATE_CACHE_TTL
            )

            now = time.time()

            async with self._lock:

                expired = [
                    key
                    for key, (_, timestamp)
                    in self._cache.items()
                    if now - timestamp >
                    RATE_CACHE_TTL * 2
                ]

                for key in expired:
                    del self._cache[key]


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
            activity=discord.Game(
                name="Центробанк"
            ),
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


    # ================================================================
    # SETUP
    # ================================================================

    async def setup_hook(self):

        # ------------------------------------------------------------
        # MongoDB
        # ------------------------------------------------------------

        try:

            self.mongo_client = AsyncIOMotorClient(
                build_mongo_uri(),
                serverSelectionTimeoutMS=10000,
                maxPoolSize=10,
                minPoolSize=2
            )

            await self.mongo_client.admin.command(
                "ping"
            )

            logger.info(
                "✅ MongoDB подключена"
            )

        except Exception as e:

            logger.critical(
                f"❌ Ошибка MongoDB: {e}"
            )

            while True:
                await asyncio.sleep(60)


        self.db = self.mongo_client[
            MONGO_DB_NAME
        ]

        self.economy_collection = self.db[
            "economy"
        ]

        self.transactions_collection = self.db[
            "transactions"
        ]


        # ------------------------------------------------------------
        # Центральный банк
        # ------------------------------------------------------------

        existing = await self.economy_collection.find_one(
            {"_id": "central_bank"}
        )

        if existing is None:

            await self.economy_collection.insert_one({

                "_id": "central_bank",

                "reserve": 0,

                "printed": 0,

                "welfare_fund": 0,

                "event_fund": 0,

                "reward_fund": 0
            })

            logger.info(
                "✅ Центробанк инициализирован"
            )


        # ------------------------------------------------------------
        # UnbelievaBoat
        # ------------------------------------------------------------

        self.ub = UBClient(
            UB_TOKEN
        )

        try:

            self.ub_guild = await self.ub.get_guild(
                GUILD_ID
            )

            logger.info(
                "✅ UnbelievaBoat подключён"
            )

        except Exception as e:

            logger.critical(
                f"❌ Ошибка UnbelievaBoat: {e}"
            )


        # ------------------------------------------------------------
        # Hugging Face
        # ------------------------------------------------------------

        if HF_TOKEN:

            self.hf_client = InferenceClient(
                token=HF_TOKEN
            )

            logger.info(
                "✅ Hugging Face подключён"
            )


        # ------------------------------------------------------------
        # Currency cache
        # ------------------------------------------------------------

        self._currency_cleanup_task = asyncio.create_task(
            currency_cache.cleanup_loop()
        )


        # ------------------------------------------------------------
        # Shutdown
        # ------------------------------------------------------------

        try:

            signal.signal(
                signal.SIGTERM,
                lambda s, f:
                asyncio.create_task(
                    self._emergency_shutdown()
                )
            )

            signal.signal(
                signal.SIGINT,
                lambda s, f:
                asyncio.create_task(
                    self._emergency_shutdown()
                )
            )

        except ValueError:
            pass


        logger.info(
            f"✅ Бот готов, версия {BOT_VERSION}"
        )


    # ================================================================
    # SHUTDOWN
    # ================================================================

    async def _emergency_shutdown(self):

        logger.info(
            "⚠️ Завершение..."
        )

        if self._currency_cleanup_task:

            self._currency_cleanup_task.cancel()

        if self.mongo_client:

            self.mongo_client.close()

        logger.info(
            "✅ Завершено"
        )


    # ================================================================
    # ФУНКЦИИ ЦБ
    # ================================================================

    async def get_central_bank(self) -> dict:

        return await self.economy_collection.find_one(
            {"_id": "central_bank"}
        ) or {}


    async def change_reserve(
        self,
        amount: int
    ) -> bool:

        if amount == 0:
            return True


        # Приход
        if amount > 0:

            result = await self.economy_collection.update_one(

                {"_id": "central_bank"},

                {
                    "$inc": {
                        "reserve": amount
                    }
                }
            )

            return result.modified_count == 1


        # Расход
        result = await self.economy_collection.update_one(

            {
                "_id": "central_bank",
                "reserve": {
                    "$gte": abs(amount)
                }
            },

            {
                "$inc": {
                    "reserve": amount
                }
            }
        )

        return result.modified_count == 1


    async def add_transaction(
        self,
        source: str,
        destination: str,
        amount: int,
        reason: str
    ):

        await self.transactions_collection.insert_one({

            "time": datetime.now(
                timezone.utc
            ),

            "source": source,

            "destination": destination,

            "amount": amount,

            "reason": reason
        })


    async def income_to_reserve(
        self,
        amount: int,
        source: str,
        reason: str
    ) -> bool:

        if amount <= 0:
            return False

        if not await self.change_reserve(
            amount
        ):
            return False

        await self.add_transaction(
            source,
            "central_bank",
            amount,
            reason
        )

        return True


    async def spend_from_reserve(
        self,
        amount: int,
        destination: str,
        reason: str
    ) -> bool:

        if amount <= 0:
            return False

        if not await self.change_reserve(
            -amount
        ):
            return False

        await self.add_transaction(
            "central_bank",
            destination,
            amount,
            reason
        )

        return True


    async def get_total_balance(self) -> int:

        if self.ub is None:
            return 0

        leaderboard = await self.ub.get_guild_leaderboard(
            str(GUILD_ID),
            limit=1000
        )

        return sum(
            user.get("total", 0)
            for user in leaderboard
        )


    async def calculate_rate(
        self,
        total_balance: int
    ) -> float:

        if total_balance <= 0:
            return 0

        usd = await currency_cache.get_rate(
            USD_CODE
        )

        return (
            (1_000_000 / total_balance)
            * usd
            * 0.01
        )


    # ================================================================
    # СТАТИСТИКА
    # ================================================================

    async def collect_stats(self) -> dict:

        cb = await self.get_central_bank()

        reserve = cb.get(
            "reserve",
            0
        )

        printed = cb.get(
            "printed",
            0
        )

        welfare = cb.get(
            "welfare_fund",
            0
        )

        events = cb.get(
            "event_fund",
            0
        )

        rewards = cb.get(
            "reward_fund",
            0
        )

        funds = (
            welfare
            + events
            + rewards
        )

        total_balance = await self.get_total_balance()

        rate = await self.calculate_rate(
            total_balance
        )


        day_ago = (
            datetime.now(timezone.utc)
            - timedelta(hours=24)
        )


        pipeline = [

            {
                "$match": {
                    "time": {
                        "$gte": day_ago
                    }
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
                                        "central_bank"
                                    ]
                                },
                                "$amount",
                                0
                            ]
                        }
                    },

                    "outcome": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$source",
                                        "central_bank"
                                    ]
                                },
                                "$amount",
                                0
                            ]
                        }
                    }
                }
            }
        ]


        by_reason = await (
            self.transactions_collection
            .aggregate(pipeline)
            .to_list(100)
        )


        income_total = sum(
            item["income"]
            for item in by_reason
        )

        outcome_total = sum(
            item["outcome"]
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

            "total_economy":
                reserve
                + funds
                + total_balance,

            "rate": rate,

            "reserve_change":
                income_total
                - outcome_total,

            "income_by_reason": {
                item["_id"]: item["income"]
                for item in by_reason
                if item["income"] > 0
            },

            "outcome_by_reason": {
                item["_id"]: item["outcome"]
                for item in by_reason
                if item["outcome"] > 0
            }
        }


    # ================================================================
    # HELP
    # ================================================================

    def create_help_embed(self) -> discord.Embed:

        embed = discord.Embed(

            title="🏦 Центральный банк — помощь",

            description=(
                "Все доступные команды бота.\n"
                "Команды работают через `!` и `/`."
            ),

            color=0x00BFFF
        )


        embed.add_field(

            name="💰 Экономика",

            value=(

                "`!cb` • `/cb` — "
                "состояние Центробанка\n"

                "`!economy` • `/economy` — "
                "полная статистика экономики\n"

                "`!rate` • `/rate` — "
                "текущий курс монеты\n"

                "`!chart` • `/chart` — "
                "график курса\n"

                "`!history` • `/history` — "
                "история транзакций"

            ),

            inline=False
        )


        embed.add_field(

            name="🏛️ Управление",

            value=(

                "`!print_money` • `/print_money` — "
                "напечатать деньги\n"

                "`!tax` • `/tax` — "
                "взыскать налог\n"

                "`!fund` • `/fund` — "
                "состояние фондов\n"

                "`!fund_add` • `/fund_add` — "
                "пополнить фонд\n"

                "`!fund_take` • `/fund_take` — "
                "выдать деньги из фонда"

            ),

            inline=False
        )


        embed.add_field(

            name="ℹ️ Помощь",

            value=(

                "`!help` • `/help` — "
                "показать это меню\n\n"

                "Также можно просто "
                "пингануть бота: `@CentralBank`"

            ),

            inline=False
        )


        embed.set_footer(
            text=f"Central Bank • версия {BOT_VERSION}"
        )

        return embed


    async def send_help_message(
        self,
        message: discord.Message
    ):

        embed = self.create_help_embed()

        await message.channel.send(
            embed=embed
        )


    @commands.hybrid_command(
        name="help",
        description="Показать список команд Центрального банка"
    )
    async def help(
        self,
        ctx: commands.Context
    ):

        await ctx.send(
            embed=self.create_help_embed()
        )


    # ================================================================
    # ОТЧЁТЫ
    # ================================================================

    def _fallback_report(
        self,
        stats: dict
    ) -> str:

        change = stats["reserve_change"]

        sign = (
            "+"
            if change >= 0
            else ""
        )

        return (

            f"🏦 Резерв ЦБ: "
            f"**{stats['reserve']:,}**\n"

            f"🏛️ Фонды: "
            f"**{stats['funds']:,}**\n"

            f"👥 У игроков: "
            f"**{stats['total_balance']:,}**\n"

            f"💰 Всего: "
            f"**{stats['total_economy']:,}**\n"

            f"📈 Курс: "
            f"**{stats['rate']:.4f}** ₽\n"

            f"📊 Изменение: "
            f"**{sign}{change:,}**"
        )


    async def generate_report_text(
        self,
        stats: dict
    ) -> str:

        if self.hf_client is None:

            return self._fallback_report(
                stats
            )


        income_lines = "\n".join(

            [
                f"- {key}: +{value:,}"
                for key, value
                in stats[
                    "income_by_reason"
                ].items()
            ]
        )


        outcome_lines = "\n".join(

            [
                f"- {key}: -{value:,}"
                for key, value
                in stats[
                    "outcome_by_reason"
                ].items()
            ]
        )


        prompt = f"""
Ты — аналитик Центрального банка Discord-сервера.

Напиши краткий отчёт на русском языке,
3-5 предложений.

ЦИФРЫ:

Резерв ЦБ:
{stats['reserve']:,}

Фонды:
{stats['funds']:,}

У игроков:
{stats['total_balance']:,}

Всего в экономике:
{stats['total_economy']:,}

Напечатано:
{stats['printed']:,}

Курс:
{stats['rate']:.4f} ₽

Изменение резерва:
{stats['reserve_change']:+,}

Доходы:
{income_lines or "нет"}

Расходы:
{outcome_lines or "нет"}

Не выдумывай никаких цифр.
"""


        try:

            response = await asyncio.wait_for(

                asyncio.to_thread(

                    self.hf_client.chat_completion,

                    model=MODEL_NAME,

                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],

                    max_tokens=250,

                    temperature=0.7
                ),

                timeout=20
            )


            return response.choices[
                0
            ].message.content


        except Exception as e:

            logger.error(
                f"Ошибка HF: {e}"
            )

            return self._fallback_report(
                stats
            )


    # ================================================================
    # ЕЖЕДНЕВНЫЙ ОТЧЁТ
    # ================================================================

    @tasks.loop(
        time=dtime(
            hour=REPORT_HOUR_UTC,
            minute=0,
            tzinfo=timezone.utc
        )
    )
    async def daily_report(self):

        if REPORT_CHANNEL_ID == 0:
            return


        channel = self.get_channel(
            REPORT_CHANNEL_ID
        )


        if not channel:

            try:

                channel = await self.fetch_channel(
                    REPORT_CHANNEL_ID
                )

            except Exception as e:

                logger.error(
                    f"Не удалось получить канал отчёта: {e}"
                )

                return


        stats = await self.collect_stats()

        text = await self.generate_report_text(
            stats
        )


        embed = discord.Embed(

            title="📊 Ежедневный отчёт Центробанка",

            description=text,

            color=0x00BFFF,

            timestamp=datetime.now(
                timezone.utc
            )
        )


        embed.set_footer(
            text="Данные из MongoDB"
        )


        await channel.send(
            embed=embed
        )


    @daily_report.before_loop
    async def before_daily_report(self):

        await self.wait_until_ready()


    # ================================================================
    # READY + SLASH SYNC
    # ================================================================

    async def on_ready(self):

        logger.info(
            f"✅ Бот запущен: {self.user}"
        )


        if not self._synced:

            try:

                guild = discord.Object(
                    id=GUILD_ID
                )


                # Переносим hybrid-команды
                # в конкретный сервер

                self.tree.copy_global_to(
                    guild=guild
                )


                synced = await self.tree.sync(
                    guild=guild
                )


                logger.info(
                    f"✅ Синхронизировано "
                    f"{len(synced)} slash-команд"
                )


            except Exception as e:

                logger.error(
                    "❌ Ошибка синхронизации "
                    f"slash-команд: "
                    f"{type(e).__name__}: {e}"
                )


            self._synced = True


        if (
            REPORT_CHANNEL_ID != 0
            and not self.daily_report.is_running()
        ):

            self.daily_report.start()


    # ================================================================
    # MESSAGE
    # ================================================================

    async def on_message(
        self,
        message: discord.Message
    ):

        # Игнорируем ботов

        if message.author.bot:
            return


        # ------------------------------------------------------------
        # Автоматический HELP при пинге
        # ------------------------------------------------------------

        if (
            self.user
            and self.user in message.mentions
        ):

            content = message.content


            mention_forms = [

                f"<@{self.user.id}>",

                f"<@!{self.user.id}>"

            ]


            for mention in mention_forms:

                content = content.replace(
                    mention,
                    ""
                )


            # Если пользователь просто
            # пинганул бота

            if not content.strip():

                await self.send_help_message(
                    message
                )

                return


        # ------------------------------------------------------------
        # Обычные !команды
        # ------------------------------------------------------------

        await self.process_commands(
            message
        )


    # ================================================================
    # ERRORS
    # ================================================================

    async def on_command_error(
        self,
        ctx: commands.Context,
        error
    ):

        if isinstance(
            error,
            commands.CommandNotFound
        ):
            return


        if isinstance(
            error,
            commands.MissingPermissions
        ):

            await ctx.send(
                "❌ У тебя нет прав "
                "на эту команду."
            )

            return


        if isinstance(
            error,
            commands.MissingRequiredArgument
        ):

            await ctx.send(
                f"❌ Не хватает аргумента: "
                f"`{error.param.name}`"
            )

            return


        if isinstance(
            error,
            commands.BadArgument
        ):

            await ctx.send(
                "❌ Неверный формат аргумента."
            )

            return


        logger.error(
            f"Необработанная ошибка команды: "
            f"{type(error).__name__}: {error}"
        )


        await ctx.send(
            "❌ Произошла ошибка "
            "при выполнении команды."
        )


    # ================================================================
    # !print_money / /print_money
    # ================================================================

    @commands.hybrid_command(
        name="print_money",
        description="Напечатать деньги в резерв ЦБ"
    )
    @app_commands.describe(
        amount="Сколько напечатать"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def print_money(
        self,
        ctx: commands.Context,
        amount: int
    ):

        if amount <= 0:

            await ctx.send(
                "❌ Сумма должна быть положительной."
            )

            return


        if not await self.income_to_reserve(
            amount,
            "money_printer",
            "Печать денег"
        ):

            await ctx.send(
                "❌ Не удалось."
            )

            return


        await self.economy_collection.update_one(

            {
                "_id": "central_bank"
            },

            {
                "$inc": {
                    "printed": amount
                }
            }
        )


        cb = await self.get_central_bank()


        await ctx.send(

            f"🏦 Напечатано: "
            f"**{amount:,}**\n"

            f"💰 Резерв: "
            f"**{cb.get('reserve', 0):,}**\n"

            f"🖨️ Всего напечатано: "
            f"**{cb.get('printed', 0):,}**"
        )


    # ================================================================
    # !cb / /cb
    # ================================================================

    @commands.hybrid_command(
        name="cb",
        description="Показать состояние Центробанка"
    )
    async def cb(
        self,
        ctx: commands.Context
    ):

        cb = await self.get_central_bank()

        total = await self.get_total_balance()


        funds = (

            cb.get(
                "welfare_fund",
                0
            )

            + cb.get(
                "event_fund",
                0
            )

            + cb.get(
                "reward_fund",
                0
            )
        )


        embed = discord.Embed(

            title="🏦 Центральный банк",

            color=0x00BFFF
        )


        embed.add_field(

            name="🏦 Резерв",

            value=f"{cb.get('reserve', 0):,}",

            inline=True
        )


        embed.add_field(

            name="🏛️ Фонды",

            value=f"{funds:,}",

            inline=True
        )


        embed.add_field(

            name="👥 У игроков",

            value=f"{total:,}",

            inline=True
        )


        embed.add_field(

            name="💰 Всего",

            value=(
                f"{cb.get('reserve', 0) "
                f"+ funds + total:,}"
            ),

            inline=False
        )


        embed.add_field(

            name="🖨️ Напечатано",

            value=f"{cb.get('printed', 0):,}",

            inline=False
        )


        embed.add_field(

            name="🤝 Соцфонд",

            value=f"{cb.get('welfare_fund', 0):,}",

            inline=True
        )


        embed.add_field(

            name="🎉 Ивенты",

            value=f"{cb.get('event_fund', 0):,}",

            inline=True
        )


        embed.add_field(

            name="🎁 Награды",

            value=f"{cb.get('reward_fund', 0):,}",

            inline=True
        )


        await ctx.send(
            embed=embed
        )


    # ================================================================
    # !economy / /economy
    # ================================================================

    @commands.hybrid_command(
        name="economy",
        description="Полная экономика: резерв, фонды, доходы и расходы"
    )
    async def economy(
        self,
        ctx: commands.Context
    ):

        stats = await self.collect_stats()


        embed = discord.Embed(

            title="🏦 Экономика Центробанка",

            color=0x00BFFF
        )


        embed.add_field(

            name="🏦 Резерв ЦБ",

            value=f"**{stats['reserve']:,}**",

            inline=True
        )


        embed.add_field(

            name="🏛️ Фонды",

            value=f"**{stats['funds']:,}**",

            inline=True
        )


        embed.add_field(

            name="👥 У игроков",

            value=f"**{stats['total_balance']:,}**",

            inline=True
        )


        embed.add_field(

            name="💰 Всего в экономике",

            value=f"**{stats['total_economy']:,}**",

            inline=False
        )


        if stats["income_by_reason"]:

            income = "\n".join(

                [
                    f"• {key}: **+{value:,}**"
                    for key, value
                    in stats[
                        "income_by_reason"
                    ].items()
                ]
            )


            embed.add_field(

                name="📥 Доходы за 24ч",

                value=income,

                inline=False
            )


        if stats["outcome_by_reason"]:

            outcome = "\n".join(

                [
                    f"• {key}: **-{value:,}**"
                    for key, value
                    in stats[
                        "outcome_by_reason"
                    ].items()
                ]
            )


            embed.add_field(

                name="📤 Расходы за 24ч",

                value=outcome,

                inline=False
            )


        change = stats[
            "reserve_change"
        ]


        sign = (
            "+"
            if change >= 0
            else ""
        )


        embed.add_field(

            name="📊 Изменение резерва",

            value=f"**{sign}{change:,}**",

            inline=False
        )


        embed.set_footer(
            text="Центробанк • данные из MongoDB"
        )


        await ctx.send(
            embed=embed
        )


    # ================================================================
    # !tax / /tax
    # ================================================================

    @commands.hybrid_command(
        name="tax",
        description="Списать налог у игрока в резерв ЦБ"
    )
    @app_commands.describe(

        member="Кого облагаем налогом",

        amount="Сумма налога"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def tax(
        self,
        ctx: commands.Context,
        member: discord.Member,
        amount: int
    ):

        if amount <= 0:

            await ctx.send(
                "❌ Сумма должна быть положительной."
            )

            return


        user = await self.ub_guild.get_user_balance(
            member.id
        )


        if user.cash < amount:

            await ctx.send(

                f"❌ У {member.mention} "
                f"недостаточно денег."
            )

            return


        await user.update(
            cash=-amount
        )


        if not await self.income_to_reserve(

            amount,

            f"user_{member.id}",

            "Налог"
        ):

            await user.update(
                cash=amount
            )


            await ctx.send(
                "❌ Не удалось провести "
                "операцию. Средства возвращены."
            )

            return


        await ctx.send(

            f"💰 Налог "
            f"**{amount:,}** "
            f"списан у "
            f"{member.mention}."
        )


    # ================================================================
    # !fund / /fund
    # ================================================================

    @commands.hybrid_command(
        name="fund",
        description="Показать состояние фондов"
    )
    @app_commands.describe(
        fund_name="welfare, event, reward или all"
    )
    async def fund(
        self,
        ctx: commands.Context,
        fund_name: str = "all"
    ):

        cb = await self.get_central_bank()


        if fund_name == "all":

            embed = discord.Embed(

                title="🏛️ Фонды",

                color=0x00BFFF
            )


            embed.add_field(

                name="🤝 Соцфонд",

                value=f"{cb.get('welfare_fund', 0):,}",

                inline=True
            )


            embed.add_field(

                name="🎉 Ивенты",

                value=f"{cb.get('event_fund', 0):,}",

                inline=True
            )


            embed.add_field(

                name="🎁 Награды",

                value=f"{cb.get('reward_fund', 0):,}",

                inline=True
            )


            await ctx.send(
                embed=embed
            )

            return


        fmap = {

            "welfare":
                "welfare_fund",

            "event":
                "event_fund",

            "reward":
                "reward_fund"
        }


        if fund_name not in fmap:

            await ctx.send(
                "❌ Доступно: "
                "welfare, event, reward"
            )

            return


        await ctx.send(

            f"🏛️ {fund_name}: "
            f"**{cb.get(fmap[fund_name], 0):,}**"
        )


    # ================================================================
    # !fund_add / /fund_add
    # ================================================================

    @commands.hybrid_command(
        name="fund_add",
        description="Пополнить фонд из резерва ЦБ"
    )
    @app_commands.describe(

        fund_name="welfare, event или reward",

        amount="Сумма пополнения"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_add(

        self,

        ctx: commands.Context,

        fund_name: str,

        amount: int
    ):

        if amount <= 0:

            await ctx.send(
                "❌ Сумма должна быть положительной."
            )

            return


        fmap = {

            "welfare":
                "welfare_fund",

            "event":
                "event_fund",

            "reward":
                "reward_fund"
        }


        if fund_name not in fmap:

            await ctx.send(
                "❌ Доступно: "
                "welfare, event, reward"
            )

            return


        if not await self.spend_from_reserve(

            amount,

            f"fund_{fund_name}",

            f"Пополнение {fund_name}"
        ):

            await ctx.send(
                "❌ В ЦБ недостаточно средств."
            )

            return


        await self.economy_collection.update_one(

            {
                "_id": "central_bank"
            },

            {
                "$inc": {
                    fmap[fund_name]:
                        amount
                }
            }
        )


        await ctx.send(

            f"✅ +{amount:,} "
            f"в фонд **{fund_name}**"
        )


    # ================================================================
    # !fund_take / /fund_take
    # ================================================================

    @commands.hybrid_command(
        name="fund_take",
        description="Выдать деньги игроку из фонда"
    )
    @app_commands.describe(

        fund_name="welfare, event или reward",

        amount="Сумма выдачи",

        member="Кому выдать"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_take(

        self,

        ctx: commands.Context,

        fund_name: str,

        amount: int,

        member: discord.Member
    ):

        if amount <= 0:

            await ctx.send(
                "❌ Сумма должна быть положительной."
            )

            return


        fmap = {

            "welfare":
                "welfare_fund",

            "event":
                "event_fund",

            "reward":
                "reward_fund"
        }


        if fund_name not in fmap:

            await ctx.send(
                "❌ Доступно: "
                "welfare, event, reward"
            )

            return


        cb = await self.get_central_bank()


        if cb.get(
            fmap[fund_name],
            0
        ) < amount:

            await ctx.send(
                "❌ В фонде недостаточно."
            )

            return


        await self.economy_collection.update_one(

            {
                "_id": "central_bank"
            },

            {
                "$inc": {
                    fmap[fund_name]:
                        -amount
                }
            }
        )


        user = await self.ub_guild.get_user_balance(
            member.id
        )


        await user.update(
            cash=amount
        )


        await self.add_transaction(

            f"fund_{fund_name}",

            f"user_{member.id}",

            amount,

            f"Выдача из {fund_name}"
        )


        await ctx.send(

            f"✅ {member.mention} "
            f"получил **{amount:,}** "
            f"из фонда {fund_name}."
        )


    # ================================================================
    # !rate / /rate
    # ================================================================

    @commands.hybrid_command(
        name="rate",
        description="Курс монеты к ₽, $, €, ¥"
    )
    async def rate(
        self,
        ctx: commands.Context
    ):

        total = await self.get_total_balance()


        coin_rub = await self.calculate_rate(
            total
        )


        usd = await currency_cache.get_rate(
            USD_CODE
        )

        eur = await currency_cache.get_rate(
            EUR_CODE
        )

        cny = await currency_cache.get_rate(
            CNY_CODE
        )


        coin_usd = (
            coin_rub / usd
            if usd > 0
            else 0
        )


        coin_eur = (
            coin_rub / eur
            if eur > 0
            else 0
        )


        coin_cny = (
            coin_rub / cny
            if cny > 0
            else 0
        )


        embed = discord.Embed(

            title="📈 Курс валюты",

            color=0x00FF00
        )


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

            value=(

                f"USD: {usd:.2f} ₽\n"

                f"EUR: {eur:.2f} ₽\n"

                f"CNY: {cny:.2f} ₽"

            ),

            inline=False
        )


        await ctx.send(
            embed=embed
        )


        await self.db[
            "stats"
        ].update_one(

            {
                "_id":
                    "rate_history"
            },

            {
                "$push": {

                    "history": {

                        "date":
                            datetime.now().strftime(
                                "%d.%m"
                            ),

                        "rate":
                            round(
                                coin_rub,
                                4
                            )
                    }
                }
            },

            upsert=True
        )


    # ================================================================
    # !chart / /chart
    # ================================================================

    @commands.hybrid_command(
        name="chart",
        description="График курса монеты"
    )
    async def chart(
        self,
        ctx: commands.Context
    ):

        doc = await self.db[
            "stats"
        ].find_one(
            {
                "_id":
                    "rate_history"
            }
        )


        history = (
            doc["history"]
            if doc
            else []
        )


        if len(history) < 2:

            await ctx.send(
                "📉 Мало данных. "
                "Повтори `!rate`."
            )

            return


        dates = [
            item["date"]
            for item in history[-30:]
        ]


        rates = [
            item["rate"]
            for item in history[-30:]
        ]


        plt.figure(
            figsize=(8, 5)
        )


        plt.plot(
            dates,
            rates,
            marker="o",
            color="#00BFFF"
        )


        plt.title(
            "Курс валюты"
        )

        plt.xlabel(
            "Дата"
        )

        plt.ylabel(
            "₽"
        )

        plt.grid(
            True,
            alpha=0.3
        )

        plt.xticks(
            rotation=45
        )


        buffer = io.BytesIO()


        plt.savefig(

            buffer,

            format="png",

            bbox_inches="tight",

            dpi=80
        )


        buffer.seek(0)

        plt.close()


        await ctx.send(

            file=discord.File(
                buffer,
                filename="chart.png"
            )
        )


    # ================================================================
    # !history / /history
    # ================================================================

    @commands.hybrid_command(
        name="history",
        description="Последние транзакции Центробанка"
    )
    @app_commands.describe(
        limit="Сколько последних записей показать (1-25)"
    )
    async def history(

        self,

        ctx: commands.Context,

        limit: int = 10
    ):

        limit = max(
            1,
            min(25, limit)
        )


        docs = await (

            self.transactions_collection

            .find()

            .sort(
                "time",
                -1
            )

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

            timestamp = document[
                "time"
            ].strftime(
                "%d.%m %H:%M"
            )


            sign = (

                "+"

                if document[
                    "destination"
                ] == "central_bank"

                else "-"
            )


            lines.append(

                f"`{timestamp}` "
                f"{sign}"
                f"{document['amount']:,} "
                f"— {document['reason']}"
            )


        embed = discord.Embed(

            title="📜 История транзакций",

            description="\n".join(
                lines
            ),

            color=0x00BFFF
        )


        embed.set_footer(

            text=(
                f"Последние "
                f"{len(docs)} записей"
            )
        )


        await ctx.send(
            embed=embed
        )


# ====================================================================
# ТОЧКА ВХОДА
# ====================================================================

def main():

    validate_config()


    # ------------------------------------------------------------
    # Flask
    # ------------------------------------------------------------

    flask_thread = threading.Thread(

        target=run_flask,

        daemon=True
    )


    flask_thread.start()


    logger.info(
        "🌐 Веб-сервер запущен"
    )


    # ------------------------------------------------------------
    # Discord Bot
    # ------------------------------------------------------------

    bot = CentralBankBot()


    try:

        bot.run(
            DISCORD_TOKEN
        )


    except discord.LoginFailure:

        logger.critical(
            "❌ Неверный токен Discord!"
        )


    except Exception as e:

        logger.critical(

            f"❌ Критическая ошибка: "
            f"{type(e).__name__}: {e}"
        )


    finally:

        logger.info(
            "Процесс завершён"
        )


# ====================================================================
# START
# ====================================================================

if __name__ == "__main__":
    main()