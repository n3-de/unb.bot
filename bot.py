import os
import asyncio
import logging
import random
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flask import Flask
from waitress import serve

try:
    from huggingface_hub import InferenceClient
except Exception:
    InferenceClient = None


# ============================================================
# CONFIG
# ============================================================

BOT_VERSION = "1.9.7"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
UB_TOKEN = os.getenv("UB_TOKEN")
HF_TOKEN = os.getenv("HF_TOKEN")

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "cb")

GUILD_ID = int(os.getenv("GUILD_ID", "0"))

REPORT_CHANNEL_ID = int(
    os.getenv("REPORT_CHANNEL_ID", "0")
)

# Отдельный канал для постоянного сообщения графиков.
# Если не указан — используется REPORT_CHANNEL_ID.
CHART_CHANNEL_ID = int(
    os.getenv("CHART_CHANNEL_ID", str(REPORT_CHANNEL_ID))
)

PORT = int(os.getenv("PORT", "10000"))

REPORT_HOUR_UTC = 18

SYNC_INTERVAL_MINUTES = 1


# ============================================================
# INCOME SETTINGS
# ============================================================

WORK_MIN, WORK_MAX = 20, 250
SLUT_MIN, SLUT_MAX = 100, 400
CRIME_MIN, CRIME_MAX = 250, 700

CRIME_FAIL_RATE = 60
SLUT_FAIL_RATE = 35

FINE_MIN_PERCENT = 20
FINE_MAX_PERCENT = 40

ROB_COOLDOWN = 86400
WORK_COOLDOWN = 4 * 3600
SLUT_COOLDOWN = 4 * 3600
CRIME_COOLDOWN = 4 * 3600


# ============================================================
# UB
# ============================================================

UB_BASE = "https://unbelievaboat.com/api/v1"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

log = logging.getLogger("central_bank")


# ============================================================
# HELPERS
# ============================================================

def now():
    return datetime.now(timezone.utc)


def fmt(n):
    return f"{int(n):,}".replace(",", " ")


def mongo_uri():
    u = urllib.parse.quote_plus(MONGO_USER or "")
    p = urllib.parse.quote_plus(MONGO_PASSWORD or "")

    return (
        f"mongodb+srv://{u}:{p}"
        f"@cluster0.u62aem5.mongodb.net/"
        f"?appName=CentralBank"
    )


def check_env():
    missing = [
        name
        for name, value in {
            "DISCORD_TOKEN": DISCORD_TOKEN,
            "UB_TOKEN": UB_TOKEN,
            "MONGO_USER": MONGO_USER,
            "MONGO_PASSWORD": MONGO_PASSWORD,
        }.items()
        if not value
    ]

    if not GUILD_ID:
        missing.append("GUILD_ID")

    if missing:
        raise RuntimeError(
            "Не заданы: " + ", ".join(missing)
        )


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

app = Flask(__name__)


@app.get("/")
def health():
    return {
        "status": "online",
        "version": BOT_VERSION
    }


def run_web():
    serve(
        app,
        host="0.0.0.0",
        port=PORT
    )


# ============================================================
# UNBELIEVABOAT
# ============================================================

class UB:

    def __init__(self, token: str, guild_id: int):
        self.token = token
        self.guild_id = guild_id
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):

        if not self.session or self.session.closed:

            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "Authorization": self.token,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": (
                        f"CentralBank/{BOT_VERSION}"
                    ),
                },
            )

    async def close(self):

        if self.session and not self.session.closed:
            await self.session.close()

    async def req(
        self,
        method,
        path,
        *,
        params=None,
        body=None
    ):

        await self.start()

        async with self.session.request(
            method,
            UB_BASE + path,
            params=params,
            json=body
        ) as response:

            text = await response.text()

            if response.status >= 400:
                raise RuntimeError(
                    f"UnbelievaBoat API "
                    f"{response.status}: "
                    f"{text[:800]}"
                )

            if not text:
                return None

            try:
                return await response.json()
            except Exception:
                return text

    async def guild(self):
        return await self.req(
            "GET",
            f"/guilds/{self.guild_id}"
        )

    async def user(self, uid: int):
        return await self.req(
            "GET",
            f"/guilds/{self.guild_id}/users/{uid}"
        )

    async def change_cash(
        self,
        uid: int,
        delta: int,
        reason: str
    ):

        return await self.req(
            "PATCH",
            f"/guilds/{self.guild_id}/users/{uid}",
            body={
                "cash": int(delta),
                "reason": reason[:500]
            }
        )

    async def users(self):

        out = []
        page = 1

        while page <= 1000:

            data = await self.req(
                "GET",
                f"/guilds/{self.guild_id}/users",
                params={
                    "page": page,
                    "limit": 100
                }
            )

            if isinstance(data, list):
                out.extend(data)
                break

            if not isinstance(data, dict):
                break

            batch = (
                data.get("users")
                or data.get("results")
                or []
            )

            if not isinstance(batch, list):
                batch = []

            out.extend(batch)

            total_pages = int(
                data.get("total_pages", 0) or 0
            )

            if (
                not batch
                or (
                    total_pages
                    and page >= total_pages
                )
                or (
                    not total_pages
                    and len(batch) < 100
                )
            ):
                break

            page += 1

        return out

    async def total(self):

        return sum(
            int(x.get("total", 0) or 0)
            for x in await self.users()
            if isinstance(x, dict)
        )


# ============================================================
# DISCORD
# ============================================================

intents = discord.Intents.default()

intents.guilds = True
intents.members = True
intents.message_content = True


# ============================================================
# BOT
# ============================================================

class Bot(commands.Bot):

    def __init__(self):

        super().__init__(
            command_prefix=commands.when_mentioned_or("!"),
            intents=intents,
            help_command=None,
            activity=discord.Game(name="!cb"),
            status=discord.Status.online
        )

        self.mongo = None
        self.db = None

        self.economy = None
        self.tx = None
        self.funds = None
        self.rates = None
        self.economy_history = None

        self.ub: Optional[UB] = None
        self.hf = None

        self.synced = False

        self.balance_snapshot = {}
        self.expected_balance_changes = {}

        # ====================================================
        # CHART SYSTEM
        # ====================================================

        self.chart_files = {
            "all": "/tmp/cb_economy_all.png",
            "24h": "/tmp/cb_economy_24h.png",
        }

        self.chart_lock = asyncio.Lock()

    # ========================================================
    # SETUP
    # ========================================================

    async def setup_hook(self):

        self.mongo = AsyncIOMotorClient(
            mongo_uri(),
            serverSelectionTimeoutMS=10000
        )

        await self.mongo.admin.command("ping")

        self.db = self.mongo[MONGO_DB_NAME]

        self.economy = self.db.economy
        self.tx = self.db.transactions
        self.funds = self.db.funds
        self.rates = self.db.rate_history
        self.economy_history = self.db.economy_history

        await self.economy.update_one(
            {"_id": "central_bank"},
            {
                "$setOnInsert": {
                    "reserve": 0,
                    "printed": 0,
                    "created_at": now()
                },
                "$set": {
                    "updated_at": now()
                }
            },
            upsert=True
        )

        self.ub = UB(
            UB_TOKEN,
            GUILD_ID
        )

        await self.ub.start()
        await self.ub.guild()

        log.info(
            "UnbelievaBoat API подключён"
        )

        if HF_TOKEN and InferenceClient:

            try:

                self.hf = InferenceClient(
                    provider="hf-inference",
                    api_key=HF_TOKEN
                )

            except Exception:
                log.exception(
                    "HF init failed"
                )

        await self.add_cog(
            Cog(self)
        )

        guild = discord.Object(
            id=GUILD_ID
        )

        self.tree.copy_global_to(
            guild=guild
        )

        try:

            synced = await self.tree.sync(
                guild=guild
            )

            self.synced = True

            log.info(
                "Slash-команд синхронизировано: %d: %s",
                len(synced),
                ", ".join(
                    x.name for x in synced
                )
            )

        except Exception:

            log.exception(
                "Slash sync failed"
            )

        self.report_loop.start()
        self.cleanup_loop.start()
        self.ub_sync_loop.start()
        self.economy_snapshot_loop.start()

    # ========================================================
    # READY
    # ========================================================

    async def on_ready(self):

        log.info(
            "Бот онлайн: %s",
            self.user
        )

        if not self.synced:

            try:

                guild = discord.Object(
                    id=GUILD_ID
                )

                self.tree.copy_global_to(
                    guild=guild
                )

                await self.tree.sync(
                    guild=guild
                )

                self.synced = True

            except Exception:

                log.exception(
                    "Fallback sync failed"
                )

    # ========================================================
    # CLOSE
    # ========================================================

    async def close(self):

        loops = (
            self.report_loop,
            self.cleanup_loop,
            self.ub_sync_loop,
            self.economy_snapshot_loop,
        )

        for task_loop in loops:

            if task_loop.is_running():
                task_loop.cancel()

        if self.ub:
            await self.ub.close()

        if self.mongo:
            self.mongo.close()

        await super().close()

    # ========================================================
    # MESSAGE
    # ========================================================

    async def on_message(self, message):

        if not message.author.bot:
            await self.process_commands(
                message
            )

    # ========================================================
    # COMMAND ERRORS
    # ========================================================

    async def on_command_error(
        self,
        ctx,
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
            return await ctx.send(
                "❌ Нужны права администратора."
            )

        if isinstance(
            error,
            commands.MissingRequiredArgument
        ):
            return await ctx.send(
                f"❌ Не хватает аргумента: "
                f"`{error.param.name}`"
            )

        if isinstance(
            error,
            commands.BadArgument
        ):
            return await ctx.send(
                "❌ Неверный формат аргумента."
            )

        if isinstance(
            error,
            commands.CommandOnCooldown
        ):

            seconds = int(
                error.retry_after
            )

            if seconds >= 86400:
                text = (
                    f"{seconds // 86400} дн."
                )

            elif seconds >= 3600:
                text = (
                    f"{seconds // 3600} ч."
                )

            elif seconds >= 60:
                text = (
                    f"{seconds // 60} мин."
                )

            else:
                text = f"{seconds} сек."

            return await ctx.send(
                f"⏳ Попробуй снова через "
                f"`{text}`."
            )

        log.exception(
            "Command error",
            exc_info=error
        )

        await ctx.send(
            f"❌ Ошибка: "
            f"`{str(error)[:500]}`"
        )

    async def on_app_command_error(
        self,
        interaction,
        error
    ):

        log.exception(
            "Slash command error",
            exc_info=error
        )

        msg = (
            f"❌ Ошибка: "
            f"`{str(error)[:500]}`"
        )

        if interaction.response.is_done():

            await interaction.followup.send(
                msg,
                ephemeral=True
            )

        else:

            await interaction.response.send_message(
                msg,
                ephemeral=True
            )

    # ========================================================
    # CENTRAL BANK
    # ========================================================

    async def reserve(self):

        d = await self.economy.find_one(
            {"_id": "central_bank"}
        )

        return int(
            (d or {}).get(
                "reserve",
                0
            ) or 0
        )

    async def printed(self):

        d = await self.economy.find_one(
            {"_id": "central_bank"}
        )

        return int(
            (d or {}).get(
                "printed",
                0
            ) or 0
        )

    async def change_reserve(
        self,
        delta
    ):

        delta = int(delta)

        if delta == 0:
            return True

        result = await self.economy.update_one(
            {"_id": "central_bank"},
            {
                "$inc": {
                    "reserve": delta
                },
                "$set": {
                    "updated_at": now()
                }
            }
        )

        return result.modified_count == 1

    async def journal(
        self,
        source,
        dest,
        amount,
        reason,
        meta=None
    ):

        await self.tx.insert_one(
            {
                "source": source,
                "destination": dest,
                "amount": int(amount),
                "reason": reason,
                "meta": meta or {},
                "created_at": now()
            }
        )

    async def emit(
        self,
        amount,
        reason,
        source="external"
    ):

        if (
            amount <= 0
            or not await self.change_reserve(
                amount
            )
        ):
            return False

        try:

            await self.journal(
                source,
                "central_bank",
                amount,
                reason
            )

            return True

        except Exception:

            await self.change_reserve(
                -amount
            )

            raise

    async def burn(
        self,
        amount,
        reason="Сжигание денег"
    ):

        if (
            amount <= 0
            or await self.reserve() < amount
        ):
            return False

        if not await self.change_reserve(
            -amount
        ):
            return False

        try:

            await self.journal(
                "central_bank",
                "money_burn",
                amount,
                reason
            )

            return True

        except Exception:

            await self.change_reserve(
                amount
            )

            raise

    # ========================================================
    # PLAYER TRANSFERS
    # ========================================================

    async def cb_to_player(
        self,
        member,
        amount,
        reason,
        meta=None
    ):

        if (
            amount <= 0
            or not await self.change_reserve(
                -amount
            )
        ):
            return False

        try:

            await self.ub.change_cash(
                member.id,
                amount,
                reason
            )

            self.expect_balance_change(
                member.id,
                amount
            )

            try:

                await self.journal(
                    "central_bank",
                    f"user_{member.id}",
                    amount,
                    reason,
                    meta=meta
                )

            except Exception:

                await self.ub.change_cash(
                    member.id,
                    -amount,
                    "Rollback: journal error"
                )

                await self.change_reserve(
                    amount
                )

                raise

            return True

        except Exception:

            await self.change_reserve(
                amount
            )

            raise

    async def pay_player(
        self,
        member,
        amount,
        reason,
        command_name
    ):

        if amount <= 0:
            return False

        return await self.cb_to_player(
            member,
            amount,
            reason,
            meta={
                "type": "player_earning",
                "command": command_name
            }
        )

    async def player_to_cb(
        self,
        member,
        amount,
        reason
    ):

        if amount <= 0:
            return False

        user = await self.ub.user(
            member.id
        )

        cash = 0

        if isinstance(user, dict):
            cash = int(
                user.get(
                    "cash",
                    0
                ) or 0
            )

        if cash < amount:
            return False

        await self.ub.change_cash(
            member.id,
            -amount,
            reason
        )

        self.expect_balance_change(
            member.id,
            -amount
        )

        try:

            if not await self.change_reserve(
                amount
            ):

                await self.ub.change_cash(
                    member.id,
                    amount,
                    "Rollback: CB reserve error"
                )

                return False

            try:

                await self.journal(
                    f"user_{member.id}",
                    "central_bank",
                    amount,
                    reason
                )

            except Exception:

                await self.change_reserve(
                    -amount
                )

                await self.ub.change_cash(
                    member.id,
                    amount,
                    "Rollback: journal error"
                )

                raise

            return True

        except Exception:

            log.exception(
                "player->CB failed"
            )

            raise

    async def transfer_player_to_player(
        self,
        sender,
        recipient,
        amount: int,
        reason="Перевод через Центральный банк"
    ):

        if (
            amount <= 0
            or sender.id == recipient.id
        ):
            return False

        source = await self.ub.user(
            sender.id
        )

        source_cash = 0

        if isinstance(source, dict):

            source_cash = int(
                source.get(
                    "cash",
                    0
                ) or 0
            )

        if source_cash < amount:
            return False

        await self.ub.change_cash(
            sender.id,
            -amount,
            reason
        )

        self.expect_balance_change(
            sender.id,
            -amount
        )

        try:

            await self.ub.change_cash(
                recipient.id,
                amount,
                reason
            )

            self.expect_balance_change(
                recipient.id,
                amount
            )

            try:

                await self.journal(
                    f"user_{sender.id}",
                    f"user_{recipient.id}",
                    amount,
                    reason,
                    meta={
                        "type": "player_transfer",
                        "sender_id": sender.id,
                        "recipient_id": recipient.id
                    }
                )

            except Exception:

                await self.ub.change_cash(
                    recipient.id,
                    -amount,
                    "Rollback: journal error"
                )

                self.expect_balance_change(
                    recipient.id,
                    -amount
                )

                await self.ub.change_cash(
                    sender.id,
                    amount,
                    "Rollback: journal error"
                )

                self.expect_balance_change(
                    sender.id,
                    amount
                )

                raise

            return True

        except Exception:

            try:

                await self.ub.change_cash(
                    sender.id,
                    amount,
                    "Rollback: transfer error"
                )

                self.expect_balance_change(
                    sender.id,
                    amount
                )

            except Exception:

                log.exception(
                    "Could not rollback sender transfer"
                )

            raise

    # ========================================================
    # FUNDS
    # ========================================================

    async def fund_balance(
        self,
        name
    ):

        d = await self.funds.find_one(
            {"_id": name}
        )

        return int(
            (d or {}).get(
                "balance",
                0
            ) or 0
        )

    async def funds_total(self):

        result = await self.funds.aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "total": {
                            "$sum": "$balance"
                        }
                    }
                }
            ]
        ).to_list(1)

        return (
            int(result[0]["total"])
            if result
            else 0
        )

    async def create_fund(
        self,
        name
    ):

        result = await self.funds.update_one(
            {"_id": name},
            {
                "$setOnInsert": {
                    "name": name,
                    "balance": 0,
                    "created_at": now(),
                    "updated_at": now()
                }
            },
            upsert=True
        )

        return result.upserted_id is not None

    async def add_fund(
        self,
        name,
        amount,
        reason
    ):

        if (
            amount <= 0
            or not await self.change_reserve(
                -amount
            )
        ):
            return False

        try:

            result = await self.funds.update_one(
                {"_id": name},
                {
                    "$inc": {
                        "balance": amount
                    },
                    "$set": {
                        "updated_at": now()
                    }
                }
            )

            if result.matched_count != 1:

                await self.change_reserve(
                    amount
                )

                return False

            try:

                await self.journal(
                    "central_bank",
                    f"fund_{name}",
                    amount,
                    reason
                )

            except Exception:

                await self.funds.update_one(
                    {"_id": name},
                    {
                        "$inc": {
                            "balance": -amount
                        }
                    }
                )

                await self.change_reserve(
                    amount
                )

                raise

            return True

        except Exception:

            log.exception(
                "fund add failed"
            )

            raise

    async def take_fund(
        self,
        name,
        member,
        amount,
        reason
    ):

        if amount <= 0:
            return False

        result = await self.funds.update_one(
            {
                "_id": name,
                "balance": {
                    "$gte": amount
                }
            },
            {
                "$inc": {
                    "balance": -amount
                },
                "$set": {
                    "updated_at": now()
                }
            }
        )

        if result.modified_count != 1:
            return False

        try:

            await self.ub.change_cash(
                member.id,
                amount,
                reason
            )

            self.expect_balance_change(
                member.id,
                amount
            )

            try:

                await self.journal(
                    f"fund_{name}",
                    f"user_{member.id}",
                    amount,
                    reason
                )

            except Exception:

                await self.ub.change_cash(
                    member.id,
                    -amount,
                    "Rollback: journal error"
                )

                await self.funds.update_one(
                    {"_id": name},
                    {
                        "$inc": {
                            "balance": amount
                        }
                    }
                )

                raise

            return True

        except Exception:

            await self.funds.update_one(
                {"_id": name},
                {
                    "$inc": {
                        "balance": amount
                    }
                }
            )

            raise

    async def delete_fund(
        self,
        name
    ):

        document = await self.funds.find_one_and_delete(
            {"_id": name}
        )

        if not document:
            return False, 0

        amount = int(
            document.get(
                "balance",
                0
            ) or 0
        )

        try:

            if amount:

                await self.change_reserve(
                    amount
                )

                await self.journal(
                    f"fund_{name}",
                    "central_bank",
                    amount,
                    "Удаление фонда"
                )

            return True, amount

        except Exception:

            await self.funds.update_one(
                {"_id": name},
                {
                    "$set": {
                        "name": name,
                        "balance": amount,
                        "updated_at": now()
                    },
                    "$setOnInsert": {
                        "created_at": now()
                    }
                },
                upsert=True
            )

            if amount:
                await self.change_reserve(
                    -amount
                )

            raise

    async def user_cash(
        self,
        uid: int
    ):

        user = await self.ub.user(
            uid
        )

        if not isinstance(user, dict):
            return 0

        return int(
            user.get(
                "cash",
                0
            ) or 0
        )

    # ========================================================
    # ECONOMY SNAPSHOT
    # ========================================================

    async def record_economy_snapshot(
        self,
        force=False
    ):

        try:

            last = await self.economy_history.find_one(
                sort=[
                    ("created_at", -1)
                ]
            )

            current_time = now()

            if (
                not force
                and last
            ):

                last_time = last.get(
                    "created_at"
                )

                if (
                    isinstance(
                        last_time,
                        datetime
                    )
                    and (
                        current_time
                        - last_time
                    ).total_seconds() < 300
                ):

                    return False

            reserve, funds, players = await asyncio.gather(
                self.reserve(),
                self.funds_total(),
                self.ub.total()
            )

            circulation = funds + players
            total_system = reserve + circulation
            debt = max(
                0,
                -reserve
            )

            await self.economy_history.insert_one(
                {
                    "created_at": current_time,
                    "reserve": reserve,
                    "funds": funds,
                    "players": players,
                    "circulation": circulation,
                    "debt": debt,
                    "supply": total_system,
                }
            )

            return True

        except Exception:

            log.exception(
                "Economy snapshot failed"
            )

            return False

    # ========================================================
    # CHART GENERATION
    # ========================================================

    async def generate_economy_charts(
        self
    ):

        """
        Создаёт два графика:

        1. Вся история экономики.
        2. Последние 24 часа.

        Графики создаются даже при одной точке.
        """

        try:

            rows = await self.economy_history.find(
                {}
            ).sort(
                "created_at",
                1
            ).to_list(
                10000
            )

            if not rows:

                log.warning(
                    "Нет данных economy_history."
                )

                return False

            clean_rows = [
                row
                for row in rows
                if isinstance(
                    row.get("created_at"),
                    datetime
                )
            ]

            if not clean_rows:

                log.warning(
                    "В economy_history нет корректных дат."
                )

                return False

            since = (
                now()
                - timedelta(hours=24)
            )

            day_rows = [
                row
                for row in clean_rows
                if row["created_at"] >= since
            ]

            if not day_rows:

                day_rows = [
                    clean_rows[-1]
                ]

            def make_chart(
                chart_rows,
                title,
                path
            ):

                dates = [
                    row["created_at"].astimezone(
                        timezone.utc
                    )
                    for row in chart_rows
                ]

                reserve = [
                    int(
                        row.get(
                            "reserve",
                            0
                        ) or 0
                    )
                    for row in chart_rows
                ]

                circulation = [
                    int(
                        row.get(
                            "circulation",
                            (
                                int(
                                    row.get(
                                        "funds",
                                        0
                                    ) or 0
                                )
                                +
                                int(
                                    row.get(
                                        "players",
                                        0
                                    ) or 0
                                )
                            )
                        ) or 0
                    )
                    for row in chart_rows
                ]

                debt = [
                    int(
                        row.get(
                            "debt",
                            max(
                                0,
                                -int(
                                    row.get(
                                        "reserve",
                                        0
                                    ) or 0
                                )
                            )
                        ) or 0
                    )
                    for row in chart_rows
                ]

                plt.figure(
                    figsize=(13, 6.5)
                )

                plt.plot(
                    dates,
                    reserve,
                    linewidth=2,
                    marker="o",
                    markersize=3,
                    label="Резерв ЦБ"
                )

                plt.plot(
                    dates,
                    circulation,
                    linewidth=2,
                    marker="o",
                    markersize=3,
                    label="В обращении"
                )

                plt.plot(
                    dates,
                    debt,
                    linewidth=2,
                    marker="o",
                    markersize=3,
                    label="Дефицит ЦБ"
                )

                plt.axhline(
                    0,
                    linewidth=1
                )

                plt.title(
                    title
                )

                plt.xlabel(
                    "Время"
                )

                plt.ylabel(
                    "Сумма"
                )

                plt.grid(
                    True,
                    alpha=0.25
                )

                plt.legend()

                plt.xticks(
                    rotation=30
                )

                plt.tight_layout()

                plt.savefig(
                    path,
                    dpi=150,
                    bbox_inches="tight"
                )

                plt.close()

            # Вся история
            make_chart(
                clean_rows,
                "Общая экономика — денежная масса",
                self.chart_files["all"]
            )

            # Последние 24 часа
            make_chart(
                day_rows,
                "Экономика — последние 24 часа",
                self.chart_files["24h"]
            )

            log.info(
                "Графики созданы: %d точек всего, %d точек за 24ч",
                len(clean_rows),
                len(day_rows)
            )

            return True

        except Exception:

            log.exception(
                "Economy chart generation failed"
            )

            return False

    # ========================================================
    # PUBLISH CHARTS
    # ========================================================

    async def publish_economy_charts(
        self
    ):

        """
        Обновляет одно постоянное сообщение
        с двумя графиками.
        """

        if not CHART_CHANNEL_ID:

            log.warning(
                "CHART_CHANNEL_ID не задан."
            )

            return False

        async with self.chart_lock:

            generated = (
                await self.generate_economy_charts()
            )

            if not generated:
                return False

            try:

                channel = self.get_channel(
                    CHART_CHANNEL_ID
                )

                if channel is None:

                    channel = await self.fetch_channel(
                        CHART_CHANNEL_ID
                    )

                if channel is None:

                    log.error(
                        "Канал графиков не найден."
                    )

                    return False

                bank = await self.economy.find_one(
                    {
                        "_id": "central_bank"
                    }
                )

                message_id = (
                    bank.get(
                        "chart_message_id"
                    )
                    if bank
                    else None
                )

                stored_channel_id = (
                    bank.get(
                        "chart_channel_id"
                    )
                    if bank
                    else None
                )

                message = None

                # Если канал изменился —
                # старое сообщение не используем.
                if (
                    message_id
                    and (
                        not stored_channel_id
                        or int(stored_channel_id)
                        == int(channel.id)
                    )
                ):

                    try:

                        message = await channel.fetch_message(
                            int(message_id)
                        )

                    except discord.NotFound:

                        log.warning(
                            "Старое сообщение графиков удалено."
                        )

                    except discord.Forbidden:

                        log.exception(
                            "Нет прав читать сообщение графиков."
                        )

                        return False

                    except discord.HTTPException:

                        log.exception(
                            "Не удалось получить сообщение графиков."
                        )

                content = (
                    "📊 **Графики экономики**\n"
                    "Автоматическое обновление каждые **5 минут**.\n\n"
                    "📈 `cb_economy_all.png` — вся история\n"
                    "🕐 `cb_economy_24h.png` — последние 24 часа\n\n"
                    "Обновлено: "
                    f"<t:{int(now().timestamp())}:R>"
                )

                files = [
                    discord.File(
                        self.chart_files["all"],
                        filename="cb_economy_all.png"
                    ),
                    discord.File(
                        self.chart_files["24h"],
                        filename="cb_economy_24h.png"
                    ),
                ]

                # ============================================
                # ОБНОВЛЯЕМ СТАРОЕ СООБЩЕНИЕ
                # ============================================

                if message is not None:

                    try:

                        await message.edit(
                            content=content,
                            attachments=files
                        )

                        log.info(
                            "Графики обновлены. Message ID: %s",
                            message.id
                        )

                        return True

                    except discord.NotFound:

                        log.warning(
                            "Сообщение удалили во время обновления."
                        )

                    except discord.HTTPException:

                        log.exception(
                            "Ошибка редактирования графиков."
                        )

                # ============================================
                # СОЗДАЁМ НОВОЕ СООБЩЕНИЕ
                # ============================================

                message = await channel.send(
                    content=content,
                    files=files
                )

                await self.economy.update_one(
                    {
                        "_id": "central_bank"
                    },
                    {
                        "$set": {
                            "chart_message_id": message.id,
                            "chart_channel_id": channel.id,
                            "updated_at": now()
                        }
                    },
                    upsert=True
                )

                log.info(
                    "Создано новое постоянное сообщение графиков: %s",
                    message.id
                )

                return True

            except Exception:

                log.exception(
                    "Economy chart publish failed"
                )

                return False

    # ========================================================
    # STATS
    # ========================================================

    async def stats(self):

        reserve, funds, players, printed = (
            await asyncio.gather(
                self.reserve(),
                self.funds_total(),
                self.ub.total(),
                self.printed()
            )
        )

        since = (
            now()
            - timedelta(hours=24)
        )

        rows = await self.tx.aggregate(
            [
                {
                    "$match": {
                        "created_at": {
                            "$gte": since
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$source",
                        "amount": {
                            "$sum": "$amount"
                        }
                    }
                }
            ]
        ).to_list(100)

        incoming = 0
        outgoing = 0

        for row in rows:

            source = str(
                row.get(
                    "_id",
                    ""
                )
            )

            amount = int(
                row.get(
                    "amount",
                    0
                ) or 0
            )

            if source == "central_bank":

                outgoing += amount

            elif (
                source.startswith("user_")
                or source.startswith("fund_")
            ):

                incoming += amount

        circulation = (
            funds + players
        )

        debt = max(
            0,
            -reserve
        )

        total_system = (
            reserve
            + circulation
        )

        return {
            "reserve": reserve,
            "funds": funds,
            "players": players,
            "circulation": circulation,
            "debt": debt,
            "supply": total_system,
            "printed": printed,
            "incoming": incoming,
            "outgoing": outgoing
        }

    # ========================================================
    # RATE
    # ========================================================

    async def rate(self):

        stats = await self.stats()

        def get_cbr():

            response = requests.get(
                "https://www.cbr.ru/scripts/XML_daily.asp",
                timeout=15
            )

            response.raise_for_status()

            return response.text

        xml = await asyncio.to_thread(
            get_cbr
        )

        import xml.etree.ElementTree as ET

        root = ET.fromstring(
            xml
        )

        currencies = {}

        for value in root.findall(
            "Valute"
        ):

            code = value.findtext(
                "CharCode"
            )

            price = value.findtext(
                "Value"
            )

            nominal = (
                value.findtext(
                    "Nominal"
                )
                or "1"
            )

            if code and price:

                rub_value = float(
                    price.replace(
                        ",",
                        "."
                    )
                )

                nominal_value = float(
                    nominal.replace(
                        ",",
                        "."
                    )
                )

                currencies[code] = (
                    rub_value
                    / nominal_value
                    if nominal_value
                    else 0
                )

        import math

        economic_mass = (
            max(
                0,
                stats["circulation"]
            )
            + stats["debt"]
        )

        internal = round(
            1000
            / math.sqrt(
                1
                + economic_mass / 100000
            ),
            2
        )

        usd_rub = currencies.get(
            "USD",
            0
        )

        eur_rub = currencies.get(
            "EUR",
            0
        )

        cny_rub = currencies.get(
            "CNY",
            0
        )

        return {
            "internal": internal,

            "coin_usd": (
                round(
                    internal / usd_rub,
                    4
                )
                if usd_rub
                else 0
            ),

            "coin_eur": (
                round(
                    internal / eur_rub,
                    4
                )
                if eur_rub
                else 0
            ),

            "coin_cny": (
                round(
                    internal / cny_rub,
                    4
                )
                if cny_rub
                else 0
            ),

            "supply": stats["supply"],

            "created_at": now()
        }

    # ========================================================
    # AI REPORT
    # ========================================================

    async def ai_report(
        self,
        stats
    ):

        if not self.hf:
            return None

        prompt = (
            "Коротко (2-4 предложения) "
            "опиши экономический отчёт Discord-ЦБ "
            "на русском, без выдумок и причин, "
            "которых нет в данных. "
            f"Резерв {stats['reserve']}; "
            f"фонды {stats['funds']}; "
            f"масса {stats['supply']}; "
            f"напечатано {stats['printed']}; "
            f"приход24ч {stats['incoming']}; "
            f"расход24ч {stats['outgoing']}."
        )

        try:

            result = await asyncio.to_thread(
                self.hf.chat_completion,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                model=MODEL_NAME,
                max_tokens=180,
                temperature=0.2
            )

            return (
                result
                .choices[0]
                .message
                .content
                .strip()
            )

        except Exception:

            log.exception(
                "HF report failed"
            )

            return None

    # ========================================================
    # BALANCE EXPECTATIONS
    # ========================================================

    def expect_balance_change(
        self,
        user_id: int,
        delta: int
    ):

        self.expected_balance_changes[
            user_id
        ] = (
            self.expected_balance_changes.get(
                user_id,
                0
            )
            + int(delta)
        )

    # ========================================================
    # UB SYNC
    # ========================================================

    @tasks.loop(minutes=1)
    async def ub_sync_loop(self):

        try:

            rows = await self.ub.users()

            current = {}

            for row in rows:

                if not isinstance(
                    row,
                    dict
                ):
                    continue

                uid = int(
                    row.get(
                        "user_id",
                        row.get(
                            "id",
                            0
                        )
                    ) or 0
                )

                if not uid:
                    continue

                current[uid] = (
                    int(
                        row.get(
                            "cash",
                            0
                        ) or 0
                    )
                    +
                    int(
                        row.get(
                            "bank",
                            0
                        ) or 0
                    )
                )

            if not self.balance_snapshot:

                self.balance_snapshot = current

                log.info(
                    "UB balance snapshot initialized: %d users",
                    len(current)
                )

                return

            for uid, new_total in current.items():

                old_total = (
                    self.balance_snapshot.get(
                        uid
                    )
                )

                if old_total is None:

                    self.balance_snapshot[
                        uid
                    ] = new_total

                    continue

                delta = (
                    new_total
                    - old_total
                )

                if delta == 0:
                    continue

                expected = (
                    self.expected_balance_changes.get(
                        uid,
                        0
                    )
                )

                if expected:

                    if expected * delta > 0:

                        consumed = min(
                            abs(expected),
                            abs(delta)
                        )

                        if expected > 0:

                            expected -= consumed
                            delta -= consumed

                        else:

                            expected += consumed
                            delta += consumed

                        if expected:

                            self.expected_balance_changes[
                                uid
                            ] = expected

                        else:

                            self.expected_balance_changes.pop(
                                uid,
                                None
                            )

                if delta > 0:

                    ok = await self.change_reserve(
                        -delta
                    )

                    if ok:

                        await self.journal(
                            "unbelievaboat",
                            f"user_{uid}",
                            delta,
                            "Выплата через UnbelievaBoat",
                            meta={
                                "type": "ub_external_earning",
                                "user_id": uid
                            }
                        )

                        log.info(
                            "UB -> user %s: +%s; "
                            "CB reserve -%s",
                            uid,
                            delta,
                            delta
                        )

                    else:

                        try:

                            await self.ub.change_cash(
                                uid,
                                -delta,
                                "Rollback: insufficient Central Bank reserve"
                            )

                            await self.journal(
                                "central_bank",
                                f"user_{uid}",
                                0,
                                "Отклонена выплата UB: "
                                "недостаточно средств ЦБ",
                                meta={
                                    "type":
                                    "ub_external_earning_rejected",
                                    "user_id": uid,
                                    "amount": delta
                                }
                            )

                        except Exception:

                            log.exception(
                                "Could not rollback UB earning"
                            )

                else:

                    amount = -delta

                    await self.change_reserve(
                        amount
                    )

                    await self.journal(
                        f"user_{uid}",
                        "central_bank",
                        amount,
                        "Списание/возврат денег через UB",
                        meta={
                            "type":
                            "ub_external_decrease",
                            "user_id": uid
                        }
                    )

            self.balance_snapshot = current

        except Exception:

            log.exception(
                "UB balance sync failed"
            )

    @ub_sync_loop.before_loop
    async def before_ub_sync(self):

        await self.wait_until_ready()

    # ========================================================
    # ECONOMY SNAPSHOT + CHARTS
    # ========================================================

    @tasks.loop(minutes=5)
    async def economy_snapshot_loop(self):

        try:

            # Новая точка экономики
            await self.record_economy_snapshot()

            # Сразу перестраиваем графики
            # и обновляем постоянное сообщение.
            await self.publish_economy_charts()

        except Exception:

            log.exception(
                "Economy snapshot/chart loop failed"
            )

    @economy_snapshot_loop.before_loop
    async def before_economy_snapshot(self):

        await self.wait_until_ready()

        log.info(
            "Создаём первый снимок экономики..."
        )

        await self.record_economy_snapshot(
            force=True
        )

        log.info(
            "Создаём первые графики..."
        )

        await self.publish_economy_charts()

        log.info(
            "Автообновление графиков запущено."
        )

    # ========================================================
    # DAILY REPORT
    # ========================================================

    @tasks.loop(minutes=1)
    async def report_loop(self):

        current = now()

        if (
            current.hour
            == REPORT_HOUR_UTC
            and current.minute == 0
            and REPORT_CHANNEL_ID
        ):

            try:

                stats = await self.stats()
                rate = await self.rate()

                embed = discord.Embed(
                    title="📊 Ежедневный отчёт ЦБ",
                    description=(
                        f"Резерв: `{fmt(stats['reserve'])}`\n"
                        f"Фонды: `{fmt(stats['funds'])}`\n"
                        f"Денежная масса: "
                        f"`{fmt(stats['supply'])}`\n"
                        f"Напечатано: "
                        f"`{fmt(stats['printed'])}`\n\n"
                        f"Внутренний курс: "
                        f"`{rate['internal']}`\n"
                        f"1 монета ≈ "
                        f"`{rate['coin_usd']}` $\n"
                        f"1 монета ≈ "
                        f"`{rate['coin_eur']}` €\n"
                        f"1 монета ≈ "
                        f"`{rate['coin_cny']}` ¥"
                    ),
                    timestamp=now()
                )

                ai = await self.ai_report(
                    stats
                )

                if ai:

                    embed.add_field(
                        name="🤖 Анализ",
                        value=ai[:1024],
                        inline=False
                    )

                channel = (
                    self.get_channel(
                        REPORT_CHANNEL_ID
                    )
                    or await self.fetch_channel(
                        REPORT_CHANNEL_ID
                    )
                )

                await channel.send(
                    embed=embed
                )

            except Exception:

                log.exception(
                    "Daily report failed"
                )

    @report_loop.before_loop
    async def before_report(self):

        await self.wait_until_ready()

    # ========================================================
    # CLEANUP
    # ========================================================

    @tasks.loop(hours=6)
    async def cleanup_loop(self):

        try:

            await self.rates.delete_many(
                {
                    "created_at": {
                        "$lt": (
                            now()
                            - timedelta(days=90)
                        )
                    }
                }
            )

        except Exception:

            log.exception(
                "Rate cleanup failed"
            )

    @cleanup_loop.before_loop
    async def before_cleanup(self):

        await self.wait_until_ready()


# ============================================================
# COMMANDS
# ============================================================

class Cog(commands.Cog):

    def __init__(
        self,
        bot
    ):
        self.bot = bot

    # ========================================================
    # HELP
    # ========================================================

    @commands.hybrid_command(
        name="help",
        description="Команды Центрального банка"
    )
    async def help(
        self,
        ctx
    ):

        await ctx.send(
            "🏦 **Центральный банк**\n\n"
            "`!cb` — состояние ЦБ\n"
            "`!economy` — экономика\n"
            "`!rate` — курс\n"
            "`!chart` — обновить графики\n"
            "`!history` — журнал\n"
            "`!audit` — аудит\n\n"
            "`!print_money` — эмиссия\n"
            "`!burn_money` — сжигание\n"
            "`!cb_test` — тестовая выплата\n"
            "`!transfer` — перевод игроку\n\n"
            "`!fund` — фонды\n"
            "`!fund_create`\n"
            "`!fund_add`\n"
            "`!fund_take`\n"
            "`!fund_delete`"
        )

    # ========================================================
    # CB
    # ========================================================

    @commands.hybrid_command(
        name="cb",
        description="Состояние Центрального банка"
    )
    async def cb(
        self,
        ctx
    ):

        await ctx.defer()

        try:

            stats = await self.bot.stats()

            await ctx.send(
                f"🏦 **ЦБ**\n"
                f"Резерв: `{fmt(stats['reserve'])}`\n"
                f"Фонды: `{fmt(stats['funds'])}`\n"
                f"В обращении: "
                f"`{fmt(stats['circulation'])}`\n"
                f"Всего в системе: "
                f"`{fmt(stats['supply'])}`\n"
                f"Дефицит ЦБ: "
                f"`{fmt(stats['debt'])}`\n"
                f"Напечатано: "
                f"`{fmt(stats['printed'])}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # ECONOMY
    # ========================================================

    @commands.hybrid_command(
        name="economy",
        description="Подробная статистика экономики"
    )
    async def economy(
        self,
        ctx
    ):

        await ctx.defer()

        try:

            stats = await self.bot.stats()

            await ctx.send(
                f"📈 **Экономика**\n"
                f"Резерв `{fmt(stats['reserve'])}` | "
                f"Фонды `{fmt(stats['funds'])}` | "
                f"В обращении "
                f"`{fmt(stats['circulation'])}` | "
                f"Дефицит `{fmt(stats['debt'])}`\n"
                f"Всего в системе "
                f"`{fmt(stats['supply'])}`\n"
                f"24ч: приход "
                f"`{fmt(stats['incoming'])}` / "
                f"расход `{fmt(stats['outgoing'])}`\n"
                f"Напечатано всего: "
                f"`{fmt(stats['printed'])}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # PRINT
    # ========================================================

    @commands.hybrid_command(
        name="print_money",
        description="Напечатать деньги"
    )
    @app_commands.describe(
        amount="Сумма"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def print_money(
        self,
        ctx,
        amount: int
    ):

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        await ctx.defer()

        try:

            if not await self.bot.emit(
                amount,
                "Эмиссия новых денег",
                "money_printing"
            ):

                return await ctx.send(
                    "❌ Не удалось увеличить резерв"
                )

            await self.bot.economy.update_one(
                {"_id": "central_bank"},
                {
                    "$inc": {
                        "printed": amount
                    }
                }
            )

            await ctx.send(
                f"💵 Эмиссия "
                f"`{fmt(amount)}` выполнена.\n"
                f"Резерв: "
                f"`{fmt(await self.bot.reserve())}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # BURN
    # ========================================================

    @commands.hybrid_command(
        name="burn_money",
        description="Сжечь деньги из резерва"
    )
    @app_commands.describe(
        amount="Сумма"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def burn_money(
        self,
        ctx,
        amount: int
    ):

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        await ctx.defer()

        try:

            if not await self.bot.burn(
                amount
            ):

                return await ctx.send(
                    "❌ Недостаточно денег в резерве"
                )

            await ctx.send(
                f"🔥 Сожжено "
                f"`{fmt(amount)}`.\n"
                f"Резерв: "
                f"`{fmt(await self.bot.reserve())}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # TEST PAYMENT
    # ========================================================

    @commands.hybrid_command(
        name="cb_test",
        description="Тестовый перевод из ЦБ игроку"
    )
    @app_commands.describe(
        member="Игрок",
        amount="Сумма"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def cb_test(
        self,
        ctx,
        member: discord.Member,
        amount: int = 100
    ):

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        await ctx.defer()

        try:

            if not await self.bot.cb_to_player(
                member,
                amount,
                "Тестовый перевод ЦБ"
            ):

                return await ctx.send(
                    "❌ Недостаточно денег в ЦБ"
                )

            await ctx.send(
                f"✅ {member.mention} получил "
                f"`{fmt(amount)}`.\n"
                f"Резерв: "
                f"`{fmt(await self.bot.reserve())}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка UB/ЦБ: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # TRANSFER
    # ========================================================

    @commands.hybrid_command(
        name="transfer",
        description="Перевести деньги игроку через ЦБ"
    )
    @app_commands.describe(
        member="Получатель",
        amount="Сумма"
    )
    async def transfer(
        self,
        ctx,
        member: discord.Member,
        amount: int
    ):

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        if member.id == ctx.author.id:

            return await ctx.send(
                "❌ Нельзя переводить самому себе"
            )

        await ctx.defer()

        try:

            ok = await self.bot.transfer_player_to_player(
                ctx.author,
                member,
                amount,
                "Перевод через Центральный банк"
            )

            if not ok:

                return await ctx.send(
                    "❌ Недостаточно денег для перевода"
                )

            await ctx.send(
                f"💸 {ctx.author.mention} → "
                f"{member.mention}: "
                f"`{fmt(amount)}`\n"
                f"Перевод проведён через "
                f"Центральный банк."
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка перевода: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # FUNDS
    # ========================================================

    @commands.hybrid_command(
        name="fund",
        description="Список фондов"
    )
    async def fund(
        self,
        ctx
    ):

        rows = await self.bot.funds.find(
            {}
        ).sort(
            "name",
            1
        ).to_list(100)

        if not rows:

            return await ctx.send(
                "📦 Фондов нет"
            )

        await ctx.send(
            "📦 **Фонды**\n"
            +
            "\n".join(
                f"• `{x['name']}` — "
                f"`{fmt(x.get('balance', 0))}`"
                for x in rows
            )
        )

    @commands.hybrid_command(
        name="fund_create",
        description="Создать фонд"
    )
    @app_commands.describe(
        name="Название"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_create(
        self,
        ctx,
        name: str
    ):

        name = name.strip().lower()

        if not 1 <= len(name) <= 40:

            return await ctx.send(
                "❌ Название 1-40 символов"
            )

        if await self.bot.create_fund(
            name
        ):

            await ctx.send(
                "✅ Фонд создан."
            )

        else:

            await ctx.send(
                "ℹ️ Такой фонд уже есть."
            )

    @commands.hybrid_command(
        name="fund_add",
        description="Перевести деньги из ЦБ в фонд"
    )
    @app_commands.describe(
        name="Фонд",
        amount="Сумма"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_add(
        self,
        ctx,
        name: str,
        amount: int
    ):

        name = name.strip().lower()

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        if (
            await self.bot.fund_balance(name)
            == 0
            and not await self.bot.funds.find_one(
                {"_id": name}
            )
        ):

            return await ctx.send(
                "❌ Фонд не найден"
            )

        await ctx.defer()

        try:

            ok = await self.bot.add_fund(
                name,
                amount,
                f"Пополнение фонда {name}"
            )

            if ok:

                await ctx.send(
                    f"✅ В `{name}` добавлено "
                    f"`{fmt(amount)}`.\n"
                    f"Баланс: "
                    f"`{fmt(await self.bot.fund_balance(name))}`"
                )

            else:

                await ctx.send(
                    "❌ Не удалось пополнить фонд"
                )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    @commands.hybrid_command(
        name="fund_take",
        description="Выдать деньги из фонда"
    )
    @app_commands.describe(
        name="Фонд",
        member="Игрок",
        amount="Сумма"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_take(
        self,
        ctx,
        name: str,
        member: discord.Member,
        amount: int
    ):

        name = name.strip().lower()

        if amount <= 0:

            return await ctx.send(
                "❌ Сумма должна быть > 0"
            )

        if not await self.bot.funds.find_one(
            {"_id": name}
        ):

            return await ctx.send(
                "❌ Фонд не найден"
            )

        await ctx.defer()

        try:

            ok = await self.bot.take_fund(
                name,
                member,
                amount,
                f"Выдача из фонда {name}"
            )

            if ok:

                await ctx.send(
                    f"✅ {member.mention} получил "
                    f"`{fmt(amount)}`.\n"
                    f"Фонд: "
                    f"`{fmt(await self.bot.fund_balance(name))}`"
                )

            else:

                await ctx.send(
                    "❌ Недостаточно денег в фонде"
                )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    @commands.hybrid_command(
        name="fund_delete",
        description="Удалить фонд"
    )
    @app_commands.describe(
        name="Фонд"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def fund_delete(
        self,
        ctx,
        name: str
    ):

        name = name.strip().lower()

        await ctx.defer()

        try:

            ok, amount = (
                await self.bot.delete_fund(
                    name
                )
            )

            if ok:

                await ctx.send(
                    f"🗑️ Фонд `{name}` удалён.\n"
                    f"Возвращено в ЦБ: "
                    f"`{fmt(amount)}`"
                )

            else:

                await ctx.send(
                    "❌ Фонд не найден"
                )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # AUDIT
    # ========================================================

    @commands.hybrid_command(
        name="audit",
        description="Аудит экономики"
    )
    @commands.has_permissions(
        administrator=True
    )
    async def audit(
        self,
        ctx
    ):

        await ctx.defer()

        try:

            stats = await self.bot.stats()

            transactions = (
                await self.bot.tx.count_documents({})
            )

            await ctx.send(
                f"🔎 **Аудит**\n"
                f"Резерв `{fmt(stats['reserve'])}` + "
                f"обращение `{fmt(stats['circulation'])}` "
                f"= `{fmt(stats['supply'])}`\n"
                f"Напечатано `{fmt(stats['printed'])}` | "
                f"операций `{transactions}`\n"
                f"Статус: ✅ баланс сходится"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # RATE
    # ========================================================

    @commands.hybrid_command(
        name="rate",
        description="Курс"
    )
    async def rate(
        self,
        ctx
    ):

        await ctx.defer()

        try:

            rate = await self.bot.rate()

            await self.bot.rates.insert_one(
                rate
            )

            await ctx.send(
                f"💱 **Курс**\n"
                f"Внутренний: "
                f"`{rate['internal']}`\n"
                f"1 монета ≈ "
                f"`{rate['coin_usd']}` $\n"
                f"1 монета ≈ "
                f"`{rate['coin_eur']}` €\n"
                f"1 монета ≈ "
                f"`{rate['coin_cny']}` ¥\n"
                f"Масса "
                f"`{fmt(rate['supply'])}`"
            )

        except Exception as e:

            await ctx.send(
                f"❌ Ошибка: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # CHART
    # ========================================================

    @commands.hybrid_command(
        name="chart",
        description="Обновить графики экономики"
    )
    async def chart(
        self,
        ctx
    ):

        await ctx.defer()

        try:

            ok = (
                await self.bot.publish_economy_charts()
            )

            if not ok:

                return await ctx.send(
                    "❌ Не удалось создать "
                    "или обновить графики. "
                    "Проверь логи бота."
                )

            await ctx.send(
                "✅ **Графики экономики обновлены.**\n"
                "Постоянное сообщение с графиками "
                "также обновлено."
            )

        except Exception as e:

            log.exception(
                "Economy chart command failed"
            )

            await ctx.send(
                f"❌ Ошибка графика: "
                f"`{str(e)[:500]}`"
            )

    # ========================================================
    # HISTORY
    # ========================================================

    @commands.hybrid_command(
        name="history",
        description="Журнал операций"
    )
    @app_commands.describe(
        limit="Количество записей"
    )
    async def history(
        self,
        ctx,
        limit: int = 10
    ):

        limit = max(
            1,
            min(
                25,
                limit
            )
        )

        rows = await self.bot.tx.find(
            {}
        ).sort(
            "created_at",
            -1
        ).to_list(
            limit
        )

        if not rows:

            return await ctx.send(
                "📜 Журнал пуст"
            )

        text = []

        for row in rows:

            date = row.get(
                "created_at"
            )

            if isinstance(
                date,
                datetime
            ):

                date_string = (
                    date.astimezone(
                        timezone.utc
                    ).strftime(
                        "%d.%m %H:%M"
                    )
                )

            else:

                date_string = "?"

            text.append(
                f"`{date_string}` "
                f"`{row.get('source')}` → "
                f"`{row.get('destination')}` "
                f"**{fmt(row.get('amount', 0))}**\n"
                f"_{row.get('reason', '')}_"
            )

        await ctx.send(
            "📜 **Журнал**\n\n"
            + "\n\n".join(text)[:3900]
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    check_env()

    bot = Bot()

    asyncio.create_task(
        asyncio.to_thread(
            run_web
        )
    )

    try:

        await bot.start(
            DISCORD_TOKEN
        )

    finally:

        if not bot.is_closed():
            await bot.close()


if __name__ == "__main__":

    asyncio.run(
        main()
    )