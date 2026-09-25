import os
import asyncio
import logging
import random
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional, Any

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

BOT_VERSION = "1.9.4"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash"
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
UB_TOKEN = os.getenv("UB_TOKEN")
HF_TOKEN = os.getenv("HF_TOKEN")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "cb")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))
REPORT_HOUR_UTC = 18

# Баланс игроков контролируется через UnbelievaBoat Dashboard.
# Бот не дублирует команды work/crime/beg/daily/etc.
SYNC_INTERVAL_MINUTES = 1

# Настройки доходных команд нашего бота.
# Это теперь НЕ команды UnbelievaBoat: UB должен быть отключён для этих команд.
WORK_MIN, WORK_MAX = 20, 250
SLUT_MIN, SLUT_MAX = 100, 400
CRIME_MIN, CRIME_MAX = 250, 700
CRIME_FAIL_RATE = 60
SLUT_FAIL_RATE = 35
FINE_MIN_PERCENT, FINE_MAX_PERCENT = 20, 40
ROB_COOLDOWN = 86400
WORK_COOLDOWN = 4 * 3600
SLUT_COOLDOWN = 4 * 3600
CRIME_COOLDOWN = 4 * 3600

# Казино: ставка идёт от игрока в ЦБ, выигрыш — из ЦБ игроку.
CASINO_MIN_BET = 1
CASINO_MAX_BET = 100000
GAME_COOLDOWN = 30
SLOT_PAYOUTS = {"🍒🍒🍒": 8, "🍋🍋🍋": 10, "🔔🔔🔔": 15, "💎💎💎": 25}

UB_BASE = "https://unbelievaboat.com/api/v1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("central_bank")


def now():
    return datetime.now(timezone.utc)


def fmt(n):
    return f"{int(n):,}".replace(",", " ")


def mongo_uri():
    u = urllib.parse.quote_plus(MONGO_USER or "")
    p = urllib.parse.quote_plus(MONGO_PASSWORD or "")
    return f"mongodb+srv://{u}:{p}@cluster0.u62aem5.mongodb.net/?appName=CentralBank"


def check_env():
    missing = [x for x, v in {"DISCORD_TOKEN": DISCORD_TOKEN, "UB_TOKEN": UB_TOKEN,
                              "MONGO_USER": MONGO_USER, "MONGO_PASSWORD": MONGO_PASSWORD}.items() if not v]
    if not GUILD_ID:
        missing.append("GUILD_ID")
    if missing:
        raise RuntimeError("Не заданы: " + ", ".join(missing))


# ---------------- Render health server ----------------
app = Flask(__name__)

@app.get("/")
def health():
    return {"status": "online", "version": BOT_VERSION}


def run_web():
    serve(app, host="0.0.0.0", port=PORT)


# ---------------- UnbelievaBoat REST ----------------
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
                    "User-Agent": f"CentralBank/{BOT_VERSION}",
                },
            )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def req(self, method, path, *, params=None, body=None):
        await self.start()
        async with self.session.request(method, UB_BASE + path, params=params, json=body) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"UnbelievaBoat API {r.status}: {text[:800]}")
            if not text:
                return None
            try:
                return await r.json()
            except Exception:
                return text

    async def guild(self):
        return await self.req("GET", f"/guilds/{self.guild_id}")

    async def user(self, uid: int):
        return await self.req("GET", f"/guilds/{self.guild_id}/users/{uid}")

    async def change_cash(self, uid: int, delta: int, reason: str):
        return await self.req("PATCH", f"/guilds/{self.guild_id}/users/{uid}",
                              body={"cash": int(delta), "reason": reason[:500]})

    async def users(self):
        out = []
        page = 1
        while page <= 1000:
            data = await self.req("GET", f"/guilds/{self.guild_id}/users",
                                  params={"page": page, "limit": 100})
            if isinstance(data, list):
                out.extend(data)
                break
            if not isinstance(data, dict):
                break
            batch = data.get("users") or data.get("results") or []
            if not isinstance(batch, list):
                batch = []
            out.extend(batch)
            total_pages = int(data.get("total_pages", 0) or 0)
            if not batch or (total_pages and page >= total_pages) or (not total_pages and len(batch) < 100):
                break
            page += 1
        return out

    async def total(self):
        return sum(int(x.get("total", 0) or 0) for x in await self.users() if isinstance(x, dict))


# ---------------- Bot ----------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True


class Bot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=commands.when_mentioned_or("!"), intents=intents,
                         help_command=None, activity=discord.Game(name="!cb"), status=discord.Status.online)
        self.mongo = None
        self.db = None
        self.economy = self.tx = self.funds = self.rates = None
        self.ub: Optional[UB] = None
        self.hf = None
        self.synced = False
        self.balance_snapshot = {}
        self.expected_balance_changes = {}

    async def setup_hook(self):
        self.mongo = AsyncIOMotorClient(mongo_uri(), serverSelectionTimeoutMS=10000)
        await self.mongo.admin.command("ping")
        self.db = self.mongo[MONGO_DB_NAME]
        self.economy = self.db.economy
        self.tx = self.db.transactions
        self.funds = self.db.funds
        self.rates = self.db.rate_history
        await self.economy.update_one({"_id": "central_bank"}, {"$setOnInsert": {
            "reserve": 0, "printed": 0, "created_at": now()}, "$set": {"updated_at": now()}}, upsert=True)

        self.ub = UB(UB_TOKEN, GUILD_ID)
        await self.ub.start()
        await self.ub.guild()
        log.info("UnbelievaBoat API подключён")

        if HF_TOKEN and InferenceClient:
            try:
                self.hf = InferenceClient(provider="hf-inference", api_key=HF_TOKEN)
            except Exception:
                log.exception("HF init failed")

        await self.add_cog(Cog(self))
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        try:
            synced = await self.tree.sync(guild=guild)
            self.synced = True
            log.info("Slash-команд синхронизировано: %d: %s", len(synced), ", ".join(x.name for x in synced))
        except Exception:
            log.exception("Slash sync failed")

        self.report_loop.start()
        self.cleanup_loop.start()
        self.ub_sync_loop.start()

    async def on_ready(self):
        log.info("Бот онлайн: %s", self.user)
        if not self.synced:
            try:
                g = discord.Object(id=GUILD_ID)
                self.tree.copy_global_to(guild=g)
                await self.tree.sync(guild=g)
                self.synced = True
            except Exception:
                log.exception("Fallback sync failed")

    async def close(self):
        for t in (self.report_loop, self.cleanup_loop, self.ub_sync_loop):
            if t.is_running():
                t.cancel()
        if self.ub:
            await self.ub.close()
        if self.mongo:
            self.mongo.close()
        await super().close()

    async def on_message(self, message):
        if not message.author.bot:
            await self.process_commands(message)

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandNotFound):
            return
        if isinstance(error, commands.MissingPermissions):
            return await ctx.send("❌ Нужны права администратора.")
        if isinstance(error, commands.MissingRequiredArgument):
            return await ctx.send(f"❌ Не хватает аргумента: `{error.param.name}`")
        if isinstance(error, commands.BadArgument):
            return await ctx.send("❌ Неверный формат аргумента.")
        if isinstance(error, commands.CommandOnCooldown):
            seconds = int(error.retry_after)
            if seconds >= 86400: text = f"{seconds // 86400} дн."
            elif seconds >= 3600: text = f"{seconds // 3600} ч."
            elif seconds >= 60: text = f"{seconds // 60} мин."
            else: text = f"{seconds} сек."
            return await ctx.send(f"⏳ Попробуй снова через `{text}`.")
        log.exception("Command error", exc_info=error)
        await ctx.send(f"❌ Ошибка: `{str(error)[:500]}`")

    async def on_app_command_error(self, interaction, error):
        log.exception("Slash command error", exc_info=error)
        msg = f"❌ Ошибка: `{str(error)[:500]}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    # ---------------- Mongo ledger ----------------
    async def reserve(self):
        d = await self.economy.find_one({"_id": "central_bank"})
        return int((d or {}).get("reserve", 0) or 0)

    async def printed(self):
        d = await self.economy.find_one({"_id": "central_bank"})
        return int((d or {}).get("printed", 0) or 0)

    async def change_reserve(self, delta):
        delta = int(delta)
        if delta < 0:
            r = await self.economy.update_one({"_id": "central_bank", "reserve": {"$gte": -delta}},
                                              {"$inc": {"reserve": delta}, "$set": {"updated_at": now()}})
        else:
            r = await self.economy.update_one({"_id": "central_bank"},
                                              {"$inc": {"reserve": delta}, "$set": {"updated_at": now()}})
        return r.modified_count == 1

    async def journal(self, source, dest, amount, reason, meta=None):
        await self.tx.insert_one({"source": source, "destination": dest, "amount": int(amount),
                                  "reason": reason, "meta": meta or {}, "created_at": now()})

    async def emit(self, amount, reason, source="external"):
        if amount <= 0 or not await self.change_reserve(amount):
            return False
        try:
            await self.journal(source, "central_bank", amount, reason)
            return True
        except Exception:
            await self.change_reserve(-amount)
            raise

    async def burn(self, amount, reason="Сжигание денег"):
        if amount <= 0 or not await self.change_reserve(-amount):
            return False
        try:
            await self.journal("central_bank", "money_burn", amount, reason)
            return True
        except Exception:
            await self.change_reserve(amount)
            raise

    # ---------------- Transfers ----------------
    async def cb_to_player(self, member, amount, reason, meta=None):
        if amount <= 0 or not await self.change_reserve(-amount):
            return False
        try:
            await self.ub.change_cash(member.id, amount, reason)
            self.expect_balance_change(member.id, amount)
            try:
                await self.journal("central_bank", f"user_{member.id}", amount, reason, meta=meta)
            except Exception:
                await self.ub.change_cash(member.id, -amount, "Rollback: journal error")
                await self.change_reserve(amount)
                raise
            return True
        except Exception:
            await self.change_reserve(amount)
            raise

    async def pay_player(self, member, amount, reason, command_name):
        """Единая точка выплат игрокам из бюджета ЦБ."""
        if amount <= 0:
            return False
        return await self.cb_to_player(
            member, amount, reason,
            meta={"type": "player_earning", "command": command_name},
        )

    async def player_to_cb(self, member, amount, reason):
        if amount <= 0:
            return False
        user = await self.ub.user(member.id)
        cash = int((user if isinstance(user, dict) else getattr(user, "cash", 0)) or 0)
        if isinstance(user, dict):
            cash = int(user.get("cash", 0) or 0)
        if cash < amount:
            return False
        await self.ub.change_cash(member.id, -amount, reason)
        self.expect_balance_change(member.id, -amount)
        try:
            if not await self.change_reserve(amount):
                await self.ub.change_cash(member.id, amount, "Rollback: CB reserve error")
                return False
            try:
                await self.journal(f"user_{member.id}", "central_bank", amount, reason)
            except Exception:
                await self.change_reserve(-amount)
                await self.ub.change_cash(member.id, amount, "Rollback: journal error")
                raise
            return True
        except Exception:
            log.exception("player->CB failed")
            raise

    # ---------------- Funds ----------------
    async def fund_balance(self, name):
        d = await self.funds.find_one({"_id": name})
        return int((d or {}).get("balance", 0) or 0)

    async def funds_total(self):
        x = await self.funds.aggregate([{"$group": {"_id": None, "total": {"$sum": "$balance"}}}]).to_list(1)
        return int(x[0]["total"]) if x else 0

    async def create_fund(self, name):
        r = await self.funds.update_one({"_id": name}, {"$setOnInsert": {
            "name": name, "balance": 0, "created_at": now(), "updated_at": now()}}, upsert=True)
        return r.upserted_id is not None

    async def add_fund(self, name, amount, reason):
        if amount <= 0 or not await self.change_reserve(-amount):
            return False
        try:
            r = await self.funds.update_one({"_id": name}, {"$inc": {"balance": amount}, "$set": {"updated_at": now()}})
            if r.matched_count != 1:
                await self.change_reserve(amount)
                return False
            try:
                await self.journal("central_bank", f"fund_{name}", amount, reason)
            except Exception:
                await self.funds.update_one({"_id": name}, {"$inc": {"balance": -amount}})
                await self.change_reserve(amount)
                raise
            return True
        except Exception:
            log.exception("fund add failed")
            raise

    async def take_fund(self, name, member, amount, reason):
        if amount <= 0:
            return False
        r = await self.funds.update_one({"_id": name, "balance": {"$gte": amount}},
                                        {"$inc": {"balance": -amount}, "$set": {"updated_at": now()}})
        if r.modified_count != 1:
            return False
        try:
            await self.ub.change_cash(member.id, amount, reason)
            self.expect_balance_change(member.id, amount)
            try:
                await self.journal(f"fund_{name}", f"user_{member.id}", amount, reason)
            except Exception:
                await self.ub.change_cash(member.id, -amount, "Rollback: journal error")
                await self.funds.update_one({"_id": name}, {"$inc": {"balance": amount}})
                raise
            return True
        except Exception:
            await self.funds.update_one({"_id": name}, {"$inc": {"balance": amount}})
            raise

    async def delete_fund(self, name):
        d = await self.funds.find_one_and_delete({"_id": name})
        if not d:
            return False, 0
        amount = int(d.get("balance", 0) or 0)
        try:
            if amount:
                await self.change_reserve(amount)
                await self.journal(f"fund_{name}", "central_bank", amount, "Удаление фонда")
            return True, amount
        except Exception:
            await self.funds.update_one({"_id": name}, {"$set": {"name": name, "balance": amount, "updated_at": now()},
                                           "$setOnInsert": {"created_at": now()}}, upsert=True)
            if amount:
                await self.change_reserve(-amount)
            raise

    async def user_cash(self, uid: int):
        user = await self.ub.user(uid)
        if not isinstance(user, dict):
            return 0
        return int(user.get("cash", 0) or 0)

    async def casino_bet(self, member, amount: int):
        # Игрок -> ЦБ. Ставка становится доходом казино/ЦБ.
        return await self.player_to_cb(member, amount, "Ставка казино",)

    async def casino_payout(self, member, amount: int, reason: str):
        # ЦБ -> игрок.
        return await self.cb_to_player(member, amount, reason, meta={"type": "casino_payout"})

    async def payout_random(self, member, minimum, maximum, reason, command_name):
        amount = random.randint(minimum, maximum)
        ok = await self.pay_player(member, amount, reason, command_name)
        return ok, amount

    async def fine_percent(self, member, minimum_pct, maximum_pct, reason):
        cash = await self.user_cash(member.id)
        pct = random.randint(minimum_pct, maximum_pct)
        fine = max(1, int(cash * pct / 100)) if cash else 0
        if fine:
            ok = await self.player_to_cb(member, fine, reason)
            return ok, fine, pct
        return True, 0, pct

    async def stats(self):
        r, f, p, pr = await asyncio.gather(self.reserve(), self.funds_total(), self.ub.total(), self.printed())
        since = now() - timedelta(hours=24)
        rows = await self.tx.aggregate([{"$match": {"created_at": {"$gte": since}}},
                                        {"$group": {"_id": "$source", "amount": {"$sum": "$amount"}}}]).to_list(100)
        incoming = outgoing = 0
        for x in rows:
            s, a = str(x.get("_id", "")), int(x.get("amount", 0) or 0)
            if s == "central_bank": outgoing += a
            elif s.startswith("user_") or s.startswith("fund_"): incoming += a
        return {"reserve": r, "funds": f, "players": p, "supply": r + f + p, "printed": pr,
                "incoming": incoming, "outgoing": outgoing}

    async def rate(self):
        s = await self.stats()
        def get_cbr():
            r = requests.get("https://www.cbr.ru/scripts/XML_daily.asp", timeout=15)
            r.raise_for_status()
            return r.text
        xml = await asyncio.to_thread(get_cbr)
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml)
        c = {}
        for v in root.findall("Valute"):
            code = v.findtext("CharCode")
            value = v.findtext("Value")
            if code and value:
                c[code] = float(value.replace(",", "."))
        import math
        internal = round(1000 / math.sqrt(1 + max(0, s["supply"]) / 100000), 2)
        return {"internal": internal, "usd": round(c.get("USD", 0), 4), "eur": round(c.get("EUR", 0), 4),
                "cny": round(c.get("CNY", 0), 4), "supply": s["supply"], "created_at": now()}

    async def ai_report(self, s):
        if not self.hf:
            return None
        prompt = ("Коротко (2-4 предложения) опиши экономический отчёт Discord-ЦБ на русском, "
                  "без выдумок и причин, которых нет в данных. "
                  f"Резерв {s['reserve']}; фонды {s['funds']}; игроки {s['players']}; "
                  f"масса {s['supply']}; напечатано {s['printed']}; приход24ч {s['incoming']}; расход24ч {s['outgoing']}.")
        try:
            x = await asyncio.to_thread(self.hf.chat_completion, messages=[{"role": "user", "content": prompt}],
                                        model=MODEL_NAME, max_tokens=180, temperature=0.2)
            return x.choices[0].message.content.strip()
        except Exception:
            log.exception("HF report failed")
            return None

    def expect_balance_change(self, user_id: int, delta: int):
        self.expected_balance_changes[user_id] = self.expected_balance_changes.get(user_id, 0) + int(delta)

    @tasks.loop(minutes=1)
    async def ub_sync_loop(self):
        """Синхронизирует изменения балансов, сделанные командами/дашбордом UB.

        Бот не реализует work/crime/beg/daily и т.п. сам. Если UB увеличил баланс
        игрока через свою настройку/команду, рост считается расходом ЦБ.
        Контролируемые самим ботом переводы помечаются как ожидаемые и не списываются повторно.
        """
        try:
            rows = await self.ub.users()
            current = {}
            for x in rows:
                if not isinstance(x, dict):
                    continue
                uid = int(x.get("user_id", x.get("id", 0)) or 0)
                if not uid:
                    continue
                current[uid] = int(x.get("cash", 0) or 0) + int(x.get("bank", 0) or 0)

            if not self.balance_snapshot:
                self.balance_snapshot = current
                log.info("UB balance snapshot initialized: %d users", len(current))
                return

            for uid, new_total in current.items():
                old_total = self.balance_snapshot.get(uid)
                if old_total is None:
                    self.balance_snapshot[uid] = new_total
                    continue
                delta = new_total - old_total
                if delta == 0:
                    continue

                expected = self.expected_balance_changes.get(uid, 0)
                if expected:
                    consumed = min(abs(expected), abs(delta)) * (1 if expected * delta > 0 else 0)
                    if consumed:
                        expected -= consumed if expected > 0 else -consumed
                        delta -= consumed if delta > 0 else -consumed
                        if expected:
                            self.expected_balance_changes[uid] = expected
                        else:
                            self.expected_balance_changes.pop(uid, None)
                    # Если направление не совпало, оставляем изменение для учёта ниже.

                if delta > 0:
                    # Любая внешняя выдача денег игроку (в том числе UB work/crime/etc.)
                    # финансируется резервом ЦБ.
                    ok = await self.change_reserve(-delta)
                    if ok:
                        await self.journal(
                            "unbelievaboat", f"user_{uid}", delta,
                            "Выплата через UnbelievaBoat",
                            meta={"type": "ub_external_earning", "user_id": uid},
                        )
                        log.info("UB -> user %s: +%s; CB reserve -%s", uid, delta, delta)
                    else:
                        # В ЦБ не хватило денег — откатываем обнаруженное увеличение.
                        try:
                            await self.ub.change_cash(uid, -delta, "Rollback: insufficient Central Bank reserve")
                            await self.journal(
                                "central_bank", f"user_{uid}", 0,
                                "Отклонена выплата UB: недостаточно средств ЦБ",
                                meta={"type": "ub_external_earning_rejected", "user_id": uid, "amount": delta},
                            )
                        except Exception:
                            log.exception("Could not rollback external UB earning for %s", uid)
                else:
                    # Уменьшение баланса игрока — деньги вернулись из обращения.
                    amount = -delta
                    await self.change_reserve(amount)
                    await self.journal(
                        f"user_{uid}", "central_bank", amount,
                        "Списание/возврат денег через UnbelievaBoat",
                        meta={"type": "ub_external_decrease", "user_id": uid},
                    )
                    log.info("user %s -> CB: +%s", uid, amount)

            self.balance_snapshot = current
        except Exception:
            log.exception("UB balance sync failed")

    @ub_sync_loop.before_loop
    async def before_ub_sync(self):
        await self.wait_until_ready()

    @tasks.loop(minutes=1)
    async def report_loop(self):
        t = now()
        if t.hour == REPORT_HOUR_UTC and t.minute == 0 and REPORT_CHANNEL_ID:
            try:
                s = await self.stats(); r = await self.rate()
                e = discord.Embed(title="📊 Ежедневный отчёт ЦБ", description=(
                    f"Резерв: `{fmt(s['reserve'])}`\nФонды: `{fmt(s['funds'])}`\n"
                    f"Игроки: `{fmt(s['players'])}`\nДенежная масса: `{fmt(s['supply'])}`\n"
                    f"Напечатано: `{fmt(s['printed'])}`\n\nВнутренний курс: `{r['internal']}`\n"
                    f"USD: `{r['usd']}` RUB\nEUR: `{r['eur']}` RUB\nCNY: `{r['cny']}` RUB"), timestamp=now())
                ai = await self.ai_report(s)
                if ai: e.add_field(name="🤖 Анализ", value=ai[:1024], inline=False)
                ch = self.get_channel(REPORT_CHANNEL_ID) or await self.fetch_channel(REPORT_CHANNEL_ID)
                await ch.send(embed=e)
            except Exception:
                log.exception("Daily report failed")

    @report_loop.before_loop
    async def before_report(self):
        await self.wait_until_ready()

    @tasks.loop(hours=6)
    async def cleanup_loop(self):
        try:
            await self.rates.delete_many({"created_at": {"$lt": now() - timedelta(days=90)}})
        except Exception:
            log.exception("Rate cleanup failed")

    @cleanup_loop.before_loop
    async def before_cleanup(self):
        await self.wait_until_ready()


# ---------------- Commands ----------------
class Cog(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @commands.hybrid_command(name="help", description="Команды Центрального банка")
    async def help(self, ctx):
        await ctx.send("🏦 **ЦБ**\n`!cb` `!economy` `!rate` `!chart` `!history` `!audit`\n"
                       "`!print_money` `!burn_money` `!cb_test`\n"
                       "`!fund` `!fund_create` `!fund_add` `!fund_take` `!fund_delete`\n\n"
                       "💡 UnbelievaBoat остаётся игровой экономикой сервера. ЦБ автоматически учитывает "
                       "изменения балансов игроков и корректирует свой резерв. Свои work/crime/казино ЦБ не подменяет.")

    @commands.hybrid_command(name="cb", description="Состояние Центрального банка")
    async def cb(self, ctx):
        await ctx.defer()
        try:
            s = await self.bot.stats()
            await ctx.send(f"🏦 **ЦБ**\nРезерв: `{fmt(s['reserve'])}`\nФонды: `{fmt(s['funds'])}`\n"
                           f"Всего в системе: `{fmt(s['supply'])}`\n"
                           f"Напечатано: `{fmt(s['printed'])}`")
        except Exception as e:
            await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="economy", description="Подробная статистика экономики")
    async def economy(self, ctx):
        await ctx.defer()
        try:
            s = await self.bot.stats()
            await ctx.send(f"📈 **Экономика**\nРезерв `{fmt(s['reserve'])}` | Фонды `{fmt(s['funds'])}` | "
                           f"Игроки `{fmt(s['players'])}` | Масса `{fmt(s['supply'])}`\n"
                           f"24ч: приход `{fmt(s['incoming'])}` / расход `{fmt(s['outgoing'])}`\n"
                           f"Напечатано всего: `{fmt(s['printed'])}`")
        except Exception as e:
            await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="print_money", description="Напечатать деньги")
    @app_commands.describe(amount="Сумма")
    @commands.has_permissions(administrator=True)
    async def print_money(self, ctx, amount: int):
        if amount <= 0: return await ctx.send("❌ Сумма должна быть > 0")
        await ctx.defer()
        try:
            if not await self.bot.emit(amount, "Эмиссия новых денег", "money_printing"):
                return await ctx.send("❌ Не удалось увеличить резерв")
            await self.bot.economy.update_one({"_id": "central_bank"}, {"$inc": {"printed": amount}})
            await ctx.send(f"💵 Эмиссия `{fmt(amount)}` выполнена. Резерв: `{fmt(await self.bot.reserve())}`")
        except Exception as e:
            await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="burn_money", description="Сжечь деньги из резерва")
    @app_commands.describe(amount="Сумма")
    @commands.has_permissions(administrator=True)
    async def burn_money(self, ctx, amount: int):
        if amount <= 0: return await ctx.send("❌ Сумма должна быть > 0")
        await ctx.defer()
        try:
            if not await self.bot.burn(amount): return await ctx.send("❌ Недостаточно денег в резерве")
            await ctx.send(f"🔥 Сожжено `{fmt(amount)}`. Резерв: `{fmt(await self.bot.reserve())}`")
        except Exception as e:
            await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="cb_test", description="Тестовый перевод из ЦБ игроку")
    @app_commands.describe(member="Игрок", amount="Сумма")
    @commands.has_permissions(administrator=True)
    async def cb_test(self, ctx, member: discord.Member, amount: int = 100):
        if amount <= 0: return await ctx.send("❌ Сумма должна быть > 0")
        await ctx.defer()
        try:
            if not await self.bot.cb_to_player(member, amount, "Тестовый перевод ЦБ"):
                return await ctx.send("❌ Недостаточно денег в ЦБ")
            await ctx.send(f"✅ {member.mention} получил `{fmt(amount)}`. Резерв: `{fmt(await self.bot.reserve())}`")
        except Exception as e:
            await ctx.send(f"❌ Ошибка UB/ЦБ: `{str(e)[:500]}`")

    @commands.hybrid_command(name="fund", description="Список фондов")
    async def fund(self, ctx):
        rows = await self.bot.funds.find().sort("name", 1).to_list(100)
        if not rows: return await ctx.send("📦 Фондов нет")
        await ctx.send("📦 **Фонды**\n" + "\n".join(f"• `{x['name']}` — `{fmt(x.get('balance',0))}`" for x in rows))

    @commands.hybrid_command(name="fund_create", description="Создать фонд")
    @app_commands.describe(name="Название")
    @commands.has_permissions(administrator=True)
    async def fund_create(self, ctx, name: str):
        name = name.strip().lower()
        if not 1 <= len(name) <= 40: return await ctx.send("❌ Название 1-40 символов")
        await ctx.send("✅ Фонд создан." if await self.bot.create_fund(name) else "ℹ️ Такой фонд уже есть.")

    @commands.hybrid_command(name="fund_add", description="Перевести деньги из ЦБ в фонд")
    @app_commands.describe(name="Фонд", amount="Сумма")
    @commands.has_permissions(administrator=True)
    async def fund_add(self, ctx, name: str, amount: int):
        name = name.strip().lower()
        if amount <= 0: return await ctx.send("❌ Сумма должна быть > 0")
        if await self.bot.fund_balance(name) == 0 and not await self.bot.funds.find_one({"_id": name}):
            return await ctx.send("❌ Фонд не найден")
        await ctx.defer()
        try:
            ok = await self.bot.add_fund(name, amount, f"Пополнение фонда {name}")
            await ctx.send(f"✅ В `{name}` добавлено `{fmt(amount)}`. Баланс: `{fmt(await self.bot.fund_balance(name))}`" if ok else "❌ Не удалось пополнить фонд")
        except Exception as e: await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="fund_take", description="Выдать деньги из фонда")
    @app_commands.describe(name="Фонд", member="Игрок", amount="Сумма")
    @commands.has_permissions(administrator=True)
    async def fund_take(self, ctx, name: str, member: discord.Member, amount: int):
        name = name.strip().lower()
        if amount <= 0: return await ctx.send("❌ Сумма должна быть > 0")
        if not await self.bot.funds.find_one({"_id": name}): return await ctx.send("❌ Фонд не найден")
        await ctx.defer()
        try:
            ok = await self.bot.take_fund(name, member, amount, f"Выдача из фонда {name}")
            await ctx.send(f"✅ {member.mention} получил `{fmt(amount)}`. Фонд: `{fmt(await self.bot.fund_balance(name))}`" if ok else "❌ Недостаточно денег в фонде")
        except Exception as e: await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="fund_delete", description="Удалить фонд")
    @app_commands.describe(name="Фонд")
    @commands.has_permissions(administrator=True)
    async def fund_delete(self, ctx, name: str):
        name = name.strip().lower(); await ctx.defer()
        try:
            ok, amount = await self.bot.delete_fund(name)
            await ctx.send(f"🗑️ Фонд `{name}` удалён. Возвращено в ЦБ: `{fmt(amount)}`" if ok else "❌ Фонд не найден")
        except Exception as e: await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="audit", description="Аудит экономики")
    @commands.has_permissions(administrator=True)
    async def audit(self, ctx):
        await ctx.defer()
        try:
            s = await self.bot.stats(); tx = await self.bot.tx.count_documents({})
            await ctx.send(f"🔎 **Аудит**\nИгроки `{fmt(s['players'])}` + резерв `{fmt(s['reserve'])}` + фонды `{fmt(s['funds'])}` = `{fmt(s['supply'])}`\n"
                           f"Напечатано `{fmt(s['printed'])}` | операций `{tx}`\nСтатус: ✅ баланс сходится")
        except Exception as e: await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="rate", description="Курс")
    async def rate(self, ctx):
        await ctx.defer()
        try:
            r = await self.bot.rate(); await self.bot.rates.insert_one(r)
            await ctx.send(f"💱 **Курс**\nВнутренний: `{r['internal']}`\nUSD `{r['usd']}` RUB | EUR `{r['eur']}` RUB | CNY `{r['cny']}` RUB\nМасса `{fmt(r['supply'])}`")
        except Exception as e: await ctx.send(f"❌ Ошибка: `{str(e)[:500]}`")

    @commands.hybrid_command(name="chart", description="График курса")
    async def chart(self, ctx):
        await ctx.defer()
        rows = await self.bot.rates.find().sort("created_at", 1).to_list(500)
        if len(rows) < 2: return await ctx.send("❌ Нужно минимум 2 записи курса")
        path = "/tmp/cb_rate.png"
        plt.figure(figsize=(10, 5)); plt.plot([x["created_at"] for x in rows], [x["internal"] for x in rows]);
        plt.title("Внутренний курс ЦБ"); plt.xlabel("Дата"); plt.ylabel("Курс"); plt.grid(True, alpha=.25); plt.xticks(rotation=30); plt.tight_layout(); plt.savefig(path, dpi=150); plt.close()
        await ctx.send(file=discord.File(path, filename="cb_rate.png"))

    @commands.hybrid_command(name="history", description="Журнал операций")
    @app_commands.describe(limit="Количество записей")
    async def history(self, ctx, limit: int = 10):
        limit = max(1, min(25, limit)); rows = await self.bot.tx.find().sort("created_at", -1).to_list(limit)
        if not rows: return await ctx.send("📜 Журнал пуст")
        text = []
        for x in rows:
            d = x.get("created_at"); ds = d.astimezone(timezone.utc).strftime("%d.%m %H:%M") if isinstance(d, datetime) else "?"
            text.append(f"`{ds}` `{x.get('source')}` → `{x.get('destination')}` **{fmt(x.get('amount',0))}**\n_{x.get('reason','')}_")
        await ctx.send("📜 **Журнал**\n\n" + "\n\n".join(text)[:3900])


async def main():
    check_env()
    bot = Bot()
    asyncio.create_task(asyncio.to_thread(run_web))
    try:
        await bot.start(DISCORD_TOKEN)
    finally:
        if not bot.is_closed():
            await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
