import os
import io
import json
import re
import threading
from datetime import datetime

import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flask import Flask
import discord
from discord.ext import commands
from unbelievaboat import Client as UBClient


# =========================================================
# НАСТРОЙКИ
# =========================================================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
UB_TOKEN = os.getenv("UB_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

PORT = int(os.getenv("PORT", "10000"))


# =========================================================
# FLASK — НУЖЕН RENDER
# =========================================================

app = Flask(__name__)


@app.route("/")
def home():
    return "Discord bot is running!"


@app.route("/health")
def health():
    return "OK"


def run_flask():
    app.run(
        host="0.0.0.0",
        port=PORT
    )


# =========================================================
# ФАЙЛЫ
# =========================================================

DATA_FILE = "economy_data.json"
RATE_FILE = "rate_history.json"


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2
        )


def load_economy():
    return load_json(
        DATA_FILE,
        {
            "printed": 0,
            "history": []
        }
    )


def save_economy(data):
    save_json(DATA_FILE, data)


def load_rates():
    return load_json(
        RATE_FILE,
        {
            "dates": [],
            "rates": []
        }
    )


def save_rates(data):
    save_json(RATE_FILE, data)


# =========================================================
# DISCORD
# =========================================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)


# =========================================================
# UNBELIEVABOAT
# =========================================================

ub = None


# =========================================================
# ФУНКЦИИ
# =========================================================

def get_usd_rate():
    """Курс доллара к рублю с сайта ЦБ РФ."""

    try:
        r = requests.get(
            "https://www.cbr.ru/scripts/XML_daily.asp",
            timeout=10
        )

        match = re.search(
            r'<Valute ID="R01235">.*?<Value>(.*?)</Value>',
            r.text,
            re.DOTALL
        )

        if match:
            return float(
                match.group(1).replace(",", ".")
            )

    except Exception as e:
        print(f"Ошибка получения курса USD: {e}")

    return 90.0


async def get_total_balance():
    """Общий баланс всех игроков сервера."""

    if ub is None:
        return 0

    leaderboard = await ub.get_guild_leaderboard(
        str(GUILD_ID),
        limit=1000
    )

    return sum(
        user.get("total", 0)
        for user in leaderboard
    )


def calculate_rate(total_balance):
    """Формула курса валюты."""

    if total_balance <= 0:
        return 0

    usd = get_usd_rate()

    return (
        1_000_000 / total_balance
    ) * usd * 0.01


# =========================================================
# ON READY
# =========================================================

@bot.event
async def on_ready():
    global ub

    if ub is None:
        ub = UBClient(UB_TOKEN)

    print("--------------------------------")
    print(f"Бот запущен: {bot.user}")
    print(f"Guild ID: {GUILD_ID}")
    print("UnbelievaBoat подключён")
    print("--------------------------------")


# =========================================================
# PRINT MONEY
# =========================================================

@bot.command(name="print_money")
@commands.has_permissions(administrator=True)
async def print_money(ctx, amount: int):

    if amount <= 0:
        await ctx.send(
            "❌ Сумма должна быть положительной."
        )
        return

    total = await get_total_balance()

    data = load_economy()

    data["printed"] += amount

    data["history"].append({
        "amount": amount,
        "total_after": total
    })

    save_economy(data)

    await ctx.send(
        f"💰 Напечатано: **{amount}** монет.\n"
        f"📊 Общий баланс игроков: **{total}**\n"
        f"🖨️ Всего напечатано: **{data['printed']}**"
    )


# =========================================================
# ECONOMY STATS
# =========================================================

@bot.command(name="economy_stats")
async def economy_stats(ctx):

    total = await get_total_balance()

    data = load_economy()

    embed = discord.Embed(
        title="📊 Статистика экономики",
        color=0x00BFFF
    )

    embed.add_field(
        name="Баланс игроков",
        value=f"{total} монет",
        inline=False
    )

    embed.add_field(
        name="Напечатано",
        value=f"{data['printed']} монет",
        inline=False
    )

    await ctx.send(embed=embed)


# =========================================================
# RATE
# =========================================================

@bot.command(name="rate")
async def rate(ctx):

    total = await get_total_balance()

    usd = get_usd_rate()

    coin_rate = calculate_rate(total)

    embed = discord.Embed(
        title="📈 Курс валюты",
        color=0x00FF00
    )

    embed.add_field(
        name="1 монета",
        value=f"≈ {coin_rate:.4f} ₽",
        inline=True
    )

    embed.add_field(
        name="USD/RUB (ЦБ)",
        value=f"{usd:.2f} ₽",
        inline=False
    )

    embed.set_footer(
        text="Курс зависит от общего баланса игроков"
    )

    await ctx.send(embed=embed)

    # Сохраняем историю

    history = load_rates()

    history["dates"].append(
        datetime.now().strftime("%d.%m")
    )

    history["rates"].append(
        round(coin_rate, 4)
    )

    history["dates"] = history["dates"][-30:]
    history["rates"] = history["rates"][-30:]

    save_rates(history)


# =========================================================
# CHART
# =========================================================

@bot.command(name="chart")
async def chart(ctx):

    history = load_rates()

    if len(history["dates"]) < 2:

        await ctx.send(
            "📉 Мало данных для графика.\n"
            "Повтори команду `!rate` несколько раз."
        )

        return

    plt.figure(figsize=(8, 5))

    plt.plot(
        history["dates"],
        history["rates"],
        marker="o",
        color="#00BFFF"
    )

    plt.title(
        "Курс валюты сервера"
    )

    plt.xlabel("Дата")

    plt.ylabel(
        "Курс (₽)"
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.xticks(
        rotation=45
    )

    buf = io.BytesIO()

    plt.savefig(
        buf,
        format="png",
        bbox_inches="tight",
        dpi=80
    )

    buf.seek(0)

    plt.close()

    await ctx.send(
        file=discord.File(
            buf,
            filename="chart.png"
        )
    )


# =========================================================
# TOP
# =========================================================

@bot.command(name="top")
async def top(ctx, limit: int = 10):

    if limit < 1:
        limit = 1

    if limit > 25:
        limit = 25

    leaderboard = await ub.get_guild_leaderboard(
        str(GUILD_ID),
        limit=limit
    )

    lines = []

    for i, user in enumerate(
        leaderboard,
        1
    ):

        lines.append(
            f"**{i}.** "
            f"{user.get('username', 'Unknown')} "
            f"— `{user.get('total', 0)}`"
        )

    if not lines:
        lines.append(
            "Нет данных."
        )

    embed = discord.Embed(
        title=f"🏆 Топ-{limit}",
        description="\n".join(lines),
        color=0xFFD700
    )

    await ctx.send(embed=embed)


# =========================================================
# ОШИБКИ КОМАНД
# =========================================================

@bot.event
async def on_command_error(ctx, error):

    if isinstance(
        error,
        commands.MissingPermissions
    ):

        await ctx.send(
            "❌ У тебя нет прав администратора."
        )

        return

    if isinstance(
        error,
        commands.MissingRequiredArgument
    ):

        await ctx.send(
            "❌ Не хватает аргумента."
        )

        return

    if isinstance(
        error,
        commands.BadArgument
    ):

        await ctx.send(
            "❌ Неверный аргумент."
        )

        return

    print(
        f"Ошибка команды: {error}"
    )


# =========================================================
# ЗАПУСК
# =========================================================

if __name__ == "__main__":

    # Запускаем Flask в отдельном потоке
    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    print(
        f"Flask запущен на порту {PORT}"
    )

    # Запускаем Discord
    bot.run(DISCORD_TOKEN)