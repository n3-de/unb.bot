import os
import io
import json
import re
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import discord
from discord.ext import commands
from unbelievaboat import Client as UBClient

# ===== НАСТРОЙКИ =====
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
UB_TOKEN = os.getenv("UB_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))

# ===== ФАЙЛЫ ДЛЯ ДАННЫХ =====
DATA_FILE = "economy_data.json"
RATE_FILE = "rate_history.json"

# ===== DISCORD =====
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ===== UNBELIEVABOAT =====
ub = UBClient(UB_TOKEN)

# ===== РАБОТА С ФАЙЛАМИ =====
def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def load_economy():
    return load_json(DATA_FILE, {"printed": 0, "history": []})

def save_economy(data):
    save_json(DATA_FILE, data)

def load_rates():
    return load_json(RATE_FILE, {"dates": [], "rates": []})

def save_rates(data):
    save_json(RATE_FILE, data)

# ===== ФУНКЦИИ =====
def get_usd_rate():
    """Курс доллара к рублю с сайта ЦБ РФ."""
    try:
        r = requests.get("https://www.cbr.ru/scripts/XML_daily.asp", timeout=10)
        match = re.search(r'<Valute ID="R01235">.*?<Value>(.*?)</Value>', r.text, re.DOTALL)
        if match:
            return float(match.group(1).replace(",", "."))
    except Exception:
        pass
    return 90.0

async def get_total_balance():
    """Общий баланс всех игроков сервера."""
    leaderboard = await ub.get_guild_leaderboard(str(GUILD_ID), limit=1000)
    return sum(user.get("total", 0) for user in leaderboard)

def calculate_rate(total_balance):
    """Формула курса. Меняй под себя."""
    if total_balance <= 0:
        return 0
    usd = get_usd_rate()
    # Пример: чем больше денег в экономике, тем дешевле монета
    return (1_000_000 / total_balance) * usd * 0.01

# ===== КОМАНДЫ =====
@bot.event
async def on_ready():
    print(f"Бот {bot.user} запущен.")

@bot.command(name="print_money")
@commands.has_permissions(administrator=True)
async def print_money(ctx, amount: int):
    """Печатает деньги в экономику."""
    if amount <= 0:
        await ctx.send("❌ Сумма должна быть положительной.")
        return

    total = await get_total_balance()
    data = load_economy()
    data["printed"] += amount
    data["history"].append({"amount": amount, "total_after": total})
    save_economy(data)

    await ctx.send(
        f"💰 Напечатано: **{amount}** монет.\n"
        f"📊 Общий баланс игроков: **{total}**\n"
        f"🖨️ Всего напечатано: **{data['printed']}**"
    )

@bot.command(name="economy_stats")
async def economy_stats(ctx):
    """Статистика экономики."""
    total = await get_total_balance()
    data = load_economy()

    embed = discord.Embed(title="📊 Статистика экономики", color=0x00BFFF)
    embed.add_field(name="Баланс игроков", value=f"{total} монет", inline=False)
    embed.add_field(name="Напечатано", value=f"{data['printed']} монет", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="rate")
async def rate(ctx):
    """Текущий курс валюты сервера."""
    total = await get_total_balance()
    usd = get_usd_rate()
    coin_rate = calculate_rate(total)

    embed = discord.Embed(title="📈 Курс валюты", color=0x00FF00)
    embed.add_field(name="1 монета", value=f"≈ {coin_rate:.4f} ₽", inline=True)
    embed.add_field(name="USD/RUB (ЦБ)", value=f"{usd:.2f} ₽", inline=False)
    embed.set_footer(text="Курс зависит от общего баланса игроков")
    await ctx.send(embed=embed)

    # Сохраняем точку для графика
    history = load_rates()
    from datetime import datetime
    history["dates"].append(datetime.now().strftime("%d.%m"))
    history["rates"].append(round(coin_rate, 4))
    history["dates"] = history["dates"][-30:]
    history["rates"] = history["rates"][-30:]
    save_rates(history)

@bot.command(name="chart")
async def chart(ctx):
    """График курса за последние точки."""
    history = load_rates()
    if len(history["dates"]) < 2:
        await ctx.send("📉 Мало данных для графика. Повтори команду `!rate` несколько раз.")
        return

    plt.figure(figsize=(8, 5))
    plt.plot(history["dates"], history["rates"], marker="o", color="#00BFFF")
    plt.title("Курс валюты сервера")
    plt.xlabel("Дата")
    plt.ylabel("Курс (₽)")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", dpi=80)
    buf.seek(0)
    plt.close()

    await ctx.send(file=discord.File(buf, filename="chart.png"))

@bot.command(name="top")
async def top(ctx, limit: int = 10):
    """Топ игроков по балансу."""
    leaderboard = await ub.get_guild_leaderboard(str(GUILD_ID), limit=limit)
    lines = [f"**{i}.** {u.get('username', 'Unknown')} — `{u.get('total', 0)}`" for i, u in enumerate(leaderboard, 1)]
    embed = discord.Embed(title=f"🏆 Топ-{limit}", description="\n".join(lines), color=0xFFD700)
    await ctx.send(embed=embed)

# ===== ЗАПУСК =====
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
