# =========================================================
# MOMENTUM MOON SCANNER IDX - SCALPING MODE (NO MARKET CAP)
# Focus: Rame + Anti Gorengan + T+1 Continuation
# =========================================================

import requests
import pandas as pd
import numpy as np
import yfinance as yf
import multiprocessing as mp
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
import holidays
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from concurrent.futures import ThreadPoolExecutor

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
# =========================================================
# CONFIG
# =========================================================
MIN_AVG_VOLUME = 5_000_000          # wajib rame
MIN_VALUE_TRADED = 20_000_000_000   # minimal 20M nilai transaksi
VOLUME_SPIKE_MULTIPLIER = 1.5
PERIOD = "6mo"
MAX_DAILY_ATR_PERCENT = 12
# =========================================================
# TELEGRAM CONFIG
# =========================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# =========================================================
# LOAD EMITEN
# =========================================================
emiten = pd.read_csv("emiten2.csv")
emiten["code"] = emiten["code"].astype(str).str.strip()

def init_db():
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            signal_date DATE,
            code VARCHAR(10),
            entry_price NUMERIC,
            score INTEGER,
            category VARCHAR(10),
            mode VARCHAR(20),
            volume BIGINT,
            value_traded BIGINT,
            atr_percent NUMERIC,
            result VARCHAR(10),
            return_pct NUMERIC,
            evaluated BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        conn.commit()
# =========================================================
# SCORING FUNCTION
# =========================================================
def calculate_moon_score(df):

    if len(df) < 40:
        return 0

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.dropna().copy()

    df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    df["EMA5"] = EMAIndicator(df["Close"], 5).ema_indicator()
    df["EMA20"] = EMAIndicator(df["Close"], 20).ema_indicator()

    df = df.dropna()

    latest = df.iloc[-1]
    score = 0

    high = latest["High"]
    low = latest["Low"]
    close = latest["Close"]

    # 1️⃣ Close near high (buyer dominance)
    if (high - low) > 0:
        close_pos = (high - close) / (high - low)
        if close_pos < 0.35:
            score += 20

    # 2️⃣ Volume spike
    avg_vol = df["Volume"].rolling(20).mean().iloc[-2]
    if avg_vol > 0 and latest["Volume"] > avg_vol * VOLUME_SPIKE_MULTIPLIER:
        score += 20

    # 3️⃣ Range expansion normal (bukan liar)
    range_today = high - low
    avg_range_20 = (df["High"] - df["Low"]).rolling(20).mean().iloc[-2]

    if avg_range_20 > 0 and range_today > avg_range_20 * 1.3:
        score += 15

    # 4️⃣ RSI momentum
    if 55 < latest["RSI"] < 80:
        score += 15

    # 5️⃣ EMA short trend
    if latest["EMA5"] > latest["EMA20"]:
        score += 15

    # 6️⃣ Liquidity bonus
    if avg_vol > MIN_AVG_VOLUME:
        score += 15

    return score


# =========================================================
# WORKER FUNCTION
# =========================================================
def scan_stock(row):

    code = row["code"]
    ticker = code + ".JK"

    try:
        df = yf.download(
            ticker,
            period=PERIOD,
            interval="1d",
            auto_adjust=False,
            progress=False
        )

        if df.empty or len(df) < 40:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        score = calculate_moon_score(df)

        avg_vol_20 = df["Volume"].rolling(20).mean().iloc[-2]
        if avg_vol_20 < MIN_AVG_VOLUME:
            return None

        if score < 30:
            return None

        # =============================
        # ANTI GORENGAN FILTER
        # =============================

        # 1️⃣ ATR filter
        atr_indicator = AverageTrueRange(df["High"], df["Low"], df["Close"], 14)
        df["ATR"] = atr_indicator.average_true_range()
        latest_atr = df["ATR"].iloc[-1]
        latest_close = df["Close"].iloc[-1]
        atr_percent = (latest_atr / latest_close) * 100

        if atr_percent > MAX_DAILY_ATR_PERCENT:
            return None

        # 2️⃣ Range abnormal (buang candle liar)
        latest_range = df["High"].iloc[-1] - df["Low"].iloc[-1]
        avg_range_20 = (df["High"] - df["Low"]).rolling(20).mean().iloc[-2]

        if latest_range > avg_range_20 * 3:
            return None

        # 3️⃣ Nilai transaksi (wajib rame institusi)
        value_traded = latest_close * df["Volume"].iloc[-1]
        if value_traded < MIN_VALUE_TRADED:
            return None

        # =============================
        # DAILY CHANGE
        # =============================
        change_pct = round(
            ((df["Close"].iloc[-1] - df["Close"].iloc[-2])
            / df["Close"].iloc[-2]) * 100, 2
        )

        if change_pct > 15:
            return None

        explosive = (
            change_pct >= 12 and
            df["Volume"].iloc[-1] > avg_vol_20 * 1.8
        )

        continuation = (
            5 <= change_pct < 12 and
            df["Volume"].iloc[-1] > avg_vol_20 * 1.3
        )

        return {
            "Code": code,
            "Moon Score": score,
            "Last Price": float(latest_close),
            "Change (%)": change_pct,
            "Value_raw": int(value_traded),
            "Volume_raw": int(df["Volume"].iloc[-1]),
            "ATR_percent": float(round(atr_percent, 2)),
            "Explosive": explosive,
            "Continuation": continuation
        }

    except Exception:
        return None

def format_number(num):
    if num >= 1_000_000_000_000:
        return f"{num/1_000_000_000_000:.2f}T"
    elif num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num/1_000:.2f}K"
    else:
        return str(num)

#FUNCTION SEND TELEGRAM
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, data=payload, timeout=10)
    except:
        pass

def is_market_open():
    tz = pytz.timezone("Asia/Jakarta")
    today = datetime.now(tz).date()

    # Weekend
    if today.weekday() >= 5:
        return False

    # Libur nasional Indonesia
    id_holidays = holidays.Indonesia()
    if today in id_holidays:
        return False

    return True

def evaluate_signals():

    print("📊 Evaluating signals...")

    with engine.connect() as conn:

        rows = conn.execute(text("""
            SELECT * FROM signals
            WHERE evaluated = FALSE
        """)).fetchall()

        for row in rows:

            ticker = row.code + ".JK"

            df = yf.download(
                ticker,
                period="5d",
                interval="1d",
                progress=False
            )

            if len(df) < 2:
                continue

            latest = df.iloc[-1]
            high = latest["High"]
            low = latest["Low"]

            entry = float(row.entry_price)

            tp_price = entry * 1.03
            sl_price = entry * 0.97

            result = "FLAT"
            return_pct = 0

            if high >= tp_price:
                result = "WIN"
                return_pct = 3
            elif low <= sl_price:
                result = "LOSS"
                return_pct = -3

            conn.execute(text("""
                UPDATE signals
                SET result = :result,
                    return_pct = :return_pct,
                    evaluated = TRUE
                WHERE id = :id
            """), {
                "result": result,
                "return_pct": return_pct,
                "id": row.id
            })

        conn.commit()

async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    with engine.connect() as conn:
        stats = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as win,
                SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as loss,
                AVG(return_pct) as avg_return,
                MAX(return_pct) as best,
                MIN(return_pct) as worst
            FROM signals
            WHERE evaluated = TRUE
        """)).fetchone()

    if stats.total == 0:
        message = "Belum ada statistik."
    else:
        winrate = round((stats.win / stats.total) * 100, 2)

        message = f"""
📊 MOMENTUM STATISTICS

Total Signal : {stats.total}
Win          : {stats.win}
Loss         : {stats.loss}
Winrate      : {winrate}%

Avg Return   : {round(stats.avg_return,2)}%
Best         : {stats.best}%
Worst        : {stats.worst}%
"""

    # Support command & button
    if update.message:
        await update.message.reply_text(message)
    else:
        await update.callback_query.edit_message_text(message)
# =========================================================
# MULTIPROCESS EXECUTION + SCHEDULER (RAILWAY READY)
# =========================================================

def run_eod_scan():

    global scheduler_active

    if not scheduler_active:
        print("⏸ Scheduler paused.")
        return

    if not is_market_open():
        print("📴 Market libur.")
        return

    print("🚀 Running EOD Scan...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(scan_stock, emiten.to_dict("records")))

    results = [r for r in results if r]

    if not results:
        send_telegram("📉 Tidak ada saham memenuhi kriteria hari ini.")
        return

    df = pd.DataFrame(results)

    # Categorize
    def categorize(row):
        if row["Explosive"]:
            return "🚀"
        elif row["Continuation"]:
            return "🔥"
        return "⚡"

    df["Category"] = df.apply(categorize, axis=1)

    df = df.sort_values("Moon Score", ascending=False).head(5).reset_index(drop=True)

    # ================= SAVE TO DB =================
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO signals (
                    signal_date, code, entry_price, score,
                    category, mode, volume, value_traded,
                    atr_percent
                ) VALUES (
                    CURRENT_DATE, :code, :entry_price, :score,
                    :category, :mode, :volume, :value_traded,
                    :atr_percent
                )
            """), {
                "code": row["Code"],
                "entry_price": row["Last Price"],
                "score": row["Moon Score"],
                "category": row["Category"],
                "mode": "normal",
                "volume": row["Volume_raw"],
                "value_traded": row["Value_raw"],
                "atr_percent": row["ATR_percent"]
            })
        conn.commit()

    # ================= TELEGRAM FORMAT =================
    html = "<b>🚀 EOD SCALPING MOMENTUM</b>\n\n<pre>"
    html += "CODE | PRICE | CHG% | VALUE | VOL | SCR | CTG\n"
    html += "-" * 55 + "\n"

    for _, row in df.iterrows():
        html += (
            f"{row['Code']:<4} | "
            f"{int(row['Last Price']):>6} | "
            f"{row['Change (%)']:>5.2f}% | "
            f"{format_number(row['Value_raw']):>6} | "
            f"{format_number(row['Volume_raw']):>6} | "
            f"{row['Moon Score']:>3} | "
            f"{row['Category']}\n"
        )

    html += "</pre>\n"
    html += "<b>Legend:</b>\n"
    html += "🚀 = Strong Momentum\n"
    html += "🔥 = Continuation T+1\n"
    html += "⚡ = Early Momentum\n"
    
    send_telegram(html)



    tz = pytz.timezone("Asia/Jakarta")

    print("⏰ Scheduler aktif. Menunggu jam 15:05 WIB...")

    background_scheduler.add_job(
        run_eod_scan,
        trigger='cron',
        day_of_week='mon-fri',
        hour=15,
        minute=5
    )
    
    background_scheduler.add_job(
        evaluate_signals,
        trigger='cron',
        hour=18,
        minute=0
    )

# =============================
# DASHBOARD UI
# =============================
def dashboard_keyboard():
    keyboard = [
        [InlineKeyboardButton("🔎 Scan Now", callback_data="scan_now")],
        [InlineKeyboardButton("📊 Last Result", callback_data="last_result")],
        [InlineKeyboardButton("📈 Aggressive Mode", callback_data="aggressive")],
        [InlineKeyboardButton("📈 Statistik", callback_data="stats")],
        [
            InlineKeyboardButton("⏸ Pause Scheduler", callback_data="pause"),
            InlineKeyboardButton("▶ Resume Scheduler", callback_data="resume"),
        ],
        [InlineKeyboardButton("⚙ Status", callback_data="status")],
    ]
    return InlineKeyboardMarkup(keyboard)

last_result_message = "Belum ada hasil scan."
scheduler_active = True
# =============================
# COMMAND HANDLER
# =============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 MOMENTUM MOON DASHBOARD",
        reply_markup=dashboard_keyboard()
    )


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔎 Manual scan dimulai...")
    await run_scan_async(update)


async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scheduler_active
    scheduler_active = False
    await update.message.reply_text("⏸ Scheduler paused.")


async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scheduler_active
    scheduler_active = True
    await update.message.reply_text("▶ Scheduler resumed.")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = "🟢 Active" if scheduler_active else "🔴 Paused"
    await update.message.reply_text(f"Scheduler Status: {status}")


# =============================
# CALLBACK HANDLER
# =============================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "scan_now":
        await query.edit_message_text("🔎 Manual scan dimulai...")
        await run_scan_async(query)

    elif query.data == "last_result":
        await query.edit_message_text(last_result_message)

    elif query.data == "pause":
        global scheduler_active
        scheduler_active = False
        await query.edit_message_text("⏸ Scheduler paused.")

    elif query.data == "resume":
        scheduler_active = True
        await query.edit_message_text("▶ Scheduler resumed.")
        
    elif query.data == "stats":
        await stats_handler(update, context)
        
    elif query.data == "status":
        status = "🟢 Active" if scheduler_active else "🔴 Paused"
        await query.edit_message_text(f"Scheduler Status: {status}")

    elif query.data == "aggressive":
        await query.edit_message_text("📈 Aggressive mode scan dimulai...")
        await run_scan_async(query, aggressive=True)


# =============================
# ASYNC SCAN WRAPPER
# =============================
async def run_scan_async(target, aggressive=False):

    global last_result_message

    loop = context = None

    MAX_WORKERS = 4

    with mp.Pool(MAX_WORKERS) as pool:
        results = pool.map(scan_stock, emiten.to_dict("records"))

    results = [r for r in results if r]

    if len(results) == 0:
        message = "📉 Tidak ada saham memenuhi kriteria."
    else:
        df = pd.DataFrame(results)
        df = df.sort_values(by="Moon Score", ascending=False).head(5)
        
def categorize(row):
    if row.get("Explosive"):
        return "🚀"
    elif row.get("Continuation"):
        return "🔥"
    return "⚡"

df["Category"] = df.apply(categorize, axis=1)
    html = "<b>🚀MANUAL SCALPING MOMENTUM</b>\n\n<pre>"
    html += "CODE | PRICE | CHG% | VALUE | VOL | SCR | CTG\n"
    html += "-" * 55 + "\n"

    for _, row in df.iterrows():
        html += (
            f"{row['Code']:<4} | "
            f"{int(row['Last Price']):>6} | "
            f"{row['Change (%)']:>5.2f}% | "
            f"{format_number(row['Value_raw']):>6} | "
            f"{format_number(row['Volume_raw']):>6} | "
            f"{row['Moon Score']:>3} | "
            f"{row['Category']}\n"
        )

    html += "</pre>\n"
    html += "<b>Legend:</b>\n"
    html += "🚀 = Strong Momentum\n"
    html += "🔥 = Continuation T+1\n"
    html += "⚡ = Early Momentum\n"
    


    last_result_message = message

    if hasattr(target, "message"):
        await target.message.reply_text(message, parse_mode="HTML")
    else:
        await target.edit_message_text(message, parse_mode="HTML")


# =========================================================
# MAIN BOT START
# =========================================================
if __name__ == "__main__":

    tz = pytz.timezone("Asia/Jakarta")

    background_scheduler = BackgroundScheduler(timezone=tz)
    background_scheduler.start()

    # EOD Scan 15:05 WIB
    background_scheduler.add_job(
        run_eod_scan,
        trigger='cron',
        day_of_week='mon-fri',
        hour=15,
        minute=5
    )

    # Evaluasi 18:00 WIB
    background_scheduler.add_job(
        evaluate_signals,
        trigger='cron',
        day_of_week='mon-fri',
        hour=18,
        minute=0
    )

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("pause", pause_command))
    app.add_handler(CommandHandler("resume", resume_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    init_db()

    print("🤖 Dashboard bot running...")
    app.run_polling()
