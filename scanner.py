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
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pytz
import holidays
import os

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
            "Value Traded": int(value_traded),
            "Volume": int(df["Volume"].iloc[-1]),
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
# =========================================================
# MULTIPROCESS EXECUTION + SCHEDULER (RAILWAY READY)
# =========================================================

def run_eod_scan():

    if not is_market_open():
        print("📴 Market libur. Skip scanning.")
        return

    print("🚀 Running EOD SCALPING Scan 15:05 WIB...")

    MAX_WORKERS = 4

    with mp.Pool(MAX_WORKERS) as pool:
        results = pool.map(scan_stock, emiten.to_dict("records"))

    results = [r for r in results if r]

    if len(results) == 0:
        print("❌ Tidak ada saham memenuhi kriteria.")
        send_telegram("📉 Tidak ada saham memenuhi kriteria hari ini.")
        return

    result_df = pd.DataFrame(results)

    def categorize(row):
        if row["Explosive"]:
            return "🚀"
        elif row["Continuation"]:
            return "🔥"
        else:
            return "⚡"

    result_df["Category"] = result_df.apply(categorize, axis=1)

    result_df = result_df.sort_values(
        by="Moon Score",
        ascending=False
    ).head(5).reset_index(drop=True)

    result_df.insert(0, "Rank", range(1, len(result_df) + 1))

    # Format angka
    result_df["Value Traded"] = result_df["Value Traded"].apply(format_number)
    result_df["Volume"] = result_df["Volume"].apply(format_number)

    print("\n===== TOP 5 SCALPING MOMENTUM =====\n")
    print(result_df)

    # =============================
    # TELEGRAM ALERT
    # =============================

    html_message = "<b>🚀 EOD SCALPING MOMENTUM (15:05 WIB)</b>\n\n"
    html_message += "<pre>"
    html_message += "CODE | PRICE | CHG%  | VALUE | VOLUME | SCR | CTG\n"
    html_message += "-" * 49 + "\n"

    for _, row in result_df.iterrows():
        html_message += (
            f"{row['Code']:<4} | "
            f"{int(row['Last Price']):>5} | "
            f"{row['Change (%)']:>5.2f}% | "
            f"{row['Value Traded']:>6} | "
            f"{row['Volume']:>6} | "
            f"{row['Moon Score']:>3} | "
            f"{row['Category']}\n"
        )

    html_message += "</pre>\n"
    html_message += "<b>Legend:</b>\n"
    html_message += "🚀 = Strong Momentum\n"
    html_message += "🔥 = Continuation T+1\n"
    html_message += "⚡ = Early Momentum\n"

    send_telegram(html_message)


if __name__ == "__main__":

    tz = pytz.timezone("Asia/Jakarta")

    scheduler = BlockingScheduler(timezone=tz)

    # Jalan setiap weekday jam 15:05 WIB
    scheduler.add_job(
        run_eod_scan,
        trigger='cron',
        day_of_week='mon-fri',
        hour=15,
        minute=5
    )

    print("⏰ Scheduler aktif. Menunggu jam 15:05 WIB...")
    scheduler.start()
