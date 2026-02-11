# =========================================================
# 🔥 HENKY SMART MONEY TELEGRAM BOT (FULL AUTO)
# Daily + Intraday + Command Telegram
# =========================================================

import pandas as pd
import numpy as np
import yfinance as yf
import datetime, pickle, os, time, requests
import matplotlib.pyplot as plt


# =========================================================
# 🔥 TELEGRAM CONFIG
# =========================================================
TOKEN = "8552451246:AAGIJffSNsD9wNIhpHWGb2LkRKGsyGc1s64"
CHAT_ID = "1115652607"

STOCK_FILE = "emiten2.csv"


# =========================================================
# TELEGRAM
# =========================================================
def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
    )


def send_image(path):
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
        data={"chat_id": CHAT_ID},
        files={"photo": open(path, "rb")}
    )


def get_updates(offset=None):
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    r = requests.get(url, params={"timeout": 30, "offset": offset})
    return r.json()


# =========================================================
# MODE SETTINGS
# =========================================================
def get_settings(mode):

    if mode == "intraday":
        return {
            "LOOKBACK": "7d",
            "INTERVAL": "15m",
            "VALUE_MIN": 50_000_000,
            "MA_FAST": 5,
            "MA_SLOW": 10,
            "ATR_SL": 0.8,
            "ATR_TP": 2.0,
            "CACHE": "cache_15m.pkl"
        }

    return {
        "LOOKBACK": "6mo",
        "INTERVAL": "1d",
        "VALUE_MIN": 300_000_000,
        "MA_FAST": 10,
        "MA_SLOW": 20,
        "ATR_SL": 1.0,
        "ATR_TP": 3.0,
        "CACHE": "cache_daily.pkl"
    }


# =========================================================
# LOAD DATA + CACHE
# =========================================================
def load_data(tickers, s):

    today = datetime.date.today()

    if os.path.exists(s["CACHE"]):
        file_date = datetime.date.fromtimestamp(os.path.getmtime(s["CACHE"]))
        if file_date == today:
            print("⚡ cache used")
            return pickle.load(open(s["CACHE"], "rb"))

    print("🌐 downloading...")

    data = yf.download(
        tickers,
        period=s["LOOKBACK"],
        interval=s["INTERVAL"],
        group_by="ticker",
        auto_adjust=True,
        threads=True
    )

    pickle.dump(data, open(s["CACHE"], "wb"))
    return data


# =========================================================
# INDICATORS
# =========================================================
def add_indicators(df, s):

    df["VOL_AVG"] = df["Volume"].rolling(20).mean()
    df["RVOL"] = df["Volume"] / df["VOL_AVG"]
    df["VALUE"] = df["Close"] * df["Volume"]

    df["MAF"] = df["Close"].rolling(s["MA_FAST"]).mean()
    df["MAS"] = df["Close"].rolling(s["MA_SLOW"]).mean()

    df["HIGH20"] = df["High"].rolling(20).max()

    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100/(1+rs))

    df["OBV"] = (np.sign(df["Close"].diff()) * df["Volume"]).fillna(0).cumsum()

    tr = np.maximum(
        df["High"]-df["Low"],
        np.maximum(abs(df["High"]-df["Close"].shift()),
                   abs(df["Low"]-df["Close"].shift()))
    )
    df["ATR"] = tr.rolling(14).mean()

    return df


# =========================================================
# SCAN
# =========================================================
def scan_one(code, data, s):

    ticker = f"{code}.JK"

    try:
        df = data[ticker].dropna().copy()
    except:
        return None

    if len(df) < 60:
        return None

    df = add_indicators(df, s)

    last = df.iloc[-1]
    prev5 = df.iloc[-5]

    score = sum([
        last["VALUE"] > s["VALUE_MIN"],
        last["RVOL"] > 1.2,
        last["Close"] > last["MAF"] > last["MAS"],
        last["Close"] >= last["HIGH20"]*0.97,
        55 < last["RSI"] < 75,
        last["OBV"] > prev5["OBV"]
    ])

    if score < 4:
        return None

    entry = last["Close"]
    sl = entry - s["ATR_SL"]*last["ATR"]
    tp = entry + s["ATR_TP"]*last["ATR"]

    return [code, entry, last["RSI"], tp, sl, score]


# =========================================================
# IMAGE
# =========================================================
def make_image(df):

    fig, ax = plt.subplots(figsize=(8, len(df)*0.55))
    ax.axis("off")

    table = ax.table(cellText=df.values,
                     colLabels=df.columns,
                     loc="center")

    table.auto_set_font_size(False)
    table.set_fontsize(9)

    path = "scan.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()

    return path


# =========================================================
# MAIN SCANNER
# =========================================================
def run_scan(mode):

    print(f"🔍 scanning {mode}")

    s = get_settings(mode)

    stocks = pd.read_csv(STOCK_FILE)
    tickers = [f"{c}.JK" for c in stocks["code"]]

    data = load_data(tickers, s)

    results = []

    for code in stocks["code"]:
        r = scan_one(code, data, s)
        if r:
            results.append(r)

    if not results:
        send_telegram(f"❌ Tidak ada kandidat {mode}")
        return

    res = pd.DataFrame(results,
                       columns=["Code","Entry","RSI","TP","SL","Score"])

    res = res.sort_values("Score", ascending=False)

    emoji = ["🥇","🥈","🥉","🔥","⭐","✨"]
    res["Rank"] = [emoji[i] if i < len(emoji) else "•" for i in range(len(res))]

    res = res[["Rank","Code","Entry","RSI","TP","SL","Score"]]

    now = datetime.datetime.utcnow() + datetime.timedelta(hours=7)

    msg = f"<b>🚀 {mode.upper()} SMART MONEY</b>\n"
    msg += f"🕒 {now.strftime('%d %b %H:%M')} WIB\n\n"

    table = ""
    for r in res.head(5).itertuples():
        table += f"{r.Rank} {r.Code}  {int(r.Entry)}  RSI:{int(r.RSI)}  TP:{int(r.TP)}  SL:{int(r.SL)}\n"

    msg += f"<pre>{table}</pre>"

    send_telegram(msg)

    img = make_image(res)
    send_image(img)


# =========================================================
# TELEGRAM BOT LOOP
# =========================================================
def listener():

    print("🤖 Bot aktif... ketik daily / intraday di Telegram")

    offset = None

    while True:

        updates = get_updates(offset)

        for u in updates["result"]:

            offset = u["update_id"] + 1

            try:
                text = u["message"]["text"].lower()
            except:
                continue

            if "daily" in text:
                send_telegram("📈 Scan DAILY dimulai...")
                run_scan("daily")

            elif "intraday" in text:
                send_telegram("🚀 Scan INTRADAY dimulai...")
                run_scan("intraday")

        time.sleep(2)


# =========================================================
# START
# =========================================================
if __name__ == "__main__":
    listener()
