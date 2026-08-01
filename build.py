#!/usr/bin/env python3
"""
Econ/Politics Monitor — dashboard ข่าวเศรษฐกิจ / การเมือง / ธุรกิจ / สิ่งแวดล้อม
พร้อมแผนที่โลกแบบซูมได้ และแถบราคาตลาด
รันทุก 3 ชั่วโมง แล้วเขียนทับ index.html

ติดตั้ง:  pip install feedparser requests
รัน:      python3 build.py
"""

import os
import re
import html
import json
import time
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from urllib.parse import urljoin

import feedparser
import requests

socket.setdefaulttimeout(15)   # กัน feedparser ค้างถ้าเว็บข่าวไม่ตอบสนอง

TZ = timezone(timedelta(hours=7))          # Asia/Bangkok
NOW = datetime.now(TZ)
MAX_AGE_HOURS = 24
PER_CATEGORY = 18
PER_ROW = 14          # จำนวนการ์ดต่อแถว (แยกไทย/ต่างประเทศแล้วจึงลดลงจาก PER_CATEGORY)
CACHE_FILE = "cache.json"
HISTORY_FILE = "market_history.json"
HISTORY_POINTS = 60     # 60 รอบ x 3 ชม. ≈ 7.5 วัน

# ─────────────────────────────────────────────────────────────
# แหล่งข่าว — เพิ่ม/ลบได้ตามใจ ไม่ต้องใช้ API key
# ─────────────────────────────────────────────────────────────
FEEDS = [
    ("Thai PBS",        "https://news.thaipbs.or.th/rss/news",                       "th"),
    ("The Standard",    "https://thestandard.co/feed/",                              "th"),
    ("ประชาชาติธุรกิจ",  "https://www.prachachat.net/feed",                            "th"),
    ("มติชน",           "https://www.matichon.co.th/feed",                            "th"),
    ("ไทยรัฐ",          "https://www.thairath.co.th/rss/news",                        "th"),
    ("ไทยรัฐ",          "https://www.thairath.co.th/rss/business",                    "th"),
    ("อินโฟเควสท์",      "https://www.infoquest.co.th/feed",                           "th"),
    ("BBC Business",    "https://feeds.bbci.co.uk/news/business/rss.xml",             "en"),
    ("BBC World",       "https://feeds.bbci.co.uk/news/world/rss.xml",                "en"),
    ("Al Jazeera",      "https://www.aljazeera.com/xml/rss/all.xml",                  "en"),
    ("CNBC",            "https://www.cnbc.com/id/100727362/device/rss/rss.html",      "en"),
    ("Google News",     "https://news.google.com/rss/search?q=เศรษฐกิจไทย&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Google News",     "https://news.google.com/rss/search?q=การเมืองไทย&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Reuters",         "https://news.google.com/rss/search?q=site:reuters.com+when:1d&hl=en&gl=US&ceid=US:en", "en"),
    ("Investing.com",   "https://www.investing.com/rss/news.rss",                     "en"),
    ("Google News",     "https://news.google.com/rss/search?q=ธุรกิจ&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Google News",     "https://news.google.com/rss/search?q=พฤติกรรมผู้บริโภค&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Google News",     "https://news.google.com/rss/search?q=ภัยพิบัติ+OR+น้ำท่วม+OR+แผ่นดินไหว&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Google News",     "https://news.google.com/rss/search?q=consumer+trends+OR+retail+sales+when:1d&hl=en&gl=US&ceid=US:en", "en"),
    ("Google News",     "https://news.google.com/rss/search?q=climate+OR+wildfire+OR+flood+OR+earthquake+when:1d&hl=en&gl=US&ceid=US:en", "en"),
    ("BBC Sci/Env",     "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "en"),
    ("The Guardian",    "https://www.theguardian.com/environment/rss",                 "en"),
    ("The Guardian",    "https://www.theguardian.com/environment/climate-crisis/rss",  "en"),
]

THAI_GOLD = "__THAIGOLD__"      # ไม่ใช่สัญลักษณ์ Yahoo — ดึงจากสมาคมค้าทองคำแทน

# (ชื่อที่แสดง, สัญลักษณ์ Yahoo, แถว)  — แถว th = สินทรัพย์ไทย, intl = ต่างประเทศ
TICKERS = [
    ("SET",       "^SET.BK",  "th"),
    ("USD/THB",   "THB=X",    "th"),
    ("GOLD THB",  THAI_GOLD,  "th"),
    # ธนาคาร/การเงินไทย
    ("SCB",       "SCB.BK",   "th"),
    ("KBANK",     "KBANK.BK", "th"),
    ("BBL",       "BBL.BK",   "th"),
    ("KTB",       "KTB.BK",   "th"),
    ("TTB",       "TTB.BK",   "th"),
    ("BAY",       "BAY.BK",   "th"),
    ("TISCO",     "TISCO.BK", "th"),
    # พลังงาน + ค้าปลีกไทย
    ("PTT",       "PTT.BK",   "th"),
    ("GULF",      "GULF.BK",  "th"),
    ("OR",        "OR.BK",    "th"),
    ("CPALL",     "CPALL.BK", "th"),

    ("S&P 500",   "^GSPC",    "intl"),
    ("NASDAQ",    "^IXIC",    "intl"),
    ("GOLD USD",  "GC=F",     "intl"),
    ("BITCOIN",   "BTC-USD",  "intl"),
    ("COKE",      "COKE",     "intl"),
    ("COST",      "COST",     "intl"),
    ("WMT",       "WMT",      "intl"),
    ("JEPQ",      "JEPQ",     "intl"),
    ("MSFT",      "MSFT",     "intl"),
    ("AMZN",      "AMZN",     "intl"),
    ("NVDA",      "NVDA",     "intl"),
    ("BRK.B",     "BRK-B",    "intl"),
    ("GOOG",      "GOOG",     "intl"),
    ("AAPL",      "AAPL",     "intl"),
]

# คำที่ใช้จับว่าข่าวไหน "เกี่ยวข้อง" กับสินทรัพย์ตัวไหน
#   primary   = ชื่อบริษัท/ชื่อสินทรัพย์โดยตรง (น้ำหนักสูง)
#   secondary = ปัจจัยแวดล้อมของกลุ่มนั้น (น้ำหนักรอง)
_US_EQUITY = ["wall street", "s&p", "nasdaq", "fed", "tariff", "earnings",
              "หุ้นสหรัฐ", "วอลล์สตรีท", "ตลาดหุ้นสหรัฐ", "ภาษีทรัมป์"]
_GOLD = ["safe haven", "สินทรัพย์ปลอดภัย", "เงินเฟ้อ", "inflation",
         "ดอกเบี้ย", "interest rate", "ดอลลาร์"]
_TH_BANK = ["ธนาคาร", "แบงก์", "สินเชื่อ", "หนี้เสีย", "npl", "ธปท", "แบงก์ชาติ",
            "ดอกเบี้ย", "หุ้นไทย", "ตลาดหลักทรัพย์", "กำไรไตรมาส"]
_TH_ENERGY = ["พลังงาน", "น้ำมัน", "ไฟฟ้า", "ก๊าซ", "โรงกลั่น", "ราคาน้ำมัน",
              "opec", "หุ้นไทย", "ตลาดหลักทรัพย์", "กำไรไตรมาส"]

TICKER_TERMS = {
    "SET":      (["set index", "ตลาดหลักทรัพย์", "หุ้นไทย", "ตลาดหุ้นไทย"],
                 ["เศรษฐกิจไทย", "ธปท", "แบงก์ชาติ", "ก.ล.ต.", "จีดีพี", "นักลงทุนต่างชาติ", "บจ."]),
    "S&P 500":  (["s&p 500", "s&p500", "เอสแอนด์พี"], _US_EQUITY),
    "NASDAQ":   (["nasdaq", "แนสแด็ก"],
                 _US_EQUITY + ["chip", "semiconductor", "ชิป", "หุ้นเทค", "ai"]),
    # "ดอลลาร์" เฉยๆ ใช้เป็นคำหลักไม่ได้ ข่าวไหนพูดถึงจำนวนเงินก็ติดหมด
    "USD/THB":  (["เงินบาท", "ค่าเงินบาท", "ค่าเงินดอลลาร์", "ดอลลาร์แข็งค่า",
                  "ดอลลาร์อ่อนค่า", "บาทแข็งค่า", "บาทอ่อนค่า", "usd/thb", "อัตราแลกเปลี่ยน"],
                 ["ดอลลาร์", "ค่าเงิน", "fed", "ดอกเบี้ย", "currency", "ทุนสำรอง",
                  "forex", "exchange rate"]),
    "GOLD USD": (["ทองคำ", "gold", "xau"], _GOLD),
    "GOLD THB": (["ราคาทอง", "ทองคำ", "ทองคำแท่ง", "สมาคมค้าทองคำ", "gold"],
                 _GOLD + ["เงินบาท", "ค่าเงินบาท"]),
    "BITCOIN":  (["bitcoin", "บิตคอยน์", "บิทคอยน์", "คริปโต", "crypto", "cryptocurrency", "btc"],
                 ["blockchain", "digital asset", "etf", "เหรียญดิจิทัล", "สินทรัพย์ดิจิทัล"]),
    "COKE":     (["coca-cola", "โคคา-โคล่า", "coke"], ["เครื่องดื่ม", "beverage", "consumer staples"]),
    "COST":     (["costco", "คอสท์โก้"], ["ค้าปลีก", "retail", "consumer", "warehouse"]),
    "WMT":      (["walmart", "วอลมาร์ท"], ["ค้าปลีก", "retail", "consumer", "supermarket"]),
    "JEPQ":     (["jepq", "jpmorgan", "เจพีมอร์แกน"], ["etf", "nasdaq", "dividend", "ปันผล"]),
    "MSFT":     (["microsoft", "ไมโครซอฟท์", "azure", "openai"], _US_EQUITY + ["cloud", "ai", "ชิป"]),
    "AMZN":     (["amazon", "อเมซอน", "aws"], _US_EQUITY + ["e-commerce", "อีคอมเมิร์ซ", "cloud"]),
    "NVDA":     (["nvidia", "เอ็นวิเดีย"],
                 _US_EQUITY + ["chip", "semiconductor", "ชิป", "ai", "เอไอ", "gpu"]),
    "BRK.B":    (["berkshire", "buffett", "บัฟเฟตต์", "เบิร์กเชียร์"], _US_EQUITY + ["insurance", "ลงทุน"]),
    "GOOG":     (["alphabet", "google", "กูเกิล", "gemini"], _US_EQUITY + ["ai", "เอไอ", "โฆษณา", "search"]),
    "AAPL":     (["apple", "แอปเปิล", "iphone", "ไอโฟน"], _US_EQUITY + ["ชิป", "chip", "สมาร์ทโฟน"]),

    # ธนาคารไทย — เลี่ยงคำที่กว้างเกิน เช่น "กรุงเทพ" (ชนกับกรุงเทพมหานคร)
    # และ "bay" (ชนกับ Bay Area) จึงใช้ชื่อเต็มหรือชื่อย่อที่เจาะจงแทน
    "SCB":      (["scb", "scbx", "ไทยพาณิชย์"], _TH_BANK),
    "KBANK":    (["kbank", "กสิกรไทย", "กสิกร"], _TH_BANK),
    "BBL":      (["bbl", "ธนาคารกรุงเทพ"], _TH_BANK),
    "KTB":      (["ktb", "กรุงไทย"], _TH_BANK),
    "TTB":      (["ttb", "ทีเอ็มบีธนชาต", "ทหารไทยธนชาต"], _TH_BANK),
    "BAY":      (["กรุงศรีอยุธยา", "ธนาคารกรุงศรี", "กรุงศรี", "krungsri"], _TH_BANK),

    # พลังงาน/สาธารณูปโภคไทย
    "TISCO":    (["tisco", "ทิสโก้"], _TH_BANK),
    "PTT":      (["ptt", "ปตท."], _TH_ENERGY),
    "GULF":     (["กัลฟ์", "gulf development", "gulf energy"], _TH_ENERGY),
    "OR":       (["โออาร์", "pttor", "ptt oil"], _TH_ENERGY + ["ค้าปลีก", "สถานีบริการ"]),
    "CPALL":    (["cpall", "cp all", "ซีพี ออลล์", "ซีพีออลล์", "เซเว่น", "7-eleven"],
                 ["ค้าปลีก", "retail", "ร้านสะดวกซื้อ", "หุ้นไทย", "ตลาดหลักทรัพย์",
                  "กำไรไตรมาส", "บริโภค"]),
}
RELEVANCE_FLOOR = 4      # ต่ำกว่านี้ถือว่าไม่เกี่ยว ไม่ต้องแสดง
RELEVANCE_MAX = 10       # จำนวนข่าวต่อสินทรัพย์


def _compile_terms(terms):
    """คำอังกฤษต้องจับแบบทั้งคำ ไม่งั้น 'gold' จะไปโดน 'Goldman'/'golden'
    และ 'ai' จะโดน 'said' — ส่วนภาษาไทยไม่มีตัวคั่นคำ จึงจับแบบ substring ตามเดิม
    (อนุญาต s ต่อท้ายไว้ เพราะ 'chip' กับ 'chips' ควรนับว่าตรงกัน)"""
    out = []
    for t in (x.lower() for x in terms):
        if re.search(r"[a-z]", t):
            out.append(re.compile(rf"(?<![a-z0-9]){re.escape(t)}s?(?![a-z0-9])"))
        else:
            out.append(t)
    return out


TICKER_TERMS_C = {k: (_compile_terms(p), _compile_terms(s))
                  for k, (p, s) in TICKER_TERMS.items()}


def _hit(term, text):
    return term.search(text) is not None if hasattr(term, "search") else term in text


def relevance(it, primary, secondary):
    """คะแนน "ความเกี่ยวข้อง" ของข่าวกับสินทรัพย์ 0-100

    เป็นการจับคู่คำล้วนๆ ไม่ใช่การวัดว่าข่าวทำให้ราคาขยับกี่เปอร์เซ็นต์
    (ข้อมูลแบบนั้นไม่มีให้คำนวณจริง)

    แถบสีจึงมีความหมายชัดเจน:
      > 50 (เขียว)  ข่าวเอ่ยถึงสินทรัพย์นี้ตรงๆ ในหัวข้อ
      10-49 (เหลือง) เอ่ยถึงในเนื้อข่าว หรือเกี่ยวทางอ้อมชัดเจน
      < 10 (แดง)    แตะแค่ปัจจัยตลาดกว้างๆ ไม่ได้พูดถึงตัวนี้เลย
    """
    title = it["title"].lower()
    summary = (it.get("summary") or "").lower()
    direct = 0
    for t in primary:
        if _hit(t, title):
            direct = max(direct, 60)
        elif _hit(t, summary):
            direct = max(direct, 32)
    side = 0
    for t in secondary:
        if _hit(t, title):
            side += 8
        elif _hit(t, summary):
            side += 4
    if not direct:
        return min(9, side)      # ไม่ได้พูดถึงตัวนี้เลย → อยู่ในแถบแดงเสมอ
    return min(100, direct + min(side, 24))


CHART_DIR = "chart"
CHART_RANGES = [          # (ชื่อปุ่ม, range, interval)
    ("1D", "1d", "5m"), ("1M", "1mo", "1d"), ("3M", "3mo", "1d"),
    ("6M", "6mo", "1d"), ("1Y", "1y", "1d"), ("3Y", "3y", "1wk"),
    ("5Y", "5y", "1wk"),
]


def chart_slug(label):
    """ชื่อไฟล์ที่ปลอดภัย เช่น 'S&P 500' → 'sp500', 'BRK.B' → 'brkb'"""
    return re.sub(r"[^a-z0-9]+", "", label.lower()) or "x"


def fetch_candles(sym, rng, interval):
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
        params={"range": rng, "interval": interval},
        headers={"User-Agent": "Mozilla/5.0"}, timeout=15,
    )
    res = r.json()["chart"]["result"][0]
    ts = res.get("timestamp") or []
    q = res["indicators"]["quote"][0]
    out = []
    for i, t in enumerate(ts):
        o, h, lo, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
        if None in (o, h, lo, c):
            continue
        out.append([t, round(o, 4), round(h, 4), round(lo, 4), round(c, 4)])
    return out


def build_charts(markets):
    """เขียนไฟล์แท่งเทียนแยกรายสินทรัพย์ไว้ให้หน้าเว็บโหลดตอนเปิดกราฟ

    แยกเป็นไฟล์ย่อยแทนที่จะฝังใน index.html เพราะข้อมูลรวมกันหลายเมกะไบต์
    """
    os.makedirs(CHART_DIR, exist_ok=True)
    symbols = {label: sym for label, sym, _ in TICKERS if sym != THAI_GOLD}
    jobs = [(m, tf, rng, iv) for m in markets if m["label"] in symbols
            for tf, rng, iv in CHART_RANGES]
    if not jobs:
        return {}

    def run(job):
        m, tf, rng, iv = job
        try:
            return m["label"], tf, fetch_candles(symbols[m["label"]], rng, iv)
        except Exception:
            return m["label"], tf, None

    frames = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for label, tf, candles in pool.map(run, jobs):
            if candles:
                frames.setdefault(label, {})[tf] = candles

    index = {}
    for label, tfs in frames.items():
        slug = chart_slug(label)
        save_json(f"{CHART_DIR}/{slug}.json", {"label": label, "tf": tfs})
        index[label] = slug
    total = sum(len(c) for tfs in frames.values() for c in tfs.values())
    print(f"  ✓ กราฟ {len(index)} สินทรัพย์ · {total:,} แท่งเทียน")
    return index


def attach_ticker_news(markets, news):
    """แนบข่าวที่เกี่ยวข้องกับแต่ละสินทรัพย์ เรียงตามคะแนนความเกี่ยวข้อง"""
    for m in markets:
        primary, secondary = TICKER_TERMS_C.get(m["label"], ([], []))
        scored = []
        for it in news:
            s = relevance(it, primary, secondary)
            if s >= RELEVANCE_FLOOR:
                scored.append((s, it))
        scored.sort(key=lambda x: (-x[0], -x[1]["dt"].timestamp()))
        m["news"] = [{
            "title": it["title"], "link": it["link"], "source": it["source"],
            "age": it["age"], "cat": it["cat"], "image": it.get("image"),
            "score": s,
        } for s, it in scored[:RELEVANCE_MAX]]
    return markets

KW_ECON = [
    "เศรษฐกิจ", "จีดีพี", "GDP", "เงินเฟ้อ", "ดอกเบี้ย", "ธปท", "แบงก์ชาติ", "ตลาดหุ้น",
    "หุ้น", "ค่าเงิน", "ส่งออก", "นำเข้า", "ลงทุน", "ภาษี", "งบประมาณ", "หนี้",
    "ราคาน้ำมัน", "ทองคำ", "คริปโต", "ธนาคาร", "ท่องเที่ยว", "อสังหา",
    "economy", "economic", "inflation", "gdp", "fed", "interest rate", "market",
    "stock", "trade", "tariff", "export", "import", "invest", "bank", "currency",
    "oil price", "gold", "crypto", "recession", "budget", "debt",
]
KW_POLI = [
    "การเมือง", "รัฐบาล", "นายก", "รัฐมนตรี", "สภา", "ส.ส.", "สว.", "พรรค", "เลือกตั้ง",
    "อภิปราย", "รัฐธรรมนูญ", "ยุบพรรค", "ม็อบ", "ชุมนุม", "ประท้วง",
    "นโยบาย", "ครม.", "กฎหมาย", "สงคราม", "ความขัดแย้ง", "ทหาร", "ชายแดน", "ทูต",
    "เจรจา", "สันติภาพ", "ข้อตกลง", "คว่ำบาตร", "ผู้นำ", "ประธานาธิบดี", "ความมั่นคง",
    "ลงนาม", "ประชุมสุดยอด", "หยุดยิง", "อพยพ", "ผู้ลี้ภัย",
    "politic", "government", "minister", "parliament", "election", "vote", "party",
    "president", "prime minister", "policy", "law", "war", "conflict", "military",
    "sanction", "diplomat", "protest", "coup", "border",
    "ceasefire", "peace talk", "summit", "treaty", "refugee", "strike", "sovereign",
]
KW_BIZ = [
    "ธุรกิจ", "บริษัท", "ผู้บริโภค", "พฤติกรรมผู้บริโภค", "ค้าปลีก", "อีคอมเมิร์ซ",
    "สตาร์ทอัพ", "แบรนด์", "การตลาด", "ยอดขาย", "กำไร", "ไตรมาส", "ผลประกอบการ",
    "ควบรวมกิจการ", "เปิดตัวสินค้า", "นวัตกรรม", "ผู้ประกอบการ", "เอสเอ็มอี", "แฟรนไชส์",
    "ผู้ผลิต", "โรงงาน", "ซัพพลายเชน", "ห่วงโซ่อุปทาน",
    "business", "company", "companies", "corporate", "consumer", "retail", "e-commerce",
    "startup", "brand", "marketing", "earnings", "quarterly", "ipo", "merger",
    "acquisition", "ceo", "product launch", "supply chain", "manufacturer", "factory",
]
KW_ENV = [
    "สิ่งแวดล้อม", "ภัยพิบัติ", "น้ำท่วม", "แผ่นดินไหว", "พายุ", "ไฟป่า", "ภัยแล้ง",
    "ฝนตกหนัก", "ดินถล่ม", "คลื่นความร้อน", "มลพิษ", "ฝุ่นควัน", "โลกร้อน",
    "การเปลี่ยนแปลงสภาพภูมิอากาศ", "โลกรวน", "คาร์บอน", "พลังงานสะอาด", "พลังงานหมุนเวียน",
    "ระดับน้ำทะเล", "สึนามิ", "พายุเฮอริเคน", "ทอร์นาโด", "อากาศแปรปรวน",
    "climate change", "climate crisis", "natural disaster", "flood", "earthquake",
    "wildfire", "drought", "heatwave", "hurricane", "typhoon", "tsunami", "landslide",
    "pollution", "carbon emission", "greenhouse gas", "global warming", "extreme weather",
    "cyclone", "storm surge", "wildfire smoke", "air quality", "deforestation",
]

# ─────────────────────────────────────────────────────────────
# พิกัดสถานที่ — เพิ่มเองได้: (ชื่อที่แสดง, lat, lon, [คำที่ใช้จับ])
# ─────────────────────────────────────────────────────────────
PLACES = [
    ("ไทย",           13.75,  100.50, ["ประเทศไทย", "ไทย", "กรุงเทพ", "thailand", "bangkok"]),
    ("สหรัฐฯ",        38.90,  -77.04, ["สหรัฐ", "อเมริกา", "วอชิงตัน", "ทำเนียบขาว", "เฟด", "united states",
                                        "u.s.", "usa", "america", "washington", "white house",
                                        "federal reserve", "fed", "imf", "world bank"]),
    ("นิวยอร์ก",      40.71,  -74.01, ["นิวยอร์ก", "วอลล์สตรีท", "new york", "wall street",
                                        "united nations", "สหประชาชาติ"]),
    ("จีน",           39.90,  116.40, ["จีน", "ปักกิ่ง", "china", "chinese", "beijing", "yuan"]),
    ("เซี่ยงไฮ้",      31.23,  121.47, ["เซี่ยงไฮ้", "shanghai"]),
    ("ฮ่องกง",        22.32,  114.17, ["ฮ่องกง", "hong kong"]),
    ("ไต้หวัน",       25.03,  121.57, ["ไต้หวัน", "ไทเป", "taiwan", "taipei"]),
    ("ญี่ปุ่น",        35.68,  139.69, ["ญี่ปุ่น", "โตเกียว", "japan", "tokyo", "yen"]),
    ("เกาหลีใต้",     37.57,  126.98, ["เกาหลีใต้", "โซล", "south korea", "seoul"]),
    ("เกาหลีเหนือ",   39.02,  125.75, ["เกาหลีเหนือ", "north korea", "pyongyang"]),
    ("อินเดีย",       28.61,   77.21, ["อินเดีย", "นิวเดลี", "india", "delhi", "mumbai"]),
    ("รัสเซีย",       55.75,   37.62, ["รัสเซีย", "มอสโก", "russia", "russian", "moscow", "kremlin"]),
    ("ยูเครน",        50.45,   30.52, ["ยูเครน", "เคียฟ", "ukraine", "kyiv", "kiev"]),
    ("สหราชอาณาจักร", 51.51,   -0.13, ["อังกฤษ", "สหราชอาณาจักร", "ลอนดอน", "britain", "british",
                                        "uk", "london", "bank of england"]),
    ("ฝรั่งเศส",      48.86,    2.35, ["ฝรั่งเศส", "ปารีส", "france", "french", "paris"]),
    ("เยอรมนี",       52.52,   13.40, ["เยอรมนี", "เยอรมัน", "เบอร์ลิน", "germany", "german",
                                        "berlin", "frankfurt"]),
    ("อียู",          50.85,    4.35, ["สหภาพยุโรป", "อียู", "บรัสเซลส์", "european union",
                                        "brussels", "nato", "ecb"]),
    ("อิตาลี",        41.90,   12.50, ["อิตาลี", "โรม", "italy", "rome"]),
    ("สเปน",          40.42,   -3.70, ["สเปน", "มาดริด", "spain", "madrid"]),
    ("สวิตเซอร์แลนด์", 46.95,   7.45, ["สวิตเซอร์แลนด์", "สวิส", "ดาวอส", "switzerland",
                                        "swiss", "davos", "geneva"]),
    ("เนเธอร์แลนด์",  52.37,    4.90, ["เนเธอร์แลนด์", "ฮอลแลนด์", "netherlands", "amsterdam"]),
    ("ตุรกี",         39.93,   32.86, ["ตุรกี", "อังการา", "turkey", "turkish", "ankara", "istanbul"]),
    ("อิสราเอล",      31.78,   35.22, ["อิสราเอล", "israel", "israeli", "jerusalem", "tel aviv"]),
    ("กาซา",          31.50,   34.45, ["กาซา", "ปาเลสไตน์", "gaza", "palestin"]),
    ("เลบานอน",       33.89,   35.50, ["เลบานอน", "เบรุต", "lebanon", "beirut"]),
    ("ซีเรีย",        33.51,   36.28, ["ซีเรีย", "syria", "damascus"]),
    ("อิหร่าน",       35.69,   51.39, ["อิหร่าน", "เตหะราน", "iran", "iranian", "tehran"]),
    ("ซาอุดีอาระเบีย", 24.71,  46.68, ["ซาอุ", "ริยาด", "saudi", "riyadh", "opec"]),
    ("ยูเออี",        25.20,   55.27, ["ดูไบ", "อาบูดาบี", "ยูเออี", "dubai", "abu dhabi",
                                        "uae", "emirates"]),
    ("กาตาร์",        25.29,   51.53, ["กาตาร์", "โดฮา", "qatar", "doha"]),
    ("อียิปต์",       30.04,   31.24, ["อียิปต์", "ไคโร", "egypt", "cairo"]),
    ("ไนจีเรีย",       9.06,    7.49, ["ไนจีเรีย", "nigeria", "lagos", "abuja"]),
    ("แอฟริกาใต้",   -25.75,   28.19, ["แอฟริกาใต้", "south africa", "johannesburg", "pretoria"]),
    ("บราซิล",       -15.79,  -47.88, ["บราซิล", "brazil", "brasilia", "sao paulo"]),
    ("อาร์เจนตินา",  -34.60,  -58.38, ["อาร์เจนตินา", "argentina", "buenos aires"]),
    ("เม็กซิโก",      19.43,  -99.13, ["เม็กซิโก", "mexico", "mexican"]),
    ("แคนาดา",        45.42,  -75.70, ["แคนาดา", "ออตตาวา", "canada", "canadian", "ottawa", "toronto"]),
    ("ออสเตรเลีย",   -35.28,  149.13, ["ออสเตรเลีย", "australia", "sydney", "canberra", "melbourne"]),
    ("สิงคโปร์",       1.35,  103.82, ["สิงคโปร์", "singapore"]),
    ("มาเลเซีย",       3.14,  101.69, ["มาเลเซีย", "กัวลาลัมเปอร์", "malaysia", "kuala lumpur"]),
    ("อินโดนีเซีย",   -6.21,  106.85, ["อินโดนีเซีย", "จาการ์ตา", "indonesia", "jakarta",
                                        "อาเซียน", "asean"]),
    ("เวียดนาม",      21.03,  105.85, ["เวียดนาม", "ฮานอย", "vietnam", "hanoi", "ho chi minh"]),
    ("ฟิลิปปินส์",    14.60,  120.98, ["ฟิลิปปินส์", "มะนิลา", "philippin", "manila"]),
    ("เมียนมา",       16.87,   96.20, ["เมียนมา", "พม่า", "ย่างกุ้ง", "myanmar", "burma", "yangon"]),
    ("กัมพูชา",       11.56,  104.92, ["กัมพูชา", "เขมร", "พนมเปญ", "cambodia", "phnom penh"]),
    ("สปป.ลาว",      17.97,  102.60, ["สปป.ลาว", "ประเทศลาว", "เวียงจันทน์", "laos", "vientiane"]),
    ("ปากีสถาน",      33.68,   73.05, ["ปากีสถาน", "pakistan", "islamabad"]),
    ("บังกลาเทศ",     23.81,   90.41, ["บังกลาเทศ", "bangladesh", "dhaka"]),
]

# เรียงคำยาวก่อน เพื่อให้จับคำเฉพาะเจาะจงก่อนคำกว้าง
_PLACE_LOOKUP = sorted(
    [(kw.lower(), name, lat, lon) for name, lat, lon, kws in PLACES for kw in kws],
    key=lambda x: -len(x[0]),
)


def geolocate(text):
    """คืน (ชื่อสถานที่, lat, lon) — เลือกสถานที่ที่ถูกกล่าวถึงก่อนในข้อความ"""
    t = text.lower()
    best = None
    for kw, name, lat, lon in _PLACE_LOOKUP:
        if re.search(r"[a-z]", kw):
            m = re.search(rf"(?<![a-z]){re.escape(kw)}(?![a-z])", t)
            pos = m.start() if m else -1
        else:
            pos = t.find(kw)
        if pos < 0:
            continue
        # ตำแหน่งเร็วกว่าชนะ ถ้าเท่ากันให้คำที่ยาวกว่าชนะ
        if best is None or pos < best[0] or (pos == best[0] and len(kw) > best[1]):
            best = (pos, len(kw), name, lat, lon)
    return (best[2], best[3], best[4]) if best else None


CATEGORIES = [
    ("econ", KW_ECON), ("poli", KW_POLI), ("biz", KW_BIZ), ("env", KW_ENV),
]


def classify(text):
    t = text.lower()
    scores = {cat: sum(1 for k in kws if k.lower() in t) for cat, kws in CATEGORIES}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else None


def clean(s):
    s = re.sub(r"<[^>]+>", "", s or "")
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def age_label(dt):
    mins = max(0, int((NOW - dt).total_seconds() // 60))
    if mins < 1:
        return "เมื่อครู่"
    if mins < 60:
        return f"{mins} นาทีที่แล้ว"
    hrs = mins // 60
    if hrs < 24:
        return f"{hrs} ชม.ที่แล้ว"
    return f"{hrs // 24} วันที่แล้ว"


def _widest(entries):
    """เลือกรูปที่กว้างที่สุดจาก media:thumbnail / media:content
    บางเจ้า (เช่น The Guardian) ไม่ใส่ type/medium มาด้วย จึงรับกรณีที่ไม่ระบุด้วย"""
    best, best_w = None, -1
    for m in entries or []:
        url = m.get("url")
        if not url:
            continue
        kind = (m.get("type") or "") + " " + (m.get("medium") or "")
        if kind.strip() and "image" not in kind:
            continue          # ข้าม video/audio ที่ระบุชนิดมาชัดเจน
        try:
            w = int(m.get("width") or 0)
        except (TypeError, ValueError):
            w = 0
        if w > best_w:
            best, best_w = url, w
    return best


def extract_image(e):
    """ดึงรูปประกอบข่าวจาก media:thumbnail / media:content / enclosure / <img> แรกใน summary"""
    for key in ("media_thumbnail", "media_content"):
        url = _widest(e.get(key))
        if url:
            return url

    for lk in e.get("links") or []:
        if lk.get("rel") == "enclosure" and "image" in (lk.get("type") or ""):
            return lk.get("href")

    m = re.search(r'<img[^>]+src="([^"]+)"', e.get("summary", "") or "")
    if m:
        return m.group(1)
    return None


BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"}
OG_TAG = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:og:image(?::url)?|twitter:image(?::src)?)["\'][^>]*>',
    re.I)
OG_CONTENT = re.compile(r'content=["\']([^"\']+)["\']', re.I)
IMG_WORKERS = 12
IMG_BUDGET = 110      # จำกัดจำนวนหน้าที่ไปเปิด กันไม่ให้ build นานเกินไป


def fetch_og_image(url):
    """เปิดหน้าข่าวแล้วดึง og:image / twitter:image (อ่านแค่ช่วงต้นของ <head> พอ)"""
    try:
        r = requests.get(url, headers=BROWSER_UA, timeout=8,
                         allow_redirects=True, stream=True)
        if r.status_code != 200 or "html" not in r.headers.get("content-type", ""):
            return None
        raw = r.raw.read(200_000, decode_content=True)
        r.close()
        tag = OG_TAG.search(raw.decode(r.encoding or "utf-8", errors="ignore"))
        if not tag:
            return None
        m = OG_CONTENT.search(tag.group(0))
        if not m:
            return None
        img = urljoin(r.url, html.unescape(m.group(1)).strip())
        return img if img.startswith("http") else None
    except Exception:
        return None


def enrich_images(items):
    """ข่าวที่ RSS ไม่ส่งรูปมา (เช่น CNBC, Al Jazeera) ให้ไปดึงรูปจากหน้าข่าวจริง

    ข้ามลิงก์ Google News เพราะเป็นหน้าคั่นที่ไม่มี og:image
    และ URL จริงถูกเข้ารหัสไว้จนถอดไม่ได้
    """
    targets = [it for it in items
               if not it.get("image") and "news.google.com" not in it["link"]][:IMG_BUDGET]
    if not targets:
        return
    with ThreadPoolExecutor(max_workers=IMG_WORKERS) as pool:
        found = list(pool.map(lambda it: fetch_og_image(it["link"]), targets))
    got = 0
    for it, img in zip(targets, found):
        if img:
            it["image"] = img
            got += 1
    print(f"  ✓ เติมรูปจากหน้าข่าวได้ {got}/{len(targets)} ข่าว")


def fetch_news():
    items = []
    for source, url, lang in FEEDS:
        try:
            d = feedparser.parse(url)
        except Exception as ex:
            print(f"  ! {source}: {ex}")
            continue

        for e in d.entries[:40]:
            try:
                title = clean(e.get("title", ""))
                if source == "Google News":
                    # Google News ต่อท้ายหัวข้อด้วย " - ชื่อสำนักข่าว" เสมอ
                    # ถ้าไม่ตัดออก ชื่อสำนักข่าวจะไปโดนจับคู่ด้วย เช่นข่าวค่าเงินสวิส
                    # ที่เผยแพร่โดย "Bitcoin World" กลายเป็นข่าวบิตคอยน์
                    title = re.sub(r"\s+-\s+[^-]{2,40}$", "", title).strip() or title
                if not title:
                    continue

                tp = e.get("published_parsed") or e.get("updated_parsed")
                dt = (datetime.fromtimestamp(time.mktime(tp), tz=timezone.utc).astimezone(TZ)
                      if tp else NOW)
                if (NOW - dt).total_seconds() > MAX_AGE_HOURS * 3600:
                    continue

                raw_summary = e.get("summary", "") or ""
                summary = clean(raw_summary)[:320]
                blob = title + " " + summary
                cat = classify(blob)
                if not cat:
                    continue

                geo = geolocate(blob)
                items.append({
                    "title": title, "summary": summary, "link": e.get("link", "#"),
                    "source": source, "lang": lang, "cat": cat, "dt": dt,
                    "age": age_label(dt), "image": extract_image(e),
                    "place": geo[0] if geo else None,
                    "lat": geo[1] if geo else None,
                    "lon": geo[2] if geo else None,
                })
            except Exception as ex:
                print(f"  ! {source} entry skipped: {ex}")
        print(f"  ✓ {source}")

    unique = []
    for it in sorted(items, key=lambda x: x["dt"], reverse=True):
        if any(SequenceMatcher(None, it["title"], u["title"]).ratio() > 0.72 for u in unique):
            continue
        unique.append(it)
    return unique


CAT_NAMES = [c for c, _ in CATEGORIES]

CAT_LABELS = {
    "econ": "เศรษฐกิจ", "poli": "การเมือง",
    "biz": "ธุรกิจ", "env": "สิ่งแวดล้อม", "mixed": "ผสม",
}

SCOPES = [("th", "ข่าวไทย"), ("intl", "ข่าวต่างประเทศ")]


def scope_of(it):
    """แยกข่าวไทย / ต่างประเทศ

    ใช้พิกัดที่จับได้เป็นหลัก เพราะสื่อไทยรายงานข่าวต่างประเทศเยอะ
    และสื่อต่างชาติก็รายงานข่าวไทย — ดูแค่ภาษาจะแยกผิด
    ถ้าจับพิกัดไม่ได้ค่อยเดาจากภาษาของแหล่งข่าว
    """
    if it["place"] == "ไทย":
        return "th"
    if it["place"]:
        return "intl"
    return "th" if it["lang"] == "th" else "intl"

# ไอคอนประจำหมวด — เส้น stroke ใช้ currentColor จึงรับสีจาก .ic-* ได้
CAT_ICONS = {
    # เหรียญ + กราฟขาขึ้น
    "econ": '<circle cx="6" cy="18" r="3.5"/>'
            '<path d="M11 17.5L15.5 12l2.5 2.5L22 7"/><path d="M17 7h5v5"/>',
    # อาคารเสาแบบโรมัน
    "poli": '<path d="M2.5 10L12 4.5L21.5 10"/><path d="M4 20h16"/>'
            '<path d="M6.5 10.5v9M12 10.5v9M17.5 10.5v9"/>',
    # กระเป๋าเอกสาร
    "biz": '<rect x="3" y="8" width="18" height="12" rx="2"/>'
           '<path d="M9 8V6a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/><path d="M3 13h18"/>',
    # เมฆ + ลม
    "env": '<g transform="translate(.5 -1.5) scale(.82)">'
           '<path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"/></g>'
           '<path d="M4.5 19H11M14 19h5.5M8 22h8"/>',
    "mixed": '<circle cx="12" cy="12" r="6.5"/>',
}


def cat_icon(cat, extra=""):
    cls = " ".join(x for x in ("cicon", f"ic-{cat}", extra) if x)
    return (f'<svg class="{cls}" viewBox="0 0 24 24" aria-hidden="true">'
            f'{CAT_ICONS.get(cat, "")}</svg>')


def build_markers(news):
    """รวมข่าวตามสถานที่ → จุดบนแผนที่"""
    by_place = {}
    for it in news:
        if not it["place"]:
            continue
        m = by_place.setdefault(it["place"], {
            "place": it["place"], "lat": it["lat"], "lon": it["lon"],
            **{c: 0 for c in CAT_NAMES}, "stories": [],
        })
        m[it["cat"]] += 1
        if len(m["stories"]) < 6:
            m["stories"].append({
                "title": it["title"], "link": it["link"], "image": it["image"],
                "source": it["source"], "age": it["age"], "cat": it["cat"],
            })
    out = list(by_place.values())
    for m in out:
        m["total"] = sum(m[c] for c in CAT_NAMES)
        top = max(CAT_NAMES, key=lambda c: m[c])
        m["cat"] = top if sum(1 for c in CAT_NAMES if m[c] == m[top]) == 1 else "mixed"
    return sorted(out, key=lambda x: -x["total"])


OZ_GRAM = 31.1034768      # 1 troy ounce
BAHT_GRAM = 15.244        # ทอง 1 บาท (น้ำหนักไทย)
GOLD_965 = 0.965          # ความบริสุทธิ์ทอง 96.5%


def fetch_thai_gold(got):
    """ราคาทองคำแท่ง 96.5% สมาคมค้าทองคำ (ราคาขายออก, บาท/บาททอง)

    ถ้าดึงจากสมาคมไม่ได้ ประมาณจาก gold spot × USD/THB แทน
    """
    try:
        r = requests.get("https://thaigold.info/RealTimeDataV2/gtdata_.txt",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        for row in json.loads(r.text):
            if row.get("name") != "สมาคมฯ":
                continue
            price = float(str(row["ask"]).replace(",", ""))
            diff = float(str(row.get("diff") or 0).replace(",", "").replace("+", ""))
            prev = price - diff
            return price, ((diff / prev * 100) if prev else 0.0)
    except Exception as ex:
        print(f"    (สมาคมค้าทองคำใช้ไม่ได้: {ex} — ใช้ค่าประมาณจาก spot)")

    gold, thb = got.get("GOLD USD"), got.get("USD/THB")
    if not gold or not thb:
        raise RuntimeError("ไม่มีราคา gold spot / USDTHB ให้คำนวณ")
    return gold * thb / OZ_GRAM * BAHT_GRAM * GOLD_965, 0.0


def fetch_markets():
    out, got = [], {}
    for label, sym, group in TICKERS:
        try:
            if sym == THAI_GOLD:
                price, pct = fetch_thai_gold(got)
            else:
                r = requests.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                    params={"range": "2d", "interval": "1d"},
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=12,
                )
                meta = r.json()["chart"]["result"][0]["meta"]
                price = meta["regularMarketPrice"]
                prev = meta.get("chartPreviousClose") or meta.get("previousClose") or price
                pct = (price - prev) / prev * 100 if prev else 0
            got[label] = price
            decimals = 0 if price >= 10000 else 2
            chg = price - price / (1 + pct / 100) if pct else 0.0
            out.append({"label": label, "group": group, "price": f"{price:,.{decimals}f}",
                        "raw_price": price, "pct": pct, "pct_str": f"{pct:+.2f}%",
                        "chg_str": f"{abs(chg):,.{decimals}f}"})
            print(f"  ✓ {label}")
        except Exception as ex:
            print(f"  ! {label}: {ex}")
    return out


# ─────────────────────────────────────────────────────────────
# cache (fallback ถ้ารอบนี้ดึงข่าว/ราคาไม่ได้เลย) + ประวัติราคา (sparkline)
# ─────────────────────────────────────────────────────────────
def load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as ex:
        print(f"  ! save {path}: {ex}")


def news_from_cache(cache):
    out = []
    for d in cache.get("news", []):
        try:
            d = dict(d)
            dt = datetime.fromisoformat(d["dt"])
            if (NOW - dt).total_seconds() > MAX_AGE_HOURS * 3600:
                continue
            d["dt"] = dt
            d["age"] = age_label(dt)
            out.append(d)
        except Exception:
            continue
    return out


def save_cache(news, markets):
    serial_news = []
    for it in news:
        d = dict(it)
        d["dt"] = it["dt"].isoformat() if isinstance(it["dt"], datetime) else it["dt"]
        serial_news.append(d)
    save_json(CACHE_FILE, {"news": serial_news, "markets": markets})


def update_history(markets):
    """เก็บราคาย้อนหลังต่อ ticker ไว้วาด sparkline"""
    history = load_json(HISTORY_FILE)
    labels = {label for label, _, _ in TICKERS}
    for stale in set(history) - labels:      # ทิ้งประวัติของ ticker ที่ถอด/เปลี่ยนชื่อไปแล้ว
        del history[stale]
    ts = NOW.isoformat()
    for m in markets:
        h = history.setdefault(m["label"], [])
        h.append({"t": ts, "p": m["raw_price"]})
        del h[:-HISTORY_POINTS]
    save_json(HISTORY_FILE, history)
    return history


def top_keywords(items, n=14):
    stop = set("""the a an and or of to in on for at by with from is are was were be been
    this that it its as has have had will would can could not new says say said after
    over more than about into up down out ที่ และ ของ ใน การ เป็น มี ให้ กับ จาก ได้ ไม่
    จะ ก็ แต่ ว่า ด้วย เพื่อ ต่อ ยัง ทั้ง คน ปี วัน นี้ นั้น อยู่ ขึ้น ลง""".split())
    freq = {}
    for it in items:
        for w in re.findall(r"[ก-๙]{3,}|[A-Za-z]{3,}", it["title"]):
            if w.lower() in stop:
                continue
            freq[w] = freq.get(w, 0) + 1
    return sorted(freq.items(), key=lambda x: -x[1])[:n]


# ─────────────────────────────────────────────────────────────
# JavaScript ของแผนที่ (แยกไว้ ไม่ให้ปนกับ f-string)
# ─────────────────────────────────────────────────────────────
MAP_JS = r"""
const MARKERS = window.__MARKERS__;
const ICONS = window.__ICONS__;
const svg = d3.select("#map");
const gMap = svg.append("g");
const tip = d3.select("#tip");
const detail = d3.select("#hotspot-detail");
const COLOR = { econ:"#4C8DFF", poli:"#F5A524", biz:"#2DD4BF", env:"#4ADE80", mixed:"#9B8AFB" };

function show(d){
  detail.html(
    `<div class="hd-top"><h4>${d.place}</h4><span>${d.total} ข่าว</span></div>` +
    d.stories.map(s =>
      `<a class="hd-row" href="${s.link}" target="_blank" rel="noopener">
         ${s.image ? `<img class="hd-thumb" src="${s.image}" loading="lazy" alt="" onerror="this.remove()">` : ""}
         ${ICONS[s.cat] || ""}<span>${s.title}</span>
         <span class="hd-age">${s.age}</span></a>`).join("")
  );
}

const MAX_N = d3.max(MARKERS, d => d.total) || 1;
let markerSel = null, drawn = false;

// จุดยิ่งซูมยิ่งแยกจากกัน แต่ขนาดหมุด/ตัวอักษรคงเดิม (scale สวนกับ k)
function rescale(k){
  if (!markerSel) return;
  markerSel.attr("transform", d => `translate(${d._x},${d._y}) scale(${1 / k})`);
  markerSel.selectAll("text.mk-label")
    .style("display", d => d.total >= Math.max(1, MAX_N * 0.5 / k) ? null : "none");
}

const zoom = d3.zoom()
  .scaleExtent([1, 14])
  .on("zoom", (ev) => {
    gMap.attr("transform", ev.transform);
    rescale(ev.transform.k);
  });

function plot(proj){
  const r = d3.scaleSqrt().domain([1, MAX_N]).range([5, 17]);
  const g = gMap.append("g");

  MARKERS.forEach(d => {
    const p = proj([d.lon, d.lat]);
    d._x = p[0]; d._y = p[1];
  });

  const nodes = g.selectAll("g.mk").data(MARKERS).join("g")
    .attr("class", "mk")
    .attr("transform", d => `translate(${d._x},${d._y})`);

  nodes.append("circle").attr("class", "halo")
    .attr("r", d => r(d.total)).attr("fill", d => COLOR[d.cat]);
  nodes.append("circle").attr("class", "core")
    .attr("r", d => Math.max(2.6, r(d.total) * 0.36)).attr("fill", d => COLOR[d.cat]);

  nodes.append("text").attr("class", "mk-label")
    .attr("y", d => -r(d.total) - 6).attr("text-anchor", "middle")
    .text(d => d.place);

  nodes
    .on("mousemove", (ev, d) => {
      const [mx, my] = d3.pointer(ev, svg.node());
      tip.style("opacity", 1)
         .style("left", (mx + 14) + "px")
         .style("top", (my - 8) + "px")
         .html(`<strong>${d.place}</strong><span>${d.total} ข่าว · ศก. ${d.econ} · การเมือง ${d.poli} · ธุรกิจ ${d.biz} · สวล. ${d.env}</span>`);
    })
    .on("mouseleave", () => tip.style("opacity", 0))
    .on("click", (ev, d) => { show(d); ev.stopPropagation(); });

  markerSel = nodes;
  rescale(d3.zoomTransform(svg.node()).k);
}

function draw(){
  const box = svg.node().getBoundingClientRect();
  const W = box.width, H = box.height;
  if (!W || !H) return;   // ยังวัดขนาดไม่ได้ เดี๋ยวมีคนเรียกซ้ำให้เอง
  drawn = true;
  svg.attr("viewBox", `0 0 ${W} ${H}`);
  gMap.selectAll("*").remove();
  markerSel = null;

  const proj = d3.geoNaturalEarth1()
    .fitSize([W, H * 1.28], { type: "Sphere" })
    .translate([W / 2, H / 2 + H * 0.03]);
  const path = d3.geoPath(proj);

  gMap.append("path").attr("class", "sphere").attr("d", path({ type: "Sphere" }));
  gMap.append("path").attr("class", "grat").attr("d", path(d3.geoGraticule10()));

  svg.call(zoom).on("dblclick.zoom", null);
  gMap.attr("transform", d3.zoomTransform(svg.node()));

  d3.json("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json")
    .then(topo => {
      const land = topojson.feature(topo, topo.objects.countries);
      gMap.append("g").selectAll("path").data(land.features).join("path")
        .attr("class", "country").attr("d", path);
      plot(proj);
    })
    .catch(() => plot(proj));
}

const noMotion = matchMedia("(prefers-reduced-motion:reduce)");
function ease(ms){ return noMotion.matches ? svg : svg.transition().duration(ms); }
function zoomBy(f){ ease(220).call(zoom.scaleBy, f); }
function zoomReset(){ ease(260).call(zoom.transform, d3.zoomIdentity); }

if (MARKERS.length) show(MARKERS[0]);

let _t;
const redraw = (ms) => { clearTimeout(_t); _t = setTimeout(draw, ms); };

// ตอนสคริปต์รันครั้งแรก บางครั้งเบราว์เซอร์ยังไม่รู้ความกว้างของแผนที่ (วัดได้ 0)
// จึงลองใหม่เป็นระยะจนกว่าจะวาดสำเร็จ — ใช้ตัวจับเวลาล้วน ไม่พึ่ง API
// ที่ผูกกับรอบการวาดภาพ (rAF/ResizeObserver) ซึ่งอาจไม่ทำงานในบางสภาพแวดล้อม
(function ensureDrawn(tries){
  draw();
  if (!drawn && tries > 0) setTimeout(() => ensureDrawn(tries - 1), 150);
})(24);

addEventListener("load", () => redraw(60));
addEventListener("resize", () => redraw(200));
if (window.ResizeObserver) new ResizeObserver(() => redraw(120)).observe(svg.node());
"""


def render(news, markets, charts=None):
    def pick(items, n):
        """หน้าตาเน้นรูป → เลือกข่าวที่มีรูปก่อน แล้วค่อยเรียงตามเวลาเหมือนเดิม
        (ทุกข่าวอยู่ในกรอบ 24 ชม.อยู่แล้ว ลำดับจึงไม่เพี้ยนมาก)"""
        has = [i for i in items if i.get("image")]
        rest = [i for i in items if not i.get("image")]
        chosen = (has + rest)[:n]
        chosen.sort(key=lambda x: x["dt"], reverse=True)
        return chosen

    for it in news:
        it["scope"] = scope_of(it)

    # เรื่องเด่น = ข่าวใหม่สุดที่มีรูป (ถ้าไม่มีรูปเลยใช้ข่าวใหม่สุด)
    top_story = next((i for i in news if i.get("image")), news[0] if news else None)
    primary_scope = top_story["scope"] if top_story else "th"

    groups = {}
    for sc, _ in SCOPES:
        pool = [i for i in news if i["scope"] == sc]
        top = next((i for i in pool if i.get("image")), pool[0] if pool else None)
        groups[sc] = {
            "top": top,
            "latest": pick([i for i in pool if i is not top], PER_ROW),
            "cats": {c: pick([i for i in pool if i["cat"] == c], PER_ROW) for c in CAT_NAMES},
            "n": len(pool),
        }
    markers = build_markers(news)
    kws = top_keywords(news)
    maxf = max([f for _, f in kws], default=1)
    located = sum(1 for i in news if i["place"])

    def speak_attrs(it):
        text = html.escape(f"{it['title']}. {it['summary']}", quote=True)
        lang = "th-TH" if it["lang"] == "th" else "en-US"
        return f'data-text="{text}" data-lang="{lang}"'

    def poster(it):
        # ไอคอนหมวดวางไว้ใต้รูปเสมอ ถ้ารูปโหลดไม่ขึ้นจะเห็นพื้นไล่สี+ไอคอนแทน
        img = (f'<img src="{html.escape(it["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        loc = f'<span class="loc">{html.escape(it["place"])}</span>' if it["place"] else ""
        return f"""<article class="poster">
      <a class="poster-link" href="{html.escape(it['link'])}" target="_blank" rel="noopener">
        <div class="poster-img pf-{it['cat']}">{cat_icon(it['cat'], 'ci-lg')}{img}
          <span class="poster-cat">{cat_icon(it['cat'], 'ci-sm')}</span></div>
        <div class="poster-body">
          <h3>{html.escape(it['title'])}</h3>
          <div class="poster-meta"><span class="src">{html.escape(it['source'])}{loc}</span><span class="age">{it['age']}</span></div>
        </div>
      </a>
      <button class="speak poster-speak" type="button" title="ฟังข่าว" {speak_attrs(it)}>🔊</button>
    </article>"""

    def hero(it, scope, primary):
        img = (f'<img class="hero-img" src="{html.escape(it["image"])}" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        place = f' · {html.escape(it["place"])}' if it["place"] else ""
        return f"""<section class="hero pf-{it['cat']}" data-scope="{scope}" data-primary="{1 if primary else 0}">
  {img}
  <div class="hero-scrim"></div>
  <div class="hero-body">
    <div class="hero-badge">{cat_icon(it['cat'], 'ci-sm')}<span>{CAT_LABELS[it['cat']]}</span>
      <span class="hero-sep">·</span><span>{html.escape(it['source'])}{place}</span>
      <span class="hero-sep">·</span><span>{it['age']}</span></div>
    <h2 class="hero-title">{html.escape(it['title'])}</h2>
    {f'<p class="hero-sum">{html.escape(it["summary"])}</p>' if it['summary'] else ''}
    <div class="hero-actions">
      <a class="btn btn-main" href="{html.escape(it['link'])}" target="_blank" rel="noopener">▶ อ่านฉบับเต็ม</a>
      <button class="btn btn-ghost speak" type="button" {speak_attrs(it)}>🔊 ฟังข่าว</button>
    </div>
  </div>
</section>"""

    def tick(m, dup=False):
        cls = "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat")
        arrow = "▲" if m["pct"] > 0 else ("▼" if m["pct"] < 0 else "▬")
        # ชุดที่สองมีไว้ให้ marquee วนต่อเนื่อง ไม่ต้องให้ screen reader/แป้น Tab อ่านซ้ำ
        extra = ' dup" tabindex="-1" aria-hidden="true' if dup else ''
        return f"""<button class="tick{extra}" type="button" data-label="{html.escape(m['label'], quote=True)}"
      title="ดูข่าวที่เกี่ยวข้องกับ {html.escape(m['label'], quote=True)}"><span class="t-label">{html.escape(m['label'])}</span> <span class="t-price">{m['price']}</span> <span class="t-chg {cls}">{arrow} {m.get('chg_str', '—')}</span> <span class="t-pct {cls}">{m['pct_str']}</span></button>"""

    def ticker_row(group, title):
        items = [m for m in markets if m.get("group") == group]
        if not items:
            return ""
        body = "".join(tick(m) for m in items) + "".join(tick(m, True) for m in items)
        # ความเร็วคงที่ต่อการ์ด ไม่ว่าแถวไหนจะมีกี่ตัว
        dur = max(12, round(len(items) * 1.7))
        return f"""<div class="ticker-row">
    <span class="ticker-tag">{title}</span>
    <div class="ticker"><div class="ticker-track" style="animation-duration:{dur}s">{body}</div></div>
  </div>"""

    def hot_row(m, i):
        bars = "".join(f'<i class="hb-{c}" style="flex:{m[c]}"></i>' for c in CAT_NAMES if m[c])
        return f"""<div class="hot"><span class="rank">{i+1}</span>
      <span class="hot-name">{html.escape(m['place'])}</span>
      <span class="hot-bars">{bars}</span>
      <span class="hot-n">{m['total']}</span></div>"""

    def row_section(cat, items, rid, label=None, badge=""):
        if not items:
            return ""          # ไม่ต้องโชว์แถวเปล่า
        label = label or CAT_LABELS[cat]
        body = "".join(poster(i) for i in items)
        return f"""<section class="row">
  <div class="row-head">
    <h2>{cat_icon(cat)}{label}{badge}<span class="row-n">{len(items)}</span></h2>
    <div class="row-tools">
      <input class="search" type="search" placeholder="ค้นหา…" oninput="filterItems(this)">
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',-1)" aria-label="เลื่อนซ้าย">‹</button>
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',1)" aria-label="เลื่อนขวา">›</button>
    </div>
  </div>
  <div class="row-track" id="{rid}">{body}</div>
</section>"""

    def kw_chip(w, f):
        return (f'<span class="kw" style="font-size:{0.78 + (f/maxf)*0.85:.2f}rem;'
                f'opacity:{0.45 + (f/maxf)*0.55:.2f}">{html.escape(w)}</span>')

    heroes = "".join(
        hero(g["top"], sc, sc == primary_scope)
        for sc, _ in SCOPES if (g := groups[sc])["top"]
    )

    def scope_block(sc, label, rows):
        if not rows:
            return ""
        flag = "TH" if sc == "th" else "INTL"
        return f"""<div class="scope-group" data-scope="{sc}">
  <h2 class="scope-title">{label}<span class="scope-flag">{flag}</span></h2>
  {rows}
</div>"""

    # แยกเป็นสองก้อน เพราะแผนที่กับแถบราคามาคั่นระหว่างข่าวล่าสุดกับข่าวรายหมวด
    latest_blocks = "".join(
        scope_block(sc, lb, row_section("mixed", groups[sc]["latest"], f"row-{sc}-latest",
                                        "ล่าสุด", '<span class="live">LIVE</span>'))
        for sc, lb in SCOPES)
    category_blocks = "".join(
        scope_block(sc, lb, "".join(row_section(c, groups[sc]["cats"][c], f"row-{sc}-{c}")
                                    for c in CAT_NAMES))
        for sc, lb in SCOPES)

    next_run = (NOW + timedelta(hours=3)).strftime("%H:%M")
    markers_json = json.dumps(markers, ensure_ascii=False)
    icons_json = json.dumps({c: cat_icon(c, "ci-sm") for c in CAT_NAMES}, ensure_ascii=False)
    tnews_json = json.dumps(
        {m["label"]: {"price": m["price"], "pct": m["pct_str"], "pctv": round(m["pct"], 4),
                      "group": m.get("group", "intl"),
                      "dir": "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat"),
                      "news": m.get("news") or []}
         for m in markets}, ensure_ascii=False)
    charts_json = json.dumps(charts or {}, ensure_ascii=False)
    page_desc = f"ข่าวเศรษฐกิจ-การเมือง {len(news)} ข่าวใน 24 ชม. จาก {len(FEEDS)} แหล่ง อัปเดต {NOW.strftime('%d %b %Y %H:%M')} น."
    favicon = ("data:image/svg+xml,"
               "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E"
               "%3Crect width='100' height='100' rx='20' fill='%230A0E1A'/%3E"
               "%3Ccircle cx='30' cy='62' r='9' fill='%234C8DFF'/%3E"
               "%3Ccircle cx='58' cy='40' r='9' fill='%23F5A524'/%3E"
               "%3Ccircle cx='78' cy='58' r='7' fill='%239B8AFB'/%3E%3C/svg%3E")

    return f"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="900">
<title>Econ · Politics Monitor</title>
<meta name="description" content="{html.escape(page_desc)}">
<meta property="og:type" content="website">
<meta property="og:title" content="Econ · Politics Monitor">
<meta property="og:description" content="{html.escape(page_desc)}">
<meta property="og:url" content="https://netflixss266-lang.github.io/econ-monitor/">
<meta name="twitter:card" content="summary">
<meta name="theme-color" content="#0A0E1A">
<link rel="icon" href="{favicon}">
<link rel="manifest" href="manifest.json">
<link rel="apple-touch-icon" href="apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Econ Monitor">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Thai:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Playfair+Display:wght@600;700&display=swap" rel="stylesheet">
<script>
/* เล่นอินโทรครั้งเดียวต่อการเปิดเว็บ ไม่ให้เล่นซ้ำตอนหน้ารีเฟรชอัตโนมัติทุก 15 นาที */
try{{
  if(sessionStorage.getItem('introSeen') ||
     matchMedia('(prefers-reduced-motion:reduce)').matches){{
    document.documentElement.className += ' no-intro';
  }} else {{ sessionStorage.setItem('introSeen','1'); }}
}}catch(e){{ document.documentElement.className += ' no-intro'; }}
</script>
<style>
:root{{
  --bg:#0A0E1A; --panel:#111726; --panel2:#0E1420; --line:#1E2637;
  --ink:#E7ECF5; --mute:#7A879C; --dim:#4E5A70;
  --econ:#4C8DFF; --poli:#F5A524; --biz:#2DD4BF; --env:#4ADE80; --mixed:#9B8AFB;
  --up:#3FB68B; --down:#E5484D;
  --brass:#C6A961; --cream:#F4EFE3;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--ink);
  font-family:'IBM Plex Sans Thai',system-ui,sans-serif;
  font-size:15px;line-height:1.55;padding:20px;max-width:1560px;margin:0 auto;
  animation:pageIn .85s 1.9s backwards}}
a{{color:inherit;text-decoration:none}}

header{{display:flex;flex-direction:column;align-items:center;text-align:center;gap:9px;
  padding-bottom:16px;margin-bottom:16px;border-bottom:1px solid var(--line)}}
/* โลโก้ EPM — ตัวอักษรเซริฟแบบคลาสสิกอังกฤษ + เส้นทองเรียบๆ แบบสมัยใหม่ */
.logo{{display:flex;flex-direction:column;align-items:center;gap:9px;font-weight:400}}
.logo-mark{{font-family:'Playfair Display',Georgia,'Times New Roman',serif;font-weight:700;
  font-size:clamp(2.5rem,7.4vw,3.7rem);line-height:.94;
  letter-spacing:.16em;text-indent:.16em;color:var(--cream)}}
@supports ((-webkit-background-clip:text) or (background-clip:text)){{
  .logo-mark{{background:linear-gradient(176deg,#FBF7EE 8%,var(--cream) 46%,var(--brass) 128%);
    -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}}
}}
.logo-rule{{width:min(250px,64vw);height:1px;
  background:linear-gradient(90deg,transparent,var(--brass) 24%,var(--brass) 76%,transparent);
  opacity:.72}}
.logo-sub{{font-size:clamp(.55rem,1.55vw,.71rem);font-weight:500;
  letter-spacing:.4em;text-indent:.4em;text-transform:uppercase;color:var(--mute)}}
.stamp{{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:10px;
  font-family:'IBM Plex Mono',monospace;font-size:.76rem;color:var(--mute)}}
.pulse{{width:7px;height:7px;border-radius:50%;background:var(--up);
  box-shadow:0 0 0 0 rgba(63,182,139,.6);animation:p 2.4s infinite}}
@keyframes p{{70%{{box-shadow:0 0 0 9px rgba(63,182,139,0)}}100%{{box-shadow:0 0 0 0 rgba(63,182,139,0)}}}}

/* แถบราคาเลื่อนไปทางซ้ายต่อเนื่องแบบรายการทีวี — วิ่งตลอด ไม่หยุดตอนชี้เมาส์ */
.tickers{{display:flex;flex-direction:column;gap:8px;margin-bottom:16px}}
.ticker-row{{display:flex;align-items:stretch;gap:8px;min-width:0}}
.ticker-tag{{flex:none;display:flex;align-items:center;padding:0 12px;border-radius:10px;
  background:var(--panel2);border:1px solid var(--line);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.06em;
  text-transform:uppercase;color:var(--mute);white-space:nowrap}}
.ticker{{flex:1;min-width:0;overflow:hidden;border:1px solid var(--line);
  border-radius:10px;background:var(--panel)}}
.ticker-track{{display:flex;width:max-content;animation:marquee linear infinite}}
@keyframes marquee{{from{{transform:translateX(0)}}to{{transform:translateX(-50%)}}}}
@media(prefers-reduced-motion:reduce){{
  .ticker{{overflow-x:auto}}
  .ticker-track{{animation:none;width:auto}}
  .ticker-track .dup{{display:none}}
}}
/* การ์ดราคาแบบบรรทัดเดียว: ชื่อ · ราคา · ส่วนต่าง · % (อย่างแถบหุ้นทีวี) */
.tick{{flex:0 0 auto;padding:9px 16px;white-space:nowrap;
  border:0;border-right:1px solid var(--line);background:none;color:inherit;
  font-family:'IBM Plex Mono',monospace;font-size:.8rem;cursor:pointer;
  display:flex;align-items:baseline;gap:7px;transition:background .15s}}
.tick:hover{{background:rgba(255,255,255,.06)}}
.tick:focus-visible{{outline:2px solid var(--econ);outline-offset:-2px}}
.t-chg{{font-size:.76rem}}

/* ── หน้าต่างข่าวที่เกี่ยวข้องกับสินทรัพย์ ─────────────────── */
.tmodal{{position:fixed;inset:0;z-index:60;display:grid;place-items:center;padding:20px;
  background:rgba(4,6,11,.82)}}
.tmodal-box{{width:min(760px,100%);max-height:86vh;display:flex;flex-direction:column;
  background:var(--panel);border:1px solid var(--line);border-radius:14px;overflow:hidden;
  box-shadow:0 30px 80px rgba(0,0,0,.6)}}
.tmodal-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;
  padding:16px 18px 12px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.tmodal-head h3{{font-size:1.1rem;font-weight:700;letter-spacing:.02em}}
.tmodal-price{{display:flex;align-items:baseline;gap:9px;margin-top:3px;
  font-family:'IBM Plex Mono',monospace}}
#tmodal-p{{font-size:1rem}}
#tmodal-c{{font-size:.8rem}}
.tmodal-x{{flex:none;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1.2rem;
  line-height:1;color:var(--mute);background:transparent;border:1px solid var(--line)}}
.tmodal-x:hover{{color:var(--ink);background:rgba(255,255,255,.07)}}
.tmodal-note{{padding:10px 18px;font-size:.71rem;line-height:1.5;color:var(--dim);
  border-bottom:1px solid var(--line)}}
.tmodal-note strong{{color:var(--mute);font-weight:600}}
.tmodal-list{{overflow-y:auto;padding:4px 0}}
.trow{{display:flex;align-items:center;gap:11px;padding:11px 18px;
  border-bottom:1px solid var(--line)}}
.trow:last-child{{border-bottom:0}}
.trow:hover{{background:#151C2C}}
.trow-thumb{{width:58px;height:38px;border-radius:6px;object-fit:cover;flex:none;
  background:var(--panel2)}}
.trow-body{{flex:1;min-width:0}}
.trow-title{{font-size:.85rem;line-height:1.4;font-weight:500}}
.trow-meta{{display:flex;gap:8px;margin-top:4px;font-family:'IBM Plex Mono',monospace;
  font-size:.64rem;color:var(--dim);text-transform:uppercase}}
.score{{flex:none;min-width:52px;text-align:center;padding:5px 8px;border-radius:7px;
  font-family:'IBM Plex Mono',monospace;font-size:.78rem;font-weight:600}}
/* >50 เขียว · 10-49 เหลือง · <10 แดง */
.score.hi{{color:#8CF0C6;background:rgba(63,182,139,.16);border:1px solid rgba(63,182,139,.4)}}
.score.mid{{color:#FFD27A;background:rgba(245,165,36,.14);border:1px solid rgba(245,165,36,.38)}}
.score.low{{color:#FFA9AC;background:rgba(229,72,77,.14);border:1px solid rgba(229,72,77,.38)}}
.tmodal-empty{{padding:26px 18px;color:var(--mute);font-size:.85rem;text-align:center}}

/* ── หน้ากราฟแท่งเทียน ─────────────────────────────────── */
.chart-bar{{display:flex;align-items:center;gap:12px;margin-bottom:8px}}
.chart-open{{display:inline-flex;align-items:center;gap:8px;padding:8px 15px;border-radius:9px;
  font-family:inherit;font-size:.82rem;font-weight:600;cursor:pointer;color:var(--ink);
  background:linear-gradient(180deg,#1A2334,#141B29);border:1px solid #2A3548;
  transition:border-color .16s,background .16s}}
.chart-open:hover{{border-color:var(--brass);background:#1E2839}}
.chart-open svg{{width:15px;height:15px;fill:none;stroke:var(--brass);stroke-width:2;
  stroke-linecap:round}}
.chart-hint{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim)}}

#cmodal{{padding:0}}
.cmodal-box{{width:100%;height:100%;display:flex;flex-direction:column;
  background:var(--panel);border:0;border-radius:0;overflow:hidden}}
.cmodal-head{{display:flex;align-items:center;gap:14px;flex-wrap:wrap;
  padding:13px 16px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.cmodal-title{{min-width:150px}}
.cmodal-title h3{{font-size:1.05rem;font-weight:700}}
.cmodal-price{{display:flex;gap:9px;font-family:'IBM Plex Mono',monospace;font-size:.78rem}}
.tfbar{{display:flex;gap:4px;flex:1;flex-wrap:wrap}}
.tfbtn{{padding:5px 11px;border-radius:7px;cursor:pointer;font-family:'IBM Plex Mono',monospace;
  font-size:.72rem;color:var(--mute);background:transparent;border:1px solid var(--line)}}
.tfbtn:hover{{color:var(--ink)}}
.tfbtn.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:600}}
.cmodal-body{{flex:1;display:flex;min-height:0}}
.cmodal-list{{width:190px;flex:none;overflow-y:auto;border-right:1px solid var(--line);
  padding:6px 0}}
.cgroup{{padding:9px 14px 5px;font-family:'IBM Plex Mono',monospace;font-size:.62rem;
  letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}}
.citem{{display:flex;justify-content:space-between;gap:8px;width:100%;padding:7px 14px;
  cursor:pointer;background:none;border:0;color:var(--mute);font-family:inherit;
  font-size:.79rem;text-align:left}}
.citem:hover{{background:#151C2C;color:var(--ink)}}
.citem.on{{background:#182133;color:var(--ink);box-shadow:inset 2px 0 0 var(--brass)}}
.citem span:last-child{{font-family:'IBM Plex Mono',monospace;font-size:.7rem}}
.cmodal-chart{{flex:1;min-width:0;display:flex;flex-direction:column;padding:10px 14px 8px}}
#cchart{{flex:1;min-height:0}}
#cchart svg{{width:100%;height:100%;display:block;cursor:crosshair;touch-action:none}}
.c-grid line{{stroke:#1B2434;stroke-width:1;shape-rendering:crispEdges}}
.c-axis text{{fill:var(--dim);font-family:'IBM Plex Mono',monospace;font-size:10px}}
.c-up{{fill:var(--up);stroke:var(--up)}}
.c-down{{fill:var(--down);stroke:var(--down)}}
.c-cross{{stroke:#4E5A70;stroke-width:1;stroke-dasharray:3 3;pointer-events:none}}
.creadout{{min-height:19px;font-family:'IBM Plex Mono',monospace;font-size:.7rem;
  color:var(--mute);display:flex;gap:12px;flex-wrap:wrap}}
.creadout b{{color:var(--ink);font-weight:500}}
.cmodal-note{{font-size:.65rem;color:var(--dim);margin-top:3px}}
.cempty{{display:grid;place-items:center;height:100%;color:var(--mute);font-size:.85rem}}

/* แถบข่าวของสินทรัพย์นั้น อยู่ข้างกราฟ */
.cmodal-news{{width:310px;flex:none;display:flex;flex-direction:column;
  border-left:1px solid var(--line)}}
.cnews-head{{padding:10px 14px;border-bottom:1px solid var(--line);background:var(--panel2);
  font-size:.72rem;font-weight:600;letter-spacing:.05em;text-transform:uppercase;
  color:var(--mute)}}
.cnews-list{{flex:1;overflow-y:auto}}
.cnews-row{{display:flex;gap:9px;align-items:flex-start;padding:9px 13px;
  border-bottom:1px solid var(--line)}}
.cnews-row:hover{{background:#151C2C}}
.cnews-row .score{{min-width:44px;padding:3px 6px;font-size:.68rem}}
.cnews-t{{flex:1;min-width:0;font-size:.78rem;line-height:1.38}}
.cnews-m{{display:block;margin-top:3px;font-family:'IBM Plex Mono',monospace;
  font-size:.6rem;color:var(--dim);text-transform:uppercase}}
@media(max-width:1100px){{.cmodal-news{{width:260px}}}}
@media(max-width:860px){{
  .cmodal-body{{flex-direction:column;overflow-y:auto}}
  .cmodal-list{{width:auto;display:flex;overflow-x:auto;border-right:0;
    border-bottom:1px solid var(--line);padding:6px}}
  .cgroup{{display:none}}
  .citem{{width:auto;white-space:nowrap}}
  .cmodal-chart{{min-height:340px}}
  .cmodal-news{{width:auto;border-left:0;border-top:1px solid var(--line)}}
}}
.t-label{{color:var(--ink);font-weight:600;letter-spacing:.03em}}
.t-price{{color:var(--ink);font-weight:500}}
.t-pct{{font-size:.76rem}}
.up{{color:var(--up)}} .down{{color:var(--down)}} .flat{{color:var(--mute)}}

.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden}}
.panel-head{{display:flex;align-items:center;justify-content:space-between;
  padding:12px 15px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.panel-head h2{{display:flex;align-items:center;gap:8px;
  font-size:.8rem;font-weight:600;letter-spacing:.05em;text-transform:uppercase}}
.count{{font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--dim)}}

/* ไอคอนหมวดข่าว */
.cicon{{width:17px;height:17px;flex:none;fill:none;stroke:currentColor;
  stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}}
.cicon.ci-sm{{width:14px;height:14px;stroke-width:2.1}}
.ic-econ{{color:var(--econ)}} .ic-poli{{color:var(--poli)}}
.ic-biz{{color:var(--biz)}} .ic-env{{color:var(--env)}} .ic-mixed{{color:var(--mixed)}}

.row-panel{{margin-bottom:16px}}

.map-wrap{{position:relative;height:440px;
  background:radial-gradient(ellipse at 50% 45%,#101827 0%,#0B111C 70%)}}
#map{{width:100%;height:100%;display:block;cursor:grab;touch-action:none}}
#map:active{{cursor:grabbing}}
/* เส้นขอบไม่หนาขึ้นตอนซูม */
.sphere{{fill:#0C1220;stroke:#1A2333;stroke-width:.8;vector-effect:non-scaling-stroke}}
.grat{{fill:none;stroke:#141C2B;stroke-width:.45;vector-effect:non-scaling-stroke}}
.country{{fill:#172030;stroke:#232E42;stroke-width:.45;vector-effect:non-scaling-stroke}}
.zoom-ctl{{position:absolute;right:12px;top:12px;display:flex;flex-direction:column;gap:5px;z-index:5}}
.zoom-ctl button{{width:27px;height:27px;font-family:inherit;font-size:.95rem;line-height:1;
  color:var(--mute);background:rgba(10,14,26,.85);border:1px solid var(--line);
  border-radius:6px;cursor:pointer}}
.zoom-ctl button:hover{{color:var(--ink);border-color:var(--dim)}}
.mk{{cursor:pointer}}
.mk .halo{{opacity:.16;transition:opacity .15s}}
.mk .core{{stroke:#0A0E1A;stroke-width:1.2}}
.mk:hover .halo{{opacity:.34}}
.mk-label{{font-family:'IBM Plex Sans Thai',sans-serif;font-size:10px;
  fill:var(--mute);pointer-events:none}}
#tip{{position:absolute;pointer-events:none;opacity:0;transition:opacity .12s;
  background:rgba(10,14,26,.95);border:1px solid var(--line);border-radius:7px;
  padding:7px 10px;font-size:.74rem;display:flex;flex-direction:column;gap:2px;z-index:5}}
#tip span{{color:var(--mute);font-family:'IBM Plex Mono',monospace;font-size:.66rem}}
.legend{{position:absolute;left:14px;bottom:12px;display:flex;flex-wrap:wrap;gap:6px 14px;
  font-size:.68rem;color:var(--mute);background:rgba(10,14,26,.8);
  border:1px solid var(--line);border-radius:7px;padding:6px 11px}}
.legend span{{display:flex;align-items:center;gap:5px}}

#hotspot-detail{{border-top:1px solid var(--line);max-height:172px;overflow-y:auto}}
.hd-top{{display:flex;justify-content:space-between;padding:10px 15px 6px;align-items:baseline}}
.hd-top h4{{font-size:.85rem;font-weight:600}}
.hd-top span{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hd-row{{display:flex;align-items:center;gap:9px;
  padding:8px 15px;border-top:1px solid var(--line);font-size:.8rem}}
.hd-row:hover{{background:#151C2C}}
.hd-row > span:nth-last-child(2){{flex:1;min-width:0}}
.hd-thumb{{width:36px;height:36px;border-radius:6px;object-fit:cover;flex:none;background:var(--panel2)}}
.hd-age{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--dim);white-space:nowrap}}

/* ข่าวล่าสุดอยู่เต็มความกว้างเหนือแผนที่ → จัดเป็นหลายคอลัมน์ */
.feed{{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr))}}
.feed-row{{display:flex;align-items:center;gap:10px;min-width:0;
  padding:9px 15px;border-bottom:1px solid var(--line);transition:background .12s}}
.feed-row:hover{{background:#151C2C}}
.feed-thumb{{width:44px;height:44px;border-radius:7px;object-fit:cover;flex:none;background:var(--panel2)}}
.feed-title{{flex:1;min-width:0;font-size:.82rem;line-height:1.4}}
.feed-age{{flex:none;font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim);white-space:nowrap}}

.hot{{display:grid;grid-template-columns:18px 1fr 62px 26px;gap:9px;align-items:center;
  padding:7px 15px;border-bottom:1px solid var(--line);font-size:.8rem}}
.hot:last-child{{border-bottom:0}}
.rank{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hot-bars{{display:flex;height:5px;border-radius:3px;overflow:hidden;background:#1A2333}}
.hot-bars .hb-econ{{background:var(--econ)}} .hot-bars .hb-poli{{background:var(--poli)}}
.hot-bars .hb-biz{{background:var(--biz)}} .hot-bars .hb-env{{background:var(--env)}}
.hot-n{{font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--mute);text-align:right}}

.search-wrap{{padding:11px 15px;border-bottom:1px solid var(--line)}}
.search{{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:8px;
  color:var(--ink);font-family:inherit;font-size:.82rem;padding:8px 12px}}
.search::placeholder{{color:var(--dim)}}
.search:focus{{outline:none;border-color:var(--econ)}}

.grid-side{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:700px){{.grid-side{{grid-template-columns:1fr}}}}

/* ── เรื่องเด่น (billboard) ───────────────────────────── */
.hero{{position:relative;display:flex;align-items:flex-end;overflow:hidden;
  border-radius:14px;margin-bottom:30px;min-height:clamp(320px,44vw,510px)}}
.hero-img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}}
.hero-scrim{{position:absolute;inset:0;
  background:linear-gradient(90deg,rgba(5,7,13,.95) 0%,rgba(5,7,13,.78) 40%,rgba(5,7,13,.28) 74%),
             linear-gradient(0deg,rgba(5,7,13,.96) 0%,rgba(5,7,13,0) 58%)}}
.hero-body{{position:relative;padding:clamp(18px,3vw,40px);max-width:780px}}
.hero-badge{{display:flex;align-items:center;flex-wrap:wrap;gap:7px;
  font-family:'IBM Plex Mono',monospace;font-size:.71rem;color:var(--mute)}}
.hero-sep{{color:var(--dim)}}
.hero-title{{font-size:clamp(1.35rem,3.1vw,2.55rem);font-weight:700;line-height:1.18;
  margin:11px 0 12px;text-shadow:0 2px 20px rgba(0,0,0,.65)}}
.hero-sum{{color:#C2CCDD;font-size:.92rem;line-height:1.6;max-width:640px;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}}
.hero-actions{{display:flex;flex-wrap:wrap;gap:10px;margin-top:17px}}
.btn{{display:inline-flex;align-items:center;gap:7px;padding:9px 19px;border-radius:7px;
  font-family:inherit;font-size:.86rem;font-weight:600;cursor:pointer;
  border:1px solid transparent;transition:background .16s}}
.btn-main{{background:#fff;color:#0A0E1A}}
.btn-main:hover{{background:#D7E0EF}}
.btn-ghost{{background:rgba(175,192,220,.2);color:var(--ink);border-color:rgba(255,255,255,.15)}}
.btn-ghost:hover{{background:rgba(175,192,220,.33)}}

/* ── แถวข่าวแบบเลื่อนแนวนอน ───────────────────────────── */
.row{{margin-bottom:28px}}
.row-head{{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:11px}}
.row-head h2{{display:flex;align-items:center;gap:9px;font-size:1.05rem;font-weight:700}}
.row-n{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim);font-weight:400}}
.live{{font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.08em;
  color:#fff;background:var(--down);border-radius:4px;padding:2px 6px}}
.row-tools{{display:flex;align-items:center;gap:7px}}
.row-tools .search{{width:150px}}
.row-nav{{width:30px;height:30px;flex:none;border-radius:50%;cursor:pointer;font-size:1.05rem;
  line-height:1;color:var(--ink);background:rgba(255,255,255,.06);border:1px solid var(--line)}}
.row-nav:hover{{background:rgba(255,255,255,.15)}}
/* padding + margin ติดลบ เพื่อให้การ์ดที่ขยายตอน hover ไม่โดนตัดขอบ */
.row-track{{display:flex;gap:12px;overflow-x:auto;scroll-behavior:smooth;
  scroll-snap-type:x proximity;padding:24px 4px 28px;margin:-24px -4px -28px}}
.row-track::-webkit-scrollbar{{height:0}}
.row-empty{{color:var(--mute);font-size:.82rem;padding:22px 4px}}

.poster{{position:relative;flex:0 0 288px;scroll-snap-align:start;background:var(--panel);
  border:1px solid var(--line);border-radius:10px;overflow:hidden;
  transition:transform .28s cubic-bezier(.2,.7,.3,1),box-shadow .28s,border-color .28s}}
.poster:hover{{transform:scale(1.07);z-index:3;border-color:#31415C;
  box-shadow:0 18px 42px rgba(0,0,0,.62)}}
.poster.hidden{{display:none}}
.poster-img{{position:relative;display:grid;place-items:center;aspect-ratio:16/9}}
.poster-img img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}}
.poster-img .ci-lg{{width:42px;height:42px;opacity:.45}}
.poster-cat{{position:absolute;left:9px;top:9px;z-index:2;display:grid;place-items:center;
  width:24px;height:24px;border-radius:6px;background:rgba(5,7,13,.72)}}
.poster-speak{{position:absolute;right:9px;top:9px;z-index:2;width:26px;height:26px;padding:0;
  border-radius:6px;background:rgba(5,7,13,.72);opacity:0;transition:opacity .2s}}
.poster:hover .poster-speak,.poster-speak:focus{{opacity:1}}
.poster-body{{padding:11px 12px 13px}}
.poster-body h3{{font-size:.89rem;font-weight:600;line-height:1.42;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}}
.poster-meta{{display:flex;justify-content:space-between;gap:8px;margin-top:8px}}
/* พื้นไล่สีตามหมวด ใช้ตอนข่าวไม่มีรูป */
.pf-econ{{background:linear-gradient(135deg,rgba(76,141,255,.34),rgba(76,141,255,.05))}}
.pf-poli{{background:linear-gradient(135deg,rgba(245,165,36,.34),rgba(245,165,36,.05))}}
.pf-biz{{background:linear-gradient(135deg,rgba(45,212,191,.34),rgba(45,212,191,.05))}}
.pf-env{{background:linear-gradient(135deg,rgba(74,222,128,.34),rgba(74,222,128,.05))}}
.pf-mixed{{background:linear-gradient(135deg,rgba(155,138,251,.34),rgba(155,138,251,.05))}}

.src{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--mute);
  text-transform:uppercase;letter-spacing:.05em}}
.loc{{margin-left:6px;padding:1px 5px;border:1px solid var(--line);border-radius:4px;
  color:var(--dim);text-transform:none;letter-spacing:0}}
.age{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--dim);white-space:nowrap}}
.speak{{font-family:inherit;font-size:.7rem;color:var(--mute);background:var(--panel2);
  border:1px solid var(--line);border-radius:6px;padding:4px 9px;cursor:pointer}}
.speak:hover{{color:var(--ink);border-color:var(--dim)}}
.speak.playing{{color:var(--econ);border-color:var(--econ)}}

/* ── อินโทรตอนเข้าเว็บ ─────────────────────────────────── */
#intro{{position:fixed;inset:0;z-index:99;background:#05070D;display:grid;place-items:center;
  animation:introFade .6s 1.95s forwards}}
.intro-inner{{display:flex;flex-direction:column;align-items:center;gap:14px;
  animation:introIn 1.05s cubic-bezier(.2,.7,.3,1) backwards,
            introZoom .8s 1.72s ease-in forwards}}
.intro-mark{{font-family:'Playfair Display',Georgia,'Times New Roman',serif;font-weight:700;
  font-size:clamp(3rem,12vw,5.6rem);line-height:.94;letter-spacing:.17em;text-indent:.17em;
  color:var(--cream);
  text-shadow:0 0 40px rgba(198,169,97,.42),0 0 110px rgba(198,169,97,.18)}}
.intro-rule{{width:min(300px,58vw);height:1px;
  background:linear-gradient(90deg,transparent,var(--brass) 24%,var(--brass) 76%,transparent)}}
.intro-inner b{{font-size:clamp(.58rem,2vw,.8rem);letter-spacing:.44em;text-indent:.44em;
  font-weight:500;text-transform:uppercase;color:var(--brass)}}
@keyframes introIn{{from{{opacity:0;transform:scale(.84);filter:blur(13px)}}}}
@keyframes introZoom{{to{{opacity:0;transform:scale(1.55)}}}}
@keyframes introFade{{to{{opacity:0;visibility:hidden}}}}
@keyframes pageIn{{from{{opacity:0;transform:translateY(15px)}}}}
.no-intro #intro{{display:none}}
.no-intro body{{animation:none}}

/* ── แท็บ ไทย / ต่างประเทศ ─────────────────────────────── */
.tabs{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:18px;
  border-bottom:1px solid var(--line);padding-bottom:2px}}
.tab{{position:relative;font-family:inherit;font-size:.88rem;font-weight:600;cursor:pointer;
  color:var(--mute);background:none;border:0;padding:9px 16px 11px;border-radius:8px 8px 0 0;
  transition:color .16s,background .16s}}
.tab:hover{{color:var(--ink);background:rgba(255,255,255,.05)}}
.tab.active{{color:var(--ink)}}
.tab.active::after{{content:"";position:absolute;left:12px;right:12px;bottom:-3px;height:3px;
  border-radius:3px 3px 0 0;background:linear-gradient(90deg,var(--econ),var(--poli))}}
.tab-n{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;font-weight:400;
  color:var(--dim);margin-left:6px}}
.scope-title{{display:flex;align-items:center;gap:10px;font-size:1.18rem;font-weight:700;
  margin:6px 0 14px}}
.scope-title::after{{content:"";flex:1;height:1px;background:var(--line)}}
.scope-flag{{font-size:.66rem;font-weight:500;letter-spacing:.08em;color:var(--dim);
  font-family:'IBM Plex Mono',monospace;border:1px solid var(--line);
  border-radius:5px;padding:2px 7px}}
[hidden]{{display:none!important}}

.kws{{display:flex;flex-wrap:wrap;gap:7px 12px;padding:15px;align-items:baseline}}
.kw{{font-weight:500;line-height:1.2}}

footer{{margin-top:18px;padding-top:14px;border-top:1px solid var(--line);
  display:flex;justify-content:space-between;flex-wrap:wrap;gap:12px;
  font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--dim)}}
::-webkit-scrollbar{{width:8px;height:8px}}
::-webkit-scrollbar-track{{background:transparent}}
::-webkit-scrollbar-thumb{{background:#222B3D;border-radius:4px}}
@media(prefers-reduced-motion:reduce){{
  *{{animation:none!important;transition:none!important}}
  #intro{{display:none!important}}   /* กัน overlay ค้างเมื่ออนิเมชันถูกปิด */
  .row-track{{scroll-behavior:auto}}
}}
</style>
</head>
<body>

<div id="intro" aria-hidden="true"><div class="intro-inner">
  <span class="intro-mark">EPM</span><span class="intro-rule"></span>
  <b>Economics · Politics · Monitor</b></div></div>

<div id="tmodal" class="tmodal" hidden>
  <div class="tmodal-box" role="dialog" aria-modal="true" aria-labelledby="tmodal-name">
    <div class="tmodal-head">
      <div>
        <h3 id="tmodal-name"></h3>
        <div class="tmodal-price"><span id="tmodal-p"></span><span id="tmodal-c"></span></div>
      </div>
      <button type="button" class="tmodal-x" onclick="closeTicker()" aria-label="ปิด">×</button>
    </div>
    <p class="tmodal-note">เรียงตาม<strong>ความเกี่ยวข้องของเนื้อหา</strong>กับสินทรัพย์นี้
      (จับจากคำในหัวข้อ/เนื้อข่าว) — ไม่ใช่การวัดว่าข่าวทำให้ราคาขยับกี่เปอร์เซ็นต์</p>
    <div class="tmodal-list" id="tmodal-list"></div>
  </div>
</div>

<header>
  <h1 class="logo">
    <span class="logo-mark">EPM</span>
    <span class="logo-rule"></span>
    <span class="logo-sub">Economics · Politics · Monitor</span>
  </h1>
  <div class="stamp">
    <span class="pulse"></span>
    <span>อัปเดต {NOW.strftime('%d %b %Y · %H:%M')} น.</span>
    <span style="color:var(--dim)">· รอบถัดไป {next_run} น.</span>
  </div>
</header>

<div id="cmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="กราฟราคา">
    <div class="cmodal-head">
      <div class="cmodal-title">
        <h3 id="cmodal-name">—</h3>
        <div class="cmodal-price"><span id="cmodal-p"></span><span id="cmodal-c"></span></div>
      </div>
      <div class="tfbar" id="cmodal-tf"></div>
      <button type="button" class="tmodal-x" onclick="closeCharts()" aria-label="ปิด">×</button>
    </div>
    <div class="cmodal-body">
      <div class="cmodal-list" id="cmodal-list"></div>
      <div class="cmodal-chart">
        <div id="cchart"></div>
        <div id="creadout" class="creadout"></div>
        <p class="cmodal-note">ล้อเมาส์/นิ้วเพื่อซูม · ลากเพื่อเลื่อน · ดับเบิลคลิกเพื่อรีเซ็ต</p>
      </div>
      <div class="cmodal-news">
        <div class="cnews-head">ข่าวที่เกี่ยวข้อง</div>
        <div class="cnews-list" id="cnews-list"></div>
      </div>
    </div>
  </div>
</div>

<nav class="tabs" role="tablist">
  <button class="tab active" type="button" role="tab" data-scope="all" onclick="setScope('all')">ทั้งหมด<span class="tab-n">{len(news)}</span></button>
  {''.join(f'''<button class="tab" type="button" role="tab" data-scope="{sc}" onclick="setScope('{sc}')">{lb}<span class="tab-n">{groups[sc]["n"]}</span></button>''' for sc, lb in SCOPES)}
</nav>

{heroes}

{latest_blocks}

<section class="panel row-panel">
  <div class="panel-head">
    <h2>แผนที่ข่าว</h2>
    <span class="count">{len(markers)} พื้นที่ · คลิกจุด · ซูมได้</span>
  </div>
  <div class="map-wrap">
    <svg id="map"></svg>
    <div id="tip"></div>
    <div class="zoom-ctl">
      <button type="button" onclick="zoomBy(1.6)" aria-label="ซูมเข้า">+</button>
      <button type="button" onclick="zoomBy(1/1.6)" aria-label="ซูมออก">−</button>
      <button type="button" onclick="zoomReset()" aria-label="รีเซ็ตแผนที่">⟲</button>
    </div>
    <div class="legend">
      {''.join(f'<span>{cat_icon(c, "ci-sm")}{CAT_LABELS[c]}</span>' for c in CAT_NAMES + ["mixed"])}
    </div>
  </div>
  <div id="hotspot-detail"></div>
</section>

<div class="chart-bar">
  <button class="chart-open" type="button" onclick="openCharts()">
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg>
    กราฟราคา
  </button>
  <span class="chart-hint">แท่งเทียน · 1D–5Y · ซูม/เลื่อนได้</span>
</div>

<div class="tickers">
  {ticker_row("th", "ไทย")}
  {ticker_row("intl", "ต่างประเทศ")}
</div>

{category_blocks}

<div class="grid-side">
  <section class="panel">
    <div class="panel-head"><h2>พื้นที่ที่มีข่าวมากสุด</h2></div>
    <div>{''.join(hot_row(m, i) for i, m in enumerate(markers[:10])) or '<div class="hot"><span></span><span>—</span></div>'}</div>
  </section>
  <section class="panel">
    <div class="panel-head"><h2>คำที่พูดถึงมากสุด</h2></div>
    <div class="kws">{''.join(kw_chip(w, f) for w, f in kws)}</div>
  </section>
</div>

<footer>
  <span>{len(FEEDS)} แหล่งข่าว · {len(news)} ข่าวใน 24 ชม. · ระบุพิกัดได้ {located} ข่าว</span>
  <span>รีเฟรชอัตโนมัติทุก 15 นาที · ดึงข้อมูลใหม่ทุก 3 ชม.</span>
</footer>

<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
<script>window.__MARKERS__ = {markers_json}; window.__ICONS__ = {icons_json};
window.__TNEWS__ = {tnews_json}; window.__CHARTS__ = {charts_json};</script>
<script>{MAP_JS}</script>
<script>
function filterItems(input){{
  const q = input.value.trim().toLowerCase();
  input.closest('section').querySelectorAll('.poster').forEach(it => {{
    const hit = !q || it.textContent.toLowerCase().includes(q);
    it.classList.toggle('hidden', !hit);
  }});
}}

function scrollRow(id, dir){{
  const el = document.getElementById(id);
  if (el) el.scrollBy({{ left: dir * Math.max(300, el.clientWidth * 0.8), behavior: 'smooth' }});
}}

// ── ข่าวที่เกี่ยวข้องกับสินทรัพย์ในแถบราคา ──────────────────
const TNEWS = window.__TNEWS__ || {{}};
const esc = s => String(s).replace(/[&<>"']/g, c =>
  ({{ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }}[c]));
const scoreClass = s => s > 50 ? 'hi' : (s >= 10 ? 'mid' : 'low');

function openTicker(label){{
  const d = TNEWS[label];
  if (!d) return;
  document.getElementById('tmodal-name').textContent = label;
  document.getElementById('tmodal-p').textContent = d.price;
  const c = document.getElementById('tmodal-c');
  c.textContent = d.pct;
  c.className = d.dir;
  document.getElementById('tmodal-list').innerHTML = d.news.length
    ? d.news.map(n => `<a class="trow" href="${{esc(n.link)}}" target="_blank" rel="noopener">
        ${{n.image ? `<img class="trow-thumb" src="${{esc(n.image)}}" loading="lazy" alt="" onerror="this.remove()">` : ''}}
        <span class="trow-body">
          <span class="trow-title">${{esc(n.title)}}</span>
          <span class="trow-meta"><span>${{esc(n.source)}}</span><span>${{esc(n.age)}}</span></span>
        </span>
        <span class="score ${{scoreClass(n.score)}}">${{n.score}}%</span>
      </a>`).join('')
    : '<p class="tmodal-empty">รอบนี้ยังไม่มีข่าวที่เกี่ยวข้องกับสินทรัพย์นี้</p>';
  document.getElementById('tmodal').hidden = false;
  document.body.style.overflow = 'hidden';
}}

function closeTicker(){{
  document.getElementById('tmodal').hidden = true;
  document.body.style.overflow = '';
}}

document.querySelector('.tickers').addEventListener('click', ev => {{
  const t = ev.target.closest('.tick');
  if (t) openTicker(t.dataset.label);
}});
document.getElementById('tmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'tmodal') closeTicker();      // คลิกพื้นหลังเพื่อปิด
}});
addEventListener('keydown', ev => {{
  if (ev.key === 'Escape' && !document.getElementById('tmodal').hidden) closeTicker();
}});

// ── กราฟแท่งเทียน ────────────────────────────────────────
const CHARTS = window.__CHARTS__ || {{}};
const CH_TF = ['1D','1M','3M','6M','1Y','3Y','5Y'];
let chCur = null, chTf = '3M', chCache = {{}}, chData = null, chZoom = null;

function openCharts(){{
  const modal = document.getElementById('cmodal');
  if (!Object.keys(CHARTS).length) return;
  if (!document.getElementById('cmodal-list').childElementCount) {{
    const groups = {{th: 'ไทย', intl: 'ต่างประเทศ'}};
    let html = '';
    for (const [g, title] of Object.entries(groups)) {{
      const rows = Object.entries(TNEWS)
        .filter(([l]) => CHARTS[l] && (TNEWS[l].group || 'intl') === g)
        .sort((a, b) => (b[1].pctv ?? 0) - (a[1].pctv ?? 0));   // บวกมากสุดอยู่บน
      if (!rows.length) continue;
      html += `<div class="cgroup">${{title}}</div>` + rows.map(([l, d]) =>
        `<button class="citem" type="button" data-label="${{esc(l)}}" onclick="pickChart('${{esc(l)}}')">
           <span>${{esc(l)}}</span><span class="${{d.dir}}">${{d.pct}}</span></button>`).join('');
    }}
    document.getElementById('cmodal-list').innerHTML = html;
    document.getElementById('cmodal-tf').innerHTML = CH_TF.map(t =>
      `<button class="tfbtn" type="button" data-tf="${{t}}" onclick="pickTf('${{t}}')">${{t}}</button>`).join('');
  }}
  modal.hidden = false;
  document.body.style.overflow = 'hidden';
  pickChart(chCur || Object.keys(CHARTS)[0]);
}}

function closeCharts(){{
  document.getElementById('cmodal').hidden = true;
  document.body.style.overflow = '';
}}

function pickTf(tf){{ chTf = tf; renderChart(); }}

async function pickChart(label){{
  if (!CHARTS[label]) return;
  chCur = label;
  document.querySelectorAll('.citem').forEach(b =>
    b.classList.toggle('on', b.dataset.label === label));
  const d = TNEWS[label];
  document.getElementById('cmodal-name').textContent = label;
  if (d) {{
    document.getElementById('cmodal-p').textContent = d.price;
    const c = document.getElementById('cmodal-c');
    c.textContent = d.pct; c.className = d.dir;
  }}
  document.getElementById('cnews-list').innerHTML = (d?.news || []).length
    ? d.news.map(n => `<a class="cnews-row" href="${{esc(n.link)}}" target="_blank" rel="noopener">
        <span class="score ${{scoreClass(n.score)}}">${{n.score}}%</span>
        <span class="cnews-t">${{esc(n.title)}}
          <span class="cnews-m">${{esc(n.source)}} · ${{esc(n.age)}}</span></span></a>`).join('')
    : '<p class="tmodal-empty">ยังไม่มีข่าวที่เกี่ยวข้อง</p>';
  if (!chCache[label]) {{
    document.getElementById('cchart').innerHTML = '<div class="cempty">กำลังโหลด…</div>';
    try {{
      chCache[label] = await fetch('{CHART_DIR}/' + CHARTS[label] + '.json').then(r => r.json());
    }} catch (e) {{ chCache[label] = {{tf: {{}}}}; }}
  }}
  chData = chCache[label];
  renderChart();
}}

function renderChart(){{
  const host = document.getElementById('cchart');
  const avail = CH_TF.filter(t => (chData?.tf || {{}})[t]?.length);
  if (!avail.length) {{ host.innerHTML = '<div class="cempty">ไม่มีข้อมูลกราฟ</div>'; return; }}
  if (!avail.includes(chTf)) chTf = avail.includes('3M') ? '3M' : avail[0];
  document.querySelectorAll('.tfbtn').forEach(b => {{
    b.classList.toggle('on', b.dataset.tf === chTf);
    b.disabled = !avail.includes(b.dataset.tf);
    b.style.opacity = avail.includes(b.dataset.tf) ? '' : '.35';
  }});

  const rows = chData.tf[chTf];
  const W = host.clientWidth || 700, H = host.clientHeight || 360;
  const m = {{t: 10, r: 56, b: 22, l: 8}};
  const iw = Math.max(50, W - m.l - m.r), ih = Math.max(50, H - m.t - m.b);
  host.innerHTML = '';
  const svg = d3.select(host).append('svg').attr('viewBox', `0 0 ${{W}} ${{H}}`);
  const root = svg.append('g').attr('transform', `translate(${{m.l}},${{m.t}})`);
  root.append('clipPath').attr('id', 'cclip').append('rect')
      .attr('width', iw).attr('height', ih);

  const x0 = d3.scaleLinear().domain([-0.6, rows.length - 0.4]).range([0, iw]);
  const y = d3.scaleLinear().range([ih, 0]);
  const gGrid = root.append('g').attr('class', 'c-grid');
  const gY = root.append('g').attr('class', 'c-axis').attr('transform', `translate(${{iw}},0)`);
  const gX = root.append('g').attr('class', 'c-axis').attr('transform', `translate(0,${{ih}})`);
  const gC = root.append('g').attr('clip-path', 'url(#cclip)');
  const cross = root.append('line').attr('class', 'c-cross').attr('y1', 0).attr('y2', ih)
      .style('display', 'none');

  // เวลาแบบตัวเลขดัชนี ไม่ใช่เวลาจริง เพื่อไม่ให้มีช่องว่างวันหยุด
  const fmtT = ts => {{
    const dt = new Date(ts * 1000);
    return chTf === '1D'
      ? dt.toLocaleTimeString('th-TH', {{hour: '2-digit', minute: '2-digit'}})
      : dt.toLocaleDateString('th-TH', {{day: '2-digit', month: 'short',
          year: ['3Y', '5Y', '1Y'].includes(chTf) ? '2-digit' : undefined}});
  }};

  function draw(t){{
    const zx = t.rescaleX(x0);
    const i0 = Math.max(0, Math.floor(zx.invert(0)));
    const i1 = Math.min(rows.length - 1, Math.ceil(zx.invert(iw)));
    const vis = rows.slice(i0, i1 + 1);
    if (!vis.length) return;
    const lo = d3.min(vis, d => d[3]), hi = d3.max(vis, d => d[2]);
    const pad = (hi - lo) * 0.08 || Math.abs(hi) * 0.02 || 1;
    y.domain([lo - pad, hi + pad]);

    const ticks = y.ticks(6);
    gGrid.selectAll('line').data(ticks).join('line')
      .attr('x1', 0).attr('x2', iw).attr('y1', y).attr('y2', y);
    gY.selectAll('text').data(ticks).join('text')
      .attr('x', 6).attr('y', y).attr('dy', '.32em')
      .text(d => d3.format(d >= 1000 ? ',.0f' : ',.2f')(d));

    const step = Math.max(1, Math.round((i1 - i0) / 6));
    const xt = [];
    for (let i = i0; i <= i1; i += step) xt.push(i);
    gX.selectAll('text').data(xt).join('text')
      .attr('x', d => zx(d)).attr('y', 15).attr('text-anchor', 'middle')
      .text(d => fmtT(rows[d][0]));

    const bw = Math.max(1, Math.min(18, (zx(1) - zx(0)) * 0.68));
    const g = gC.selectAll('g.cd').data(vis, d => d[0]).join(
      en => {{ const s = en.append('g').attr('class', 'cd');
               s.append('line'); s.append('rect'); return s; }});
    g.attr('class', d => 'cd ' + (d[4] >= d[1] ? 'c-up' : 'c-down'))
     .attr('transform', (d, k) => `translate(${{zx(i0 + k)}},0)`);
    g.select('line').attr('x1', 0).attr('x2', 0)
      .attr('y1', d => y(d[2])).attr('y2', d => y(d[3])).attr('stroke-width', 1);
    g.select('rect')
      .attr('x', -bw / 2).attr('width', bw)
      .attr('y', d => y(Math.max(d[1], d[4])))
      .attr('height', d => Math.max(1, Math.abs(y(d[1]) - y(d[4]))));
    svg.node().__view = {{zx, i0, i1}};
  }}

  chZoom = d3.zoom().scaleExtent([1, 30])
    .translateExtent([[0, 0], [iw, ih]]).extent([[0, 0], [iw, ih]])
    .on('zoom', ev => draw(ev.transform));
  svg.call(chZoom).on('dblclick.zoom', null);
  svg.on('dblclick', () => svg.call(chZoom.transform, d3.zoomIdentity));
  draw(d3.zoomTransform(svg.node()));

  const out = document.getElementById('creadout');
  svg.on('mousemove', ev => {{
    const v = svg.node().__view; if (!v) return;
    const px = d3.pointer(ev, root.node())[0];
    const i = Math.round(v.zx.invert(px));
    const r = rows[Math.max(v.i0, Math.min(v.i1, i))];
    if (!r) return;
    cross.style('display', null).attr('x1', v.zx(rows.indexOf(r))).attr('x2', v.zx(rows.indexOf(r)));
    const f = n => d3.format(n >= 1000 ? ',.0f' : ',.2f')(n);
    out.innerHTML = `<span>${{fmtT(r[0])}}</span><span>เปิด <b>${{f(r[1])}}</b></span>` +
      `<span>สูง <b>${{f(r[2])}}</b></span><span>ต่ำ <b>${{f(r[3])}}</b></span>` +
      `<span>ปิด <b>${{f(r[4])}}</b></span>`;
  }}).on('mouseleave', () => {{ cross.style('display', 'none'); out.innerHTML = ''; }});
}}

document.getElementById('cmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'cmodal') closeCharts();
}});
addEventListener('keydown', ev => {{
  if (ev.key === 'Escape' && !document.getElementById('cmodal').hidden) closeCharts();
}});

function setScope(s){{
  document.querySelectorAll('.tab').forEach(t =>
    t.classList.toggle('active', t.dataset.scope === s));
  document.querySelectorAll('.scope-group').forEach(g =>
    g.hidden = !(s === 'all' || g.dataset.scope === s));
  // แท็บ "ทั้งหมด" โชว์เรื่องเด่นอันเดียว (ข่าวใหม่สุด) ไม่ใช่ทั้งสองฝั่ง
  document.querySelectorAll('.hero').forEach(h =>
    h.hidden = !(s === 'all' ? h.dataset.primary === '1' : h.dataset.scope === s));
  try {{ sessionStorage.setItem('scope', s); }} catch(e) {{}}
}}

// จำแท็บที่เลือกไว้ ไม่ให้เด้งกลับตอนหน้ารีเฟรชอัตโนมัติ
(() => {{
  let s = 'all';
  try {{ s = sessionStorage.getItem('scope') || 'all'; }} catch(e) {{}}
  if (!document.querySelector(`.tab[data-scope="${{s}}"]`)) s = 'all';
  setScope(s);
}})();

// เอา overlay อินโทรออกจาก DOM หลังเล่นจบ
(() => {{
  const intro = document.getElementById('intro');
  if (!intro) return;
  if (document.documentElement.classList.contains('no-intro')) {{ intro.remove(); return; }}
  setTimeout(() => intro.remove(), 2700);
}})();

if ('speechSynthesis' in window) {{
  let currentBtn = null;
  const rest = b => {{ b.classList.remove('playing'); b.textContent = b.dataset.label; }};
  document.addEventListener('click', ev => {{
    const btn = ev.target.closest('.speak');
    if (!btn) return;
    if (btn.dataset.label === undefined) btn.dataset.label = btn.textContent;
    const wasPlaying = btn.classList.contains('playing');
    speechSynthesis.cancel();
    if (currentBtn) rest(currentBtn);
    currentBtn = null;
    if (wasPlaying) return;
    const u = new SpeechSynthesisUtterance(btn.dataset.text);
    u.lang = btn.dataset.lang;
    u.onend = u.onerror = () => {{ rest(btn); currentBtn = null; }};
    btn.classList.add('playing');
    btn.textContent = btn.classList.contains('poster-speak') ? '⏸' : '⏸ กำลังอ่าน…';
    currentBtn = btn;
    speechSynthesis.speak(u);
  }});
}} else {{
  document.querySelectorAll('.speak').forEach(b => b.style.display = 'none');
}}

if ('serviceWorker' in navigator) {{
  addEventListener('load', () => navigator.serviceWorker.register('sw.js'));
}}
</script>
</body>
</html>"""


if __name__ == "__main__":
    print("ดึงข่าว...")
    news = fetch_news()
    print(f"→ ได้ {len(news)} ข่าว (ระบุพิกัดได้ {sum(1 for i in news if i['place'])})")

    if news:
        print("เติมรูปให้ข่าวที่ RSS ไม่ส่งรูปมา...")
        enrich_images(news)
    print(f"→ มีรูปประกอบ {sum(1 for i in news if i.get('image'))}/{len(news)} ข่าว\n")

    print("ดึงราคาตลาด...")
    markets = fetch_markets()
    print()

    cache = load_json(CACHE_FILE)
    if not news:
        print("  ⚠ ดึงข่าวไม่ได้เลยรอบนี้ ใช้ cache รอบก่อนแทน")
        news = news_from_cache(cache)
    if not markets:
        print("  ⚠ ดึงราคาตลาดไม่ได้เลยรอบนี้ ใช้ cache รอบก่อนแทน")
        markets = cache.get("markets", [])

    # แถบราคาไม่มีกราฟแล้ว แต่ยังเก็บราคาย้อนหลังไว้ต่อเนื่อง เผื่อนำกลับมาใช้ภายหลัง
    if markets:
        update_history(markets)
    if markets and news:
        attach_ticker_news(markets, news)
        linked = sum(len(m.get("news") or []) for m in markets)
        print(f"จับคู่ข่าวกับสินทรัพย์ได้ {linked} รายการ")
    save_cache(news, markets)

    charts = {}
    if markets:
        print("ดึงข้อมูลแท่งเทียน...")
        charts = build_charts(markets)
    print()

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(render(news, markets, charts))
    print(f"เสร็จ · index.html · {NOW.strftime('%H:%M')} น.")
