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
import bisect
import calendar
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
LIVE_WINDOW_MIN = 90   # ข่าวที่ถือว่า "สด ณ ตอนนี้" ต้องมีเวลาเผยแพร่จริงและใหม่กว่านี้
REBUILD_MIN = 30       # ต้องตรงกับ cron ใน .github/workflows/update.yml
CHART_FULL_HOURS = 6   # ดึงแท่งเทียนชุดเต็มทุกกี่ชั่วโมง (รอบอื่นอัปเดตแค่กราฟรายวัน)
# ขยับเลขนี้ทุกครั้งที่เพิ่ม/เปลี่ยนช่วงเวลาใน CHART_RANGES เพื่อทิ้ง cache รุ่นเก่า —
# ความสดอย่างเดียวจับไม่ได้ว่า cache "ครบชุดไหม" (ตอนเพิ่ม 10Y เจอปัญหานี้กับงบการเงินมาแล้ว)
CHART_SCHEMA = 7      # 2=10Y · 3=ชื่อเต็ม · 4=ล้าง prefix ชื่อหุ้นไทย · 5=เก็บประวัติปันผล
                      # 6=ธง "d" ในดัชนี บอกว่ามีปันผล (ให้ ETF อย่าง JEPQ ที่ไม่มีงบเข้าเมนูได้)
                      # 7=ช่อง "y" อัตราปันผล TTM ไว้เรียงลำดับในแถบรายการโปรด
PER_CATEGORY = 18
PER_ROW = 14          # จำนวนการ์ดต่อแถว (แยกไทย/ต่างประเทศแล้วจึงลดลงจาก PER_CATEGORY)
CACHE_FILE = "cache.json"
LOGO_FILE = "logos.json"
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


# โดเมนบริษัทไว้ดึงโลโก้ — ตัวที่ไม่มีโลโก้จะใช้อักษรย่อแทน
TICKER_DOMAIN = {
    "SCB": "scb.co.th", "KTB": "krungthai.com", "TTB": "ttbbank.com",
    "BAY": "krungsri.com", "TISCO": "tisco.co.th", "GULF": "gulf.co.th",
    "CPALL": "cpall.co.th", "AAPL": "apple.com", "MSFT": "microsoft.com",
    "NVDA": "nvidia.com", "AMZN": "amazon.com", "GOOG": "abc.xyz",
    "WMT": "walmart.com", "COST": "costco.com", "COKE": "cokeconsolidated.com",
    "BRK.B": "brk.com", "JEPQ": "jpmorgan.com",
}


def fetch_logos():
    """เก็บเฉพาะโลโก้ที่มีจริง (บางโดเมนคืน 404 พร้อมรูปลูกโลกกลางๆ มา)

    โลโก้แทบไม่เปลี่ยน จึงเก็บไว้ใช้ซ้ำ ไม่ต้องยิงทุกรอบ build
    """
    cached = load_json(LOGO_FILE)
    if cached.get("logos") and cached.get("at"):
        try:
            if (NOW - datetime.fromisoformat(cached["at"])).total_seconds() < 86400:
                print(f"  ↻ ใช้โลโก้เดิม {len(cached['logos'])} สินทรัพย์")
                return cached["logos"]
        except Exception:
            pass

    def one(item):
        label, dom = item
        url = f"https://www.google.com/s2/favicons?domain={dom}&sz=64"
        try:
            r = requests.get(url, headers=BROWSER_UA, timeout=10)
            return (label, url) if r.status_code == 200 else (label, None)
        except Exception:
            return label, None

    out = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for label, url in pool.map(one, TICKER_DOMAIN.items()):
            if url:
                out[label] = url
    print(f"  ✓ โลโก้ {len(out)}/{len(TICKER_DOMAIN)} สินทรัพย์")
    if out:
        save_json(LOGO_FILE, {"at": NOW.isoformat(), "logos": out})
    return out


def yahoo_session():
    """Yahoo ต้องมี cookie + crumb ก่อน ถึงจะเรียกข้อมูลพื้นฐานได้ (ไม่งั้น 401)"""
    s = requests.Session()
    s.headers.update(BROWSER_UA)
    try:
        s.get("https://fc.yahoo.com", timeout=10)
        crumb = s.get("https://query1.finance.yahoo.com/v1/test/getcrumb",
                      timeout=10).text.strip()
        return (s, crumb) if crumb and "<" not in crumb else (None, None)
    except Exception as ex:
        print(f"  ! ขอ crumb ไม่สำเร็จ: {ex}")
        return None, None


def fetch_fundamentals(markets):
    """ดึง PE / กำไรต่อหุ้น / มูลค่าตามบัญชี มาให้หน้ากราฟใช้คำนวณ

    ดัชนี ค่าเงิน ทองคำ และคริปโต ไม่มีค่าพวกนี้ตามธรรมชาติ — ปล่อยว่างไว้
    """
    sess, crumb = yahoo_session()
    if not sess:
        return
    symbols = {label: sym for label, sym, _ in TICKERS if sym != THAI_GOLD}

    def one(m):
        sym = symbols.get(m["label"])
        if not sym:
            return m, None
        try:
            r = sess.get(f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}",
                         params={"modules": "defaultKeyStatistics,summaryDetail,financialData",
                                 "crumb": crumb}, timeout=12)
            if r.status_code != 200:
                return m, None
            res = r.json()["quoteSummary"]["result"][0]

            def val(mod, key):
                v = (res.get(mod) or {}).get(key)
                v = v.get("raw") if isinstance(v, dict) else v
                return round(v, 4) if isinstance(v, (int, float)) else None

            return m, {
                "pe": val("summaryDetail", "trailingPE"),
                "fpe": val("summaryDetail", "forwardPE"),
                "eps": val("defaultKeyStatistics", "trailingEps"),
                "bvps": val("defaultKeyStatistics", "bookValue"),
                "target": val("financialData", "targetMeanPrice"),
            }
        except Exception:
            return m, None

    got = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        for m, f in pool.map(one, markets):
            if f and any(v is not None for v in f.values()):
                m["fund"] = f
                got += 1
    print(f"  ✓ ข้อมูลพื้นฐาน {got}/{len(markets)} สินทรัพย์")


# จักรวาลหุ้นสำหรับเมนูค้นหาในหน้ากราฟ — หุ้นหลักของตลาดไทยและสหรัฐ
# (ไม่ได้ครอบคลุมทุกตัวในตลาด เพราะต้องดึงข้อมูลใหม่ทุก 3 ชม.)
UNIVERSE_TH = """
ADVANC AOT AWC BANPU BBL BCP BDMS BEM BGRIF BH BJC BLA BTS CBG CENTEL CPALL CPF
CPN CRC DELTA EA EGCO GLOBAL GPSC GULF HMPRO INTUCH IRPC IVL KBANK KCE KKP KTB
KTC LH MINT MTC OR OSP PTT PTTEP PTTGC RATCH SAWAD SCB SCC SCGP STA STGT TISCO
TCAP THG TIDLOR TOA TOP TQM TRUE TTB TU TVO VGI WHA AAV AMATA ASP BAM BCH BCPG
BPP CHG CK CKP COM7 DOHOME EPG ESSO GUNKUL ICHI JMART JMT KEX M MEGA NEX ORI PLANB
PRM PSH QH RBF RS SIRI SPALI SPRC SSP STARK SUPER SYNEX TASCO THANI TKN TPIPP TTA
""".split()

UNIVERSE_US = """
AAPL MSFT NVDA AMZN GOOG GOOGL META TSLA BRK-B LLY AVGO JPM V UNH XOM MA PG JNJ
HD COST ABBV MRK CVX ADBE WMT PEP KO CRM BAC AMD NFLX TMO ACN LIN MCD CSCO ABT
DHR WFC TXN DIS INTC VZ PM INTU CAT AMGN CMCSA IBM NOW UNP GE NKE COP SPGI RTX
LOW HON UPS NEE BA MS AXP T ELV SBUX BLK PLD GS DE MDT LMT SYK ISRG ADI TJX BKNG
MDLZ GILD CVS ADP VRTX C SCHW MMC ZTS CI SO REGN AMT PGR BSX EOG CB DUK SLB MO
BDX ITW APD NOC CSX FDX MU WM TGT PNC USB EMR AON ORLY MCK HUM PSA MAR MCO SHW
AJG ROP AFL TRV SRE PCAR OXY DXCM CTAS MSI PSX GM F DAL UAL AAL CCL RCL ABNB
UBER LYFT SQ PYPL SHOP SNOW PLTR COIN RIVN LCID SOFI HOOD DKNG ROKU SPOT ZM
TSM SPCX
""".split()

# กองทุน/ETF ที่เน้นกระแสเงินปันผล — แยกก้อนไว้เพราะไม่ใช่บริษัทเดินเครื่องผลิต จึงไม่มีงบ
# การเงินให้ดึง (ไม่ติดธง .f) แต่มีประวัติปันผลจริง เลยเข้าเมนูผ่านธง .d แทน
# แบ่งเป็นสามพวก: covered call เน้นปันผลสูง / ปันผลรายเดือน / พันธบัตรระยะสั้นแบบจ่ายทุกเดือน
UNIVERSE_US += """
QQQI SPYI JEPI QYLD XYLD RYLD DIVO IWMI BALI
O MAIN ARCC
SGOV BIL TFLO USFR
SCHD VYM DGRO HDV SPHD
""".split()


# อัตราดอกเบี้ยพันธบัตรสหรัฐ — อยู่ในจักรวาลเหมือนสินทรัพย์ตัวหนึ่ง ไม่ใช่กรณีพิเศษ
# จะได้ใช้ท่อดึงข้อมูล/เขียนไฟล์กราฟ/ดัชนี ชุดเดียวกับหุ้นทุกตัวโดยไม่ต้องเขียนทางแยก
# ชื่อเต็มกำหนดเองเพราะชื่อที่ Yahoo ส่งมาอ่านไม่รู้เรื่อง ("CBOE Interest Rate 10 Year T No")
RATE_SYMS = [
    ("^IRX", "US 3M",  "ดอกเบี้ยสหรัฐ 3 เดือน (พันธบัตรระยะสั้น)"),
    ("^TNX", "US 10Y", "ดอกเบี้ยสหรัฐ 10 ปี (พันธบัตรระยะยาว)"),
]
RATE_NAMES = {label: name for _, label, name in RATE_SYMS}


def universe_symbols():
    """(สัญลักษณ์ Yahoo, ชื่อที่แสดง, ตลาด) ของทุกตัวในเมนูค้นหา"""
    out = []
    seen = set()
    for sym, label, _ in RATE_SYMS:
        seen.add(label)
        out.append((sym, label, "rate"))
    for label, sym, group in TICKERS:          # ตัวที่อยู่ในแถบราคาอยู่แล้ว
        if sym != THAI_GOLD and label not in seen:
            seen.add(label)
            out.append((sym, label, group))
    for t in UNIVERSE_TH:
        if t not in seen:
            seen.add(t)
            out.append((t + ".BK", t, "th"))
    for t in UNIVERSE_US:
        # เทียบด้วยชื่อที่แสดง (BRK.B) ไม่ใช่ชื่อ ticker ดิบ (BRK-B) — ของเดิมเทียบคนละตัวกับที่
        # เก็บลง seen ตัวที่มีอยู่ในแถบราคาแล้วจึงหลุดเข้ามาซ้ำ โดนดึงข้อมูลสองรอบทุก build
        label = t.replace("-", ".")
        if label not in seen:
            seen.add(label)
            out.append((t, label, "intl"))
    return out


CHART_DIR = "chart"
CHART_RANGES = [          # (ชื่อปุ่ม, range, interval)
    ("1D", "1d", "5m"), ("1M", "1mo", "1d"), ("3M", "3mo", "1d"),
    ("6M", "6mo", "1d"), ("1Y", "1y", "1d"), ("3Y", "3y", "1wk"),
    ("5Y", "5y", "1wk"),
    # 10 ปีใช้แท่งรายเดือน (≈121 แท่ง) ไม่ใช่รายสัปดาห์ (≈523) — ดูภาพรวมทศวรรษพอ
    # และไฟล์ไม่บวมขึ้นสี่เท่าใน 258 สินทรัพย์ หุ้นที่เพิ่งเข้าตลาดจะได้สั้นกว่านี้ตามจริง
    ("10Y", "10y", "1mo"),
]


def clean_company_name(name, label):
    """ล้างชื่อบริษัทที่ Yahoo ส่งมา แล้วคืน None ถ้าไม่ควรโชว์

    หุ้นไทยที่ไม่มี longName จะตกมาใช้ shortName ซึ่งมีรูปแบบ 'PTT_PTT' /
    'SET_SET Index' คือเอาตัวย่อมาแปะหน้าซ้ำ ตัดส่วนนั้นออกก่อน
    """
    if not name:
        return None
    name = name.strip()
    head, sep, tail = name.partition("_")
    if sep and head.upper() == label.upper().replace(".BK", ""):
        name = tail.strip()
    return name if name and name.lower() != label.lower() else None


def chart_slug(label):
    """ชื่อไฟล์ที่ปลอดภัย เช่น 'S&P 500' → 'sp500', 'BRK.B' → 'brkb'"""
    return re.sub(r"[^a-z0-9]+", "", label.lower()) or "x"


def fetch_candles(sym, rng, interval, meta_out=None, want_events=False):
    """meta_out: dict ที่จะเติมชื่อเต็มบริษัทลงไปให้ — Yahoo แนบมากับกราฟอยู่แล้ว
    จึงไม่ต้องยิงขอชื่ออีกรอบต่างหาก (250+ สินทรัพย์ = ประหยัดไปทั้งชุด)

    want_events=True: ขอประวัติเงินปันผลมาพร้อมกันในคำขอเดียวกัน (ใช้กับ job ช่วง 10Y
    เท่านั้น — ยิงยาวสุดครั้งเดียวพอ ไม่ต้องขอซ้ำทุกช่วงเวลา)"""
    params = {"range": rng, "interval": interval}
    if want_events:
        params["events"] = "div,splits"
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
        params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=15,
    )
    res = r.json()["chart"]["result"][0]
    if meta_out is not None:
        meta = res.get("meta") or {}
        nm = meta.get("longName") or meta.get("shortName")
        if nm:
            meta_out["n"] = nm.strip()
        cur = meta.get("currency")
        if cur:
            meta_out["c"] = cur
        if want_events:
            divs = (res.get("events") or {}).get("dividends") or {}
            # เก็บเฉพาะ (วันที่, จำนวนเงินต่อหุ้น) เรียงเก่าไปใหม่ — พอสำหรับคำนวณ
            # ผลตอบแทนย้อนหลังฝั่งเว็บ ไม่ต้องพกฟิลด์อื่นที่ Yahoo ส่งมาเกินจำเป็น
            meta_out["divs"] = sorted(
                ({"date": d["date"], "amount": round(float(d["amount"]), 6)}
                 for d in divs.values() if d.get("amount") is not None),
                key=lambda d: d["date"])
    ts = res.get("timestamp") or []
    q = res["indicators"]["quote"][0]
    vol = q.get("volume") or []
    out = []
    for i, t in enumerate(ts):
        o, h, lo, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
        if None in (o, h, lo, c):
            continue
        v = vol[i] if i < len(vol) and vol[i] else 0
        # ปริมาณซื้อขายต่อท้าย ใช้กับอินดิเคเตอร์สาย volume
        out.append([t, round(o, 4), round(h, 4), round(lo, 4), round(c, 4), int(v)])
    return out


GOLD_THB_NOTE = ("Derived from XAU/USD × USD/THB, scaled to the latest "
                 "Gold Traders Association price — the association publishes "
                 "today's price only, not history")


def synth_thai_gold(frames, spot=None):
    """แท่งเทียนทองไทย (บาท/บาททอง) — สมาคมค้าทองคำมีแต่ราคาวันนี้ ไม่มีย้อนหลัง
    จึงคำนวณจากทองคำโลก × อัตราแลกเปลี่ยน แล้วปรับสเกลให้ปลายกราฟตรงราคาสมาคมฯ
    """
    gold, fx = frames.get("GOLD USD"), frames.get("USD/THB")
    if not gold or not fx:
        return None
    factor = BAHT_GRAM * GOLD_965 / OZ_GRAM
    out = {}
    for tf, rows in gold.items():
        frows = fx.get(tf)
        if not frows or not rows:
            continue
        fts = [r[0] for r in frows]
        fcl = [r[4] for r in frows]
        conv = []
        for r in rows:
            t, o, h, lo, c = r[0], r[1], r[2], r[3], r[4]
            # ค่าเงินใช้ค่าล่าสุดที่ไม่เกินเวลาแท่งนั้น (ตลาดทองกับตลาดเงินปิดคนละเวลา)
            rate = fcl[max(bisect.bisect_right(fts, t) - 1, 0)]
            k = rate * factor
            conv.append([t, o * k, h * k, lo * k, c * k, r[5] if len(r) > 5 else 0])
        k = (spot / conv[-1][4]) if spot and conv[-1][4] else 1.0
        out[tf] = [[t, round(o * k, 2), round(h * k, 2), round(lo * k, 2), round(c * k, 2), v]
                   for t, o, h, lo, c, v in conv]
    return out or None


def refresh_intraday(index):
    """รอบย่อย: อัปเดตเฉพาะกราฟ 1 วันของสินทรัพย์ในแถบราคา

    เว็บ build ทุกครึ่งชั่วโมง ถ้าดึงครบทุกตัวทุกช่วงเวลาทุกรอบจะเป็นหลักพันคำขอ
    เสี่ยงโดน Yahoo บล็อกจนราคาทั้งเว็บพัง — ตัวที่คนดูจริงคือกราฟวันของแถบราคา
    """
    tape = [(sym, label) for label, sym, _ in TICKERS
            if sym != THAI_GOLD and label in index]
    tf, rng, iv = CHART_RANGES[0]        # 1D

    def one(job):
        sym, label = job
        try:
            return label, fetch_candles(sym, rng, iv)
        except Exception:
            return label, None

    n = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        for label, candles in pool.map(one, tape):
            if not candles:
                continue
            path = f"{CHART_DIR}/{index[label]['s']}.json"
            d = load_json(path)
            if not d.get("tf"):
                continue
            d["tf"][tf] = candles
            save_json(path, d)
            n += 1
    print(f"  ✓ อัปเดตกราฟรายวัน {n}/{len(tape)} สินทรัพย์ (ชุดเต็มรอบหน้าอีก "
          f"{CHART_FULL_HOURS} ชม.)")
    return index


def ttm_yield(divs, tfs):
    """อัตราผลตอบแทนปันผล 12 เดือนล่าสุด (%) — คิดให้ตรงกับ divTTM ฝั่งหน้าเว็บ

    ราคาที่ใช้หารคือราคาปิดล่าสุดจากซีรีส์ละเอียดสุดที่มี (1Y ก่อน แล้วค่อย 10Y)
    เหมือนที่ renderDivSection ใช้ ไม่งั้นตัวเลขสองฝั่งจะไม่ตรงกันแล้วลำดับจะดูมั่ว
    คืน None ถ้าคิดไม่ได้จริงๆ ดีกว่าใส่ 0 ซึ่งจะกลายเป็น "ปันผล 0%" ทั้งที่แค่ไม่รู้
    """
    if not divs:
        return None
    series = tfs.get("1Y") or tfs.get("10Y") or None
    if not series:
        return None
    try:
        price = series[-1][4]
        if not price or price <= 0:
            return None
        now = NOW.timestamp()
        cutoff = now - 365 * 86400
        total = sum(d["amount"] for d in divs
                    if d.get("date") and cutoff < d["date"] <= now)
        if total <= 0:
            return None
        return round(total / price * 100, 2)
    except Exception:
        return None


def build_charts(markets=None):
    """เขียนไฟล์แท่งเทียนแยกรายสินทรัพย์ไว้ให้หน้าเว็บโหลดตอนเปิดกราฟ

    แยกเป็นไฟล์ย่อยแทนที่จะฝังใน index.html เพราะข้อมูลรวมกันหลายสิบเมกะไบต์
    ชุดเต็มดึงทุก CHART_FULL_HOURS ชม. รอบระหว่างนั้นอัปเดตแค่กราฟรายวัน
    """
    os.makedirs(CHART_DIR, exist_ok=True)
    idx_path = f"{CHART_DIR}/index.json"
    cached = load_json(idx_path)
    uni = universe_symbols()
    if cached.get("index") and cached.get("at") and cached.get("v") == CHART_SCHEMA:
        try:
            age = (NOW - datetime.fromisoformat(cached["at"])).total_seconds() / 3600
            # ความสดอย่างเดียวไม่พอ เหมือนกรณี schema — พอเพิ่มสัญลักษณ์ใหม่เข้าจักรวาล
            # cache ที่ยัง "สด" อยู่จะไม่มีตัวใหม่เลย แล้วถูกใช้ซ้ำจนตัวใหม่ไม่ขึ้นเว็บ
            # เทียบกับรายชื่อที่ "ลองดึง" รอบก่อน ไม่ใช่ตัวที่ดึงสำเร็จ — ตัวที่ Yahoo ไม่มี
            # ข้อมูลจริงๆ จะได้ไม่ทำให้ต้องดึงชุดเต็มใหม่ทุกรอบไปตลอด
            fresh = [label for _, label, _ in uni]
            added = set(fresh) - set(cached.get("uni") or [])
            if 0 <= age < CHART_FULL_HOURS and not added:
                print(f"  ↻ ใช้ชุดกราฟเดิมที่ดึงมา {age:.1f} ชม.ที่แล้ว")
                return refresh_intraday(cached["index"])
            if added:
                print(f"  ↻ มีสัญลักษณ์ใหม่ {len(added)} ตัว — ดึงชุดเต็มใหม่")
        except Exception:
            pass

    jobs = [(sym, label, tf, rng, iv) for sym, label, _ in uni
            for tf, rng, iv in CHART_RANGES]

    def run(job):
        sym, label, tf, rng, iv = job
        meta = {}
        try:
            return label, tf, fetch_candles(sym, rng, iv, meta, want_events=(tf == "10Y")), meta
        except Exception:
            return label, tf, None, meta

    frames, meta_by_label, div_by_label = {}, {}, {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        for label, tf, candles, meta in pool.map(run, jobs):
            if candles:
                frames.setdefault(label, {})[tf] = candles
            if meta.get("n") and label not in meta_by_label:
                meta_by_label[label] = meta
            if "divs" in meta:
                div_by_label[label] = meta["divs"]

    # ทองไทยไม่มีให้ดึงย้อนหลัง ต้องประกอบจากทองโลก × ค่าเงิน
    spot = next((m.get("raw_price") for m in (markets or [])
                 if m["label"] == "GOLD THB"), None)
    notes = {}
    gold_thb = synth_thai_gold(frames, spot)
    if gold_thb:
        frames["GOLD THB"] = gold_thb
        notes["GOLD THB"] = GOLD_THB_NOTE
        uni = uni + [(THAI_GOLD, "GOLD THB", "th")]

    index = {}
    for sym, label, group in uni:
        tfs = frames.get(label)
        if not tfs:
            continue
        slug = chart_slug(label)
        # divs เป็น [] ได้จริง (เช็คแล้วว่าไม่จ่ายปันผลในช่วง 10 ปีที่มีข้อมูล) ต่างจาก
        # None (ไม่ได้เช็ค เช่น job ของ tf=10Y พังไปเฉยๆ) — เก็บเป็น [] เริ่มต้นไว้เผื่อกรณีหลัง
        # ฝั่งเว็บจะได้ไม่ต้องเดาว่า "ไม่มีข้อมูล" กับ "เช็คแล้วว่าไม่จ่าย" อันไหน
        save_json(f"{CHART_DIR}/{slug}.json",
                  {"label": label, "tf": tfs, "note": notes.get(label, ""),
                   "div": div_by_label.get(label, [])})
        index[label] = {"s": slug, "g": group}
        meta = meta_by_label.get(label) or {}
        # ชื่อที่ Yahoo ส่งมาให้ดัชนีดอกเบี้ยอ่านไม่รู้เรื่อง ใช้ชื่อที่เรากำหนดเองแทน
        nm = RATE_NAMES.get(label) or clean_company_name(meta.get("n"), label)
        # ชื่อเต็มซ้ำกับตัวย่อ (เช่นดัชนี/ค่าเงิน) ไม่ต้องเก็บ ประหยัดขนาดไฟล์หน้าแรก
        if nm:
            index[label]["n"] = nm
        if meta.get("c"):
            index[label]["cur"] = meta["c"]
        # ธงบอกว่า "มีประวัติปันผลจริง" แบบเบาๆ ไว้ในดัชนี — เหมือน .f (มีงบการเงิน)
        # ฝั่งเว็บจะได้ตัดสินใจได้ (ปุ่ม FINANCIALS โชว์ไหม / อยู่ในลิสต์เลื่อนซ้าย-ขวาไหม)
        # โดยไม่ต้องดึงไฟล์กราฟรายตัวมาเช็คก่อน — กองทุน/ETF อย่าง JEPQ ไม่มีงบการเงิน
        # (ไม่ติด .f) แต่มีปันผลจริง จึงต้องมีธงนี้แยกไว้ต่างหาก ไม่งั้นหน้าเว็บจะซ่อนไปเลย
        if div_by_label.get(label):
            index[label]["d"] = 1
            # อัตราผลตอบแทนปันผล 12 เดือนล่าสุด คิดไว้ตั้งแต่ตอน build เก็บลงดัชนีเลย
            # ฝั่งเว็บจะได้เรียงลำดับได้ทันทีโดยไม่ต้องโหลดไฟล์กราฟทีละตัวมาคำนวณเอง
            # (คิดแบบเดียวกับ divTTM ในหน้าเว็บเป๊ะๆ — ย้อน 365 วัน หารด้วยราคาปิดล่าสุด)
            y = ttm_yield(div_by_label[label], tfs)
            if y is not None:
                index[label]["y"] = y
    total = sum(len(c) for tfs in frames.values() for c in tfs.values())
    print(f"  ✓ กราฟ {len(index)}/{len(uni)} สินทรัพย์ · {total:,} แท่งเทียน")
    if index:
        # เก็บรายชื่อที่ "ลองดึง" ไว้ด้วย ไม่ใช่แค่ตัวที่ดึงสำเร็จ — ตัวที่ Yahoo ไม่มีข้อมูลจะได้
        # ไม่ถูกนับว่าขาดตลอดกาลจนต้องดึงชุดเต็มใหม่ทุกรอบ (ดูเงื่อนไขใช้ cache ซ้ำด้านบน)
        save_json(idx_path, {"v": CHART_SCHEMA, "at": NOW.isoformat(), "index": index,
                             "uni": sorted(label for _, label, _ in uni)})
    return index


# ─────────────────────────────────────────────────────────────
# งบการเงิน — รายได้/ค่าใช้จ่าย/กำไร/สินทรัพย์/หนี้สิน ย้อนหลังสูงสุด 4 ปี
# + รายไตรมาสของปีที่ดำเนินอยู่ ไว้ให้กดดูจากรายการโปรดในหน้ากราฟ
# ─────────────────────────────────────────────────────────────
FIN_DIR = "fin"
# เช็คทุกวัน ไม่ใช่ทุกสัปดาห์ — จะได้ตามงบใหม่ที่เพิ่งประกาศทันภายในไม่เกิน ~24 ชม.
# (งบเปลี่ยนแค่ตอนมีไตรมาสใหม่ออก แต่ "ตอนไหน" คาดเดาล่วงหน้าไม่ได้ จึงต้องเช็คถี่พอ)
FIN_FULL_DAYS = 1
# เพดานจริงของข้อมูลฟรีจาก Yahoo คือ 5 ปี — ทดสอบย้อนไปไกลกว่านั้นแล้วได้ค่าว่างทุกตัว
# ทั้งหุ้นไทยและหุ้นนอก จึงตั้งไว้ 5 ไม่ใช่ 10 (ตั้งเกินไปก็ไม่มีข้อมูลมาเติมอยู่ดี)
FIN_YEARS = 5
FIN_BACKFILL_MAX = 3      # กันลูปไม่รู้จบถ้า Yahoo เปลี่ยนพฤติกรรม
# ขยับเลขนี้ทุกครั้งที่เปลี่ยนรูปแบบ/ความลึกของข้อมูลงบ เพื่อทิ้ง cache รุ่นเก่าทั้งหมด
FIN_SCHEMA = 2

# (คีย์สั้นในไฟล์ JSON, ชื่อฟิลด์ที่ Yahoo ใช้, ชื่อที่แสดงผล, หมวด)
FIN_FIELDS = [
    ("rev",    "TotalRevenue",                          "Revenue",              "income"),
    ("cogs",   "CostOfRevenue",                          "Cost of Revenue",      "income"),
    ("gp",     "GrossProfit",                             "Gross Profit",         "income"),
    ("opex",   "OperatingExpense",                        "Operating Expense",    "income"),
    ("opinc",  "OperatingIncome",                          "Operating Income",     "income"),
    ("ni",     "NetIncome",                                "Net Income",           "income"),
    ("eps",    "DilutedEPS",                               "Diluted EPS",          "income"),
    ("assets", "TotalAssets",                              "Total Assets",         "balance"),
    ("liab",   "TotalLiabilitiesNetMinorityInterest",      "Total Liabilities",    "balance"),
    ("equity", "TotalEquityGrossMinorityInterest",         "Total Equity",         "balance"),
    ("cash",   "CashAndCashEquivalents",                   "Cash & Equivalents",   "balance"),
    ("debt",   "TotalDebt",                                "Total Debt",           "balance"),
]
_FIN_TYPES = [f"{p}{y}" for p in ("annual", "quarterly") for _, y, _, _ in FIN_FIELDS]


def _parse_fin_periods(result):
    """แปลงผลตอบจาก fundamentals-timeseries เป็น {{annual: [...], quarterly: [...]}}
    แต่ละงวดรวมทุกฟิลด์ที่มีค่าในวันเดียวกันไว้แถวเดียว เรียงเก่าไปใหม่"""
    by_period = {"annual": {}, "quarterly": {}}
    for item in result or []:
        typ = ((item.get("meta") or {}).get("type") or [None])[0]
        if not typ:
            continue
        span = "annual" if typ.startswith("annual") else "quarterly"
        suffix = typ[len(span):]
        short = next((s for s, y, _, _ in FIN_FIELDS if y == suffix), None)
        if not short:
            continue
        for pt in item.get(typ) or []:
            date = pt.get("asOfDate")
            val = (pt.get("reportedValue") or {}).get("raw")
            if date is None or val is None:
                continue
            by_period[span].setdefault(date, {})[short] = val
    out = {}
    for span, rows in by_period.items():
        out[span] = [{"date": d, **vals} for d, vals in sorted(rows.items())]
    return out


def fetch_financials():
    """งบการเงินย่อ (ไม่ใช่งบเต็ม) ของหุ้นทุกตัวในจักรวาลค้นหา — ธนาคาร/ประกัน
    มักไม่มี cost of revenue / gross profit ตามธรรมชาติของธุรกิจ ปล่อยว่างไว้เฉยๆ

    ดัชนี ค่าเงิน ทองคำ คริปโต ไม่มีงบการเงิน — ยิงขอไปเฉยๆ แล้วข้ามถ้าไม่มีข้อมูลจริง

    คืนค่า (ชื่อสินทรัพย์ที่มีงบ, เวลาที่เช็คล่าสุด) — ใช้ตัวหลังโชว์ในหน้าเว็บว่า
    "เช็คล่าสุดเมื่อไร" ให้ผู้ใช้เห็นว่าข้อมูลยังสดอยู่ ไม่ใช่ค้างเป็นเดือน
    """
    idx_path = f"{FIN_DIR}/index.json"
    cached = load_json(idx_path)
    # ต้องเช็ครุ่นของข้อมูลด้วย ไม่ใช่แค่ "เก่าหรือยัง" — ตอนขยายจาก 4 ปีเป็น 5 ปี
    # ตัว cache บน GitHub Actions ยังเป็นชุด 4 ปีแต่เวลายังสด เลยถูกใช้ซ้ำจนของใหม่ไม่ขึ้นเว็บ
    if cached.get("labels") and cached.get("at") and cached.get("v") == FIN_SCHEMA:
        try:
            age_days = (NOW - datetime.fromisoformat(cached["at"])).total_seconds() / 86400
            if 0 <= age_days < FIN_FULL_DAYS:
                print(f"  ↻ ใช้งบการเงินเดิมที่ดึงมา {age_days:.1f} วันที่แล้ว")
                return set(cached["labels"]), cached["at"]
        except Exception:
            pass

    sess, crumb = yahoo_session()
    if not sess:
        return set(), cached.get("at")
    uni = universe_symbols()

    def _ask(sym, period2):
        r = sess.get(
            "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/"
            f"v1/finance/timeseries/{sym}",
            params={"symbol": sym, "type": ",".join(_FIN_TYPES),
                    "period1": "1", "period2": str(period2), "crumb": crumb}, timeout=15)
        if r.status_code != 200:
            return None
        return _parse_fin_periods(r.json().get("timeseries", {}).get("result"))

    def one(job):
        sym, label, _ = job
        try:
            periods = _ask(sym, int(NOW.timestamp()) + 86400)
            if not periods:
                return label, None
            # Yahoo คืนงบรายปีให้ครั้งละ 4 งวดล่าสุดเท่านั้น ไม่ว่าจะตั้ง period1 ย้อนไปไกลแค่ไหน
            # แต่ถ้าตัด period2 ไว้ก่อนงวดเก่าสุดที่เพิ่งได้มา จะได้บล็อกก่อนหน้าเพิ่มอีก
            # ยิงซ้ำแบบนี้จนไม่มีงวดใหม่โผล่ — ในทางปฏิบัติได้ครบ 5 ปีแล้วตัน (ข้อมูลมีแค่นั้น)
            for _ in range(FIN_BACKFILL_MAX):
                rows = periods.get("annual") or []
                if not rows or len(rows) >= FIN_YEARS:
                    break
                try:
                    cut = int(datetime.strptime(rows[0]["date"], "%Y-%m-%d").timestamp()) - 86400
                except Exception:
                    break
                older = _ask(sym, cut)
                extra = [r for r in ((older or {}).get("annual") or [])
                         if r["date"] < rows[0]["date"]]
                if not extra:
                    break
                periods["annual"] = sorted(extra + rows, key=lambda r: r["date"])
            periods["annual"] = (periods.get("annual") or [])[-FIN_YEARS:]
            if not periods.get("annual") and not periods.get("quarterly"):
                return label, None
            return label, periods
        except Exception:
            return label, None

    os.makedirs(FIN_DIR, exist_ok=True)
    got = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for label, periods in pool.map(one, uni):
            if not periods:
                continue
            slug = chart_slug(label)
            save_json(f"{FIN_DIR}/{slug}.json", {"label": label, **periods})
            got.add(label)
    print(f"  ✓ งบการเงิน {len(got)}/{len(uni)} สินทรัพย์")
    at = NOW.isoformat()
    save_json(idx_path, {"v": FIN_SCHEMA, "at": at, "labels": sorted(got)})
    return got, at


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

# ข่าวอาชญากรรม/อุบัติเหตุ มักติดเข้ามาในหมวดการเมืองผ่านคำอย่าง "ทหาร" "กฎหมาย"
# (เช่น ข่าวลอบยิงทหารพราน หรือคดีในศาล) — กันไว้ไม่ให้ปนกับข่าวการเมืองจริง
KW_CRIME = [
    "ฆาตกรรม", "ฆ่า", "ยิงดับ", "ลอบยิง", "กราดยิง", "มือปืน", "ข่มขืน", "ล่วงละเมิด",
    "ชิงทรัพย์", "ปล้น", "ลักทรัพย์", "โจรกรรม", "ยาเสพติด", "ยาบ้า", "ไอซ์",
    "ไฟไหม้", "เพลิงไหม้", "วางเพลิง", "ระเบิดพลีชีพ",
    "อุบัติเหตุ", "รถชน", "รถคว่ำ", "เมาแล้วขับ", "ซิ่ง", "จมน้ำ", "ตกตึก",
    "ทะเลาะวิวาท", "แก๊งคอลเซ็นเตอร์", "หลอกลวง", "ต้มตุ๋น", "พนันออนไลน์",
    "ศพ", "ดับสลด", "สลด", "คดีข่มขืน", "ล่าตัว", "รวบตัว", "จับกุม",
    "murder", "homicide", "stabbing", "shooting", "gunman", "arson", "robbery",
    "burglary", "rape", "kidnap", "car crash", "drink driving", "manhunt",
]
# ข่าวจะเป็น "การเมือง" ได้ ต้องมีคำที่ชี้สถาบัน/ตัวแสดงทางการเมืองชัดเจนอย่างน้อยหนึ่งคำ
# ไม่งั้นข่าวพยากรณ์อากาศ งานมหาวิทยาลัย หรือเหตุยิงกันในพื้นที่ จะหลุดเข้ามาด้วย
KW_POLI_STRONG = [
    "รัฐบาล", "นายกรัฐมนตรี", "นายกฯ", "รัฐมนตรี", "ครม.", "รัฐสภา", "สภาผู้แทน",
    "ส.ส.", "สส.", "ส.ว.", "สว.", "วุฒิสภา", "ฝ่ายค้าน", "พรรค", "ปชน.", "ภท.", "ปชป.",
    "เลือกตั้ง", "อภิปราย", "รัฐธรรมนูญ", "ยุบพรรค", "กกต.", "ศาลรัฐธรรมนูญ", "ป.ป.ช.",
    "นโยบายรัฐ", "ประธานาธิบดี", "ผู้นำประเทศ", "ทูต", "เจรจา", "ข้อตกลง", "คว่ำบาตร",
    "ประชุมสุดยอด", "หยุดยิง", "สงคราม", "กองทัพ", "ทำเนียบ", "กระทรวง", "มติ ครม.",
    "government", "parliament", "election", "president", "prime minister",
    "minister", "senate", "congress", "sanction", "diplomat", "treaty",
    "ceasefire", "summit", "war", "coup", "referendum", "cabinet",
]
_CRIME_C = _compile_terms(KW_CRIME)
_POLI_STRONG_C = _compile_terms(KW_POLI_STRONG)


def classify(title, summary=""):
    """หัวข้อข่าวบอกว่าข่าวนั้น "เกี่ยวกับอะไร" ได้ตรงกว่าเนื้อข่าว
    จึงให้น้ำหนักคำในหัวข้อมากกว่า ไม่งั้นรายงานราคาหุ้นที่เนื้อข่าวเอ่ยถึง
    การเจรจาระหว่างประเทศ จะถูกจัดเป็นข่าวการเมือง"""
    th, sm = title.lower(), (summary or "").lower()
    scores = {}
    for cat, kws in CATEGORIES:
        scores[cat] = sum(3 for k in kws if k.lower() in th) \
            + sum(1 for k in kws if k.lower() not in th and k.lower() in sm)

    # ข่าวการเมืองต้องมีคำที่ชี้สถาบัน/ตัวแสดงทางการเมือง "ในหัวข้อ"
    # กันข่าวอาชญากรรม อุบัติเหตุ พยากรณ์อากาศ ไม่ให้หลุดเข้าหมวดนี้
    if scores["poli"] and not any(_hit(p, th) for p in _POLI_STRONG_C):
        scores["poli"] = 0
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

                # feedparser คืนเวลาเป็น UTC เสมอ ต้องใช้ timegm ไม่ใช่ mktime
                # (mktime อ่านเป็นเวลาท้องถิ่น ทำให้ข่าวดูเก่ากว่าจริงตาม timezone ของเครื่อง)
                tp = e.get("published_parsed") or e.get("updated_parsed")
                dt = (datetime.fromtimestamp(calendar.timegm(tp), tz=timezone.utc).astimezone(TZ)
                      if tp else NOW)
                if (NOW - dt).total_seconds() > MAX_AGE_HOURS * 3600:
                    continue

                raw_summary = e.get("summary", "") or ""
                summary = clean(raw_summary)[:320]
                blob = title + " " + summary
                cat = classify(title, summary)
                if not cat:
                    continue

                geo = geolocate(blob)
                items.append({
                    "title": title, "summary": summary, "link": e.get("link", "#"),
                    "source": source, "lang": lang, "cat": cat, "dt": dt,
                    "age": age_label(dt), "image": extract_image(e),
                    # ฟีดที่ไม่ส่งเวลามาให้ ถูกตีเป็น NOW จึงนับเป็นข่าวสดไม่ได้
                    "dated": bool(tp),
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


# ── ช่องข่าวที่ถ่ายทอดสด (อ่านสถานะสดจากหน้า /live ของ YouTube) ──
LIVE_CHANNELS = [
    # (ชื่อช่อง, handle, แถว)
    ("Thai PBS",           "ThaiPBS",          "th"),
    ("PPTV HD 36",         "pptvhd36",         "th"),
    ("TNN Online",         "tnnonline",        "th"),
    ("Thairath",           "thairath",         "th"),
    ("Amarin TV",          "amarintvhd",       "th"),
    ("Matichon TV",        "MatichonTV",       "th"),
    ("workpoint news",     "workpointnews",    "th"),
    ("Voice TV",           "VoiceTVOfficial",  "th"),
    ("Ch3 Plus",           "ch3plus",          "th"),
    ("Al Jazeera English", "aljazeeraenglish", "intl"),
    ("Sky News",           "SkyNews",          "intl"),
    ("DW News",            "dwnews",           "intl"),
    ("France 24",          "France24_en",      "intl"),
    ("ABC News",           "ABCNews",          "intl"),
    ("NBC News",           "NBCNews",          "intl"),
    ("CBS News",           "cbsnews",          "intl"),
    ("CNA",                "channelnewsasia",  "intl"),
    ("Bloomberg TV",       "markets",          "intl"),
    ("CNBC",               "CNBC",             "intl"),
    ("Reuters",            "Reuters",          "intl"),
    ("NHK WORLD",          "NHKWORLDJAPAN",    "intl"),
    ("euronews",           "euronews",         "intl"),
]

_LIVE_BRACKET = re.compile(r"^\s*\[[^\]]{0,30}live[^\]]{0,20}\]\s*", re.I)
_LIVE_PREFIX = re.compile(r"^[\s​🔴⭕️▶️•\-|]*(?:live|สด)?[\s:：|\-–]*", re.I)


def fetch_live_streams():
    """ช่องข่าวที่กำลังออกอากาศสดอยู่ ณ ตอนที่ build

    ดูจากหน้า /live ของแต่ละช่อง — ถ้าไม่ได้ไลฟ์อยู่ YouTube จะส่งไลฟ์ครั้งก่อนมาแทน
    จึงต้องเช็ก isLive ไม่ใช่แค่ว่ามีวิดีโอ
    """
    def one(ch):
        name, handle, group = ch
        try:
            r = requests.get(f"https://www.youtube.com/@{handle}/live",
                             headers=BROWSER_UA, timeout=15)
            if r.status_code != 200 or '"isLive":true' not in r.text:
                return None
            vid = re.search(r'"videoId":"([\w-]{11})"', r.text)
            if not vid:
                return None
            og = re.search(r'<meta property="og:title" content="([^"]{0,140})"', r.text)
            title = html.unescape(og.group(1)).strip() if og else ""
            # ตัดคำนำหน้าซ้ำๆ ทีละชั้น เช่น "🔴[Live] ..." / "[CNA 24/7 LIVE] ..."
            for _ in range(2):
                title = _LIVE_BRACKET.sub("", _LIVE_PREFIX.sub("", title))
            title = title.strip() or name
            vid = vid.group(1)
            return {"name": name, "group": group, "title": title,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "thumb": f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
                    "thumb_alt": f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"}
        except Exception:
            return None

    out = []
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            out = [s for s in pool.map(one, LIVE_CHANNELS) if s]
    except Exception as ex:
        print(f"  ! ดึงรายการถ่ายทอดสดไม่ได้: {ex}")
    # ไทยขึ้นก่อน แล้วเรียงตามลำดับที่ตั้งไว้
    order = {n: i for i, (n, _, _) in enumerate(LIVE_CHANNELS)}
    out.sort(key=lambda s: (s["group"] != "th", order.get(s["name"], 99)))
    print(f"  ✓ ถ่ายทอดสด {len(out)}/{len(LIVE_CHANNELS)} ช่อง")
    return out


CAT_NAMES = [c for c, _ in CATEGORIES]

CAT_LABELS = {
    "econ": "ECONOMY", "poli": "POLITICS",
    "biz": "BUSINESS", "env": "ENVIRONMENT", "mixed": "MIXED",
}

SCOPES = [("th", "THAI NEWS"), ("intl", "INTERNATIONAL NEWS")]


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


INFL_FILE = "inflation.json"
INFL_HOURS = 12       # เช็ควันละสองครั้ง — ตัวเลขจริงออกเดือนละครั้ง แต่ไม่รู้ว่าวันไหน
# ไฟล์แคชเก็บ "ข้อความที่จะแสดง" (ชื่อแหล่ง/งวด) ไว้ด้วย ไม่ใช่แค่ตัวเลข
# แก้ข้อความในโค้ดแล้วของเก่าจึงยังค้างอยู่จนกว่าแคชจะหมดอายุ — ต้องมีเลขรุ่นกำกับ
INFL_SCHEMA = 2


def fetch_inflation():
    """อัตราเงินเฟ้อสหรัฐฯ (รายเดือน) และไทย (รายปี) จากแหล่งที่เปิดให้ใช้ฟรีไม่ต้องมีคีย์

    สหรัฐฯ: BLS ให้ "ดัชนี" CPI มา ไม่ใช่อัตราเงินเฟ้อ จึงคำนวณ YoY เองจาก
    ดัชนีเดือนล่าสุดเทียบเดือนเดียวกันปีก่อน (สูตรมาตรฐาน)

    ไทย: ไม่มีแหล่งรายเดือนที่เปิดฟรีและเสถียรพอ (FRED ต้องมีคีย์/ต่อไม่ติดจากที่นี่)
    จึงใช้ World Bank ซึ่งเป็นรายปีและช้ากว่า — เก็บ "งวด" ติดไปด้วยเสมอ
    จะได้เห็นชัดว่าเลขสองฝั่งไม่ได้สดเท่ากัน ไม่ใช่เอาไปเทียบกันตรงๆ
    """
    cached = load_json(INFL_FILE)
    if cached.get("at") and cached.get("v") == INFL_SCHEMA:
        try:
            age = (NOW - datetime.fromisoformat(cached["at"])).total_seconds() / 3600
            if 0 <= age < INFL_HOURS:
                print(f"  ↻ ใช้เงินเฟ้อเดิมที่ดึงมา {age:.1f} ชม.ที่แล้ว")
                return cached.get("data") or {}
        except Exception:
            pass

    out = {}
    # ── สหรัฐฯ: ดัชนี CPI รายเดือนจาก BLS แล้วคิด YoY เอง ──
    try:
        yr = NOW.year
        r = requests.post(
            "https://api.bls.gov/publicAPI/v1/timeseries/data/",
            json={"seriesid": ["CUUR0000SA0"],
                  "startyear": str(yr - 1), "endyear": str(yr)},
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
            timeout=25)
        rows = r.json()["Results"]["series"][0]["data"]
        by = {}
        for x in rows:
            if not x.get("period", "").startswith("M"):
                continue
            try:
                by[(int(x["year"]), int(x["period"][1:]))] = float(x["value"])
            except (ValueError, TypeError):
                continue          # BLS ส่ง "-" มาสำหรับเดือนที่ยังไม่ประกาศ
        if by:
            last = max(by)
            prior = (last[0] - 1, last[1])
            if by.get(prior):
                rate = (by[last] / by[prior] - 1) * 100
                out["intl"] = {
                    "rate": round(rate, 2),
                    "period": f"{MONTH_ABBR[last[1] - 1]} {last[0]}",
                    "label": "US CPI", "freq": "year over year",
                    "src": "US BLS",
                }
    except Exception as ex:
        print(f"  ! เงินเฟ้อสหรัฐฯ ดึงไม่ได้: {ex}")

    # ── ไทย: World Bank รายปี (ค่าล่าสุดที่มี) ──
    try:
        r = requests.get(
            "https://api.worldbank.org/v2/country/TH/indicator/FP.CPI.TOTL.ZG",
            params={"format": "json", "mrnev": "1"},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=25)
        rows = (r.json() or [None, None])[1] or []
        for row in rows:
            if row.get("value") is not None:
                out["th"] = {
                    "rate": round(float(row["value"]), 2),
                    "period": str(row.get("date") or ""),
                    "label": "Thailand CPI", "freq": "annual average",
                    "src": "World Bank",
                }
                break
    except Exception as ex:
        print(f"  ! เงินเฟ้อไทย ดึงไม่ได้: {ex}")

    if out:
        save_json(INFL_FILE, {"v": INFL_SCHEMA, "at": NOW.isoformat(), "data": out})
        bits = " · ".join(f"{v['label']} {v['rate']}% ({v['period']})" for v in out.values())
        print(f"  ✓ เงินเฟ้อ {bits}")
    else:
        print("  ! ไม่ได้ตัวเลขเงินเฟ้อเลย ใช้ของเดิมถ้ามี")
        return (cached.get("data") or {})
    return out


MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# คำที่ชี้ความขัดแย้งด้วยตัวเองได้เลย — เจอคำเดียวก็นับ
CONFLICT_STRONG = [
    "war", "warfare", "invasion", "invade", "airstrike", "air strike", "missile",
    "ceasefire", "cease-fire", "nuclear weapon", "warhead", "offensive", "shelling",
    "insurgent", "militant", "genocide", "coup", "martial law", "mobilisation",
    "mobilization", "warship", "drone strike", "occupation", "annex",
    "สงคราม", "ขีปนาวุธ", "นิวเคลียร์", "รัฐประหาร", "สู้รบ", "กองทัพ", "ปะทะเดือด",
    "กฎอัยการศึก", "โดรนโจมตี", "ยิงถล่ม", "ฐานทัพ", "กองกำลัง", "โจมตีทางทหาร",
    "ยิงจรวด", "หยุดยิง", "ผู้ก่อการร้าย",
]
# คำที่ต้องมีคู่กันอย่างน้อยสองคำถึงจะนับ — คำเดียวมักเป็นข่าวอาชญากรรมหรือข่าวทั่วไป
CONFLICT_WEAK = [
    "military", "troops", "army", "navy", "border", "clash", "attack", "strike",
    "sanction", "nato", "defence", "defense", "soldier", "rebel", "hostage",
    "ทหาร", "ชายแดน", "ปะทะ", "โจมตี", "คว่ำบาตร", "อาวุธ", "ตึงเครียด", "กบฏ",
    "ตอบโต้", "ขัดแย้ง", "จรวด", "ระเบิด", "สังหาร",
]


def _conflict_matcher(words):
    """คำอังกฤษต้องจับแบบทั้งคำ ไม่ใช่ substring — ไม่งั้น 'war' จะไปโดน Awards/warming/Warsh
    และ 'nato' จะไปโดน senator ทำให้ข่าวรางวัล ข่าวน้ำท่วม ข่าวเฟด ถูกนับเป็นข่าวสงครามหมด
    ส่วนภาษาไทยไม่มีการเว้นวรรคระหว่างคำ จึงต้องใช้ substring ตามเดิม
    """
    ascii_w = [w for w in words if w.isascii()]
    thai_w = [w for w in words if not w.isascii()]
    pat = None
    if ascii_w:
        alt = "|".join(re.escape(w) for w in sorted(ascii_w, key=len, reverse=True))
        pat = re.compile(rf"(?<![a-z]){alt}(?![a-z])")
    return pat, thai_w


_STRONG_RE, _STRONG_TH = _conflict_matcher(CONFLICT_STRONG)
_WEAK_RE, _WEAK_TH = _conflict_matcher(CONFLICT_WEAK)


def _count_conflict(pat, thai_words, blob):
    n = len(set(pat.findall(blob))) if pat else 0
    return n + sum(1 for w in thai_words if w in blob)


def conflict_pulse(news):
    """สัดส่วนข่าวความขัดแย้งในชุดข่าว 24 ชม.ล่าสุด

    ตัวเลขที่คืนคือ "ข่าวความขัดแย้งคิดเป็นกี่ % ของข่าวทั้งหมดตอนนี้" ซึ่งนับได้จริง
    และตรวจย้อนได้ ไม่ใช่ดัชนีชี้วัดโอกาสเกิดสงคราม — อย่างหลังไม่มีวิธีคำนวณที่ซื่อสัตย์
    จากข่าว จึงไม่ทำ (ดูหมายเหตุที่แสดงบนหน้าเว็บประกอบ)
    """
    hits = []
    for it in news:
        blob = f"{it.get('title', '')} {it.get('summary', '')}".lower()
        if not blob.strip():
            continue
        strong = _count_conflict(_STRONG_RE, _STRONG_TH, blob)
        weak = _count_conflict(_WEAK_RE, _WEAK_TH, blob)
        if strong >= 1 or weak >= 2:
            hits.append((strong * 2 + weak, it))
    total = len(news) or 1
    hits.sort(key=lambda x: (-x[0], -x[1]["dt"].timestamp()))
    places = sorted({h[1]["place"] for h in hits if h[1].get("place")})
    return {
        "pct": round(len(hits) / total * 100),
        "n": len(hits), "total": total, "places": places[:12],
        "stories": [{
            "title": i["title"], "link": i["link"], "source": i["source"],
            "age": i["age"], "place": i["place"], "image": i["image"],
        } for _, i in hits[:14]],
    }


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
    จะ ก็ แต่ ว่า ด้วย เพื่อ ต่อ ยัง ทั้ง คน ปี วัน นี้ นั้น อยู่ ขึ้น ลง
    january february march april may june july august september october november december
    reuters bloomberg afp cnbc cnn bbc guardian ข่าว""".split())
    # นับแบบไม่แยกตัวพิมพ์ ไม่งั้น Retail กับ retail จะกินโควตาคนละช่อง
    freq, forms, rep = {}, {}, {}
    for it in items:
        seen = set()
        for w in re.findall(r"[ก-๙]{3,}|[A-Za-z]{3,}", it["title"]):
            key = w.lower()
            if key in stop:
                continue
            freq[key] = freq.get(key, 0) + 1
            forms.setdefault(key, {})[w] = forms.setdefault(key, {}).get(w, 0) + 1
            # ข่าวตัวแทนของคำนั้น — เลือกข่าวที่มีรูปก่อน ไว้ใช้เป็นภาพพื้นหลัง
            if key not in seen:
                seen.add(key)
                cur = rep.get(key)
                if cur is None or (not cur.get("image") and it.get("image")):
                    rep[key] = it
    top = sorted(freq.items(), key=lambda x: -x[1])[:n]
    out = []
    for key, f in top:
        it = rep.get(key) or {}
        label = max(forms[key].items(), key=lambda x: x[1])[0]     # รูปคำที่พบบ่อยสุด
        out.append({"w": label, "n": f, "image": it.get("image"), "link": it.get("link"),
                    "cat": it.get("cat", "mixed"), "title": it.get("title", "")})
    return out


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
    `<div class="hd-top"><h4>${d.place}</h4><span>${d.total} stories</span></div>` +
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

function tipHtml(d){
  // เอาข่าวเด่นสุดของจุดนั้นที่มีรูปมาโชว์ ถ้าไม่มีรูปเลยก็โชว์แต่ตัวหนังสือ
  const s = (d.stories || []).find(x => x.image) || (d.stories || [])[0];
  const img = (s && s.image)
    ? `<img class="tip-img" src="${s.image}" alt="" onerror="this.remove()">` : '';
  const head = s ? `<span class="tip-head">${s.title}</span>` : '';
  const meta = s ? `<span>${s.source} · ${s.age}</span>` : '';
  return img + `<strong>${d.place}</strong>` + head + meta +
    `<span>${d.total} stories · Econ ${d.econ} · Politics ${d.poli} · Business ${d.biz} · Env ${d.env}</span>`;
}

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
         .html(tipHtml(d));
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


def render(news, markets, charts=None, logos=None, streams=None, fin_at=None,
           infl=None):
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

    def is_live(it):
        """ข่าวสด = มีเวลาเผยแพร่จริงจากฟีด และเพิ่งออกภายในกรอบ LIVE_WINDOW_MIN
        ฟีดที่ไม่ส่งเวลามาจะถูกตีเป็น NOW ซึ่งทำให้ข่าวเก่าปนมาเป็นข่าวสดได้"""
        return bool(it.get("dated")) and (NOW - it["dt"]).total_seconds() <= LIVE_WINDOW_MIN * 60

    # เรื่องเด่น = ข่าวใหม่สุดที่มีรูป (ถ้าไม่มีรูปเลยใช้ข่าวใหม่สุด)
    top_story = next((i for i in news if i.get("image")), news[0] if news else None)
    primary_scope = top_story["scope"] if top_story else "th"

    groups = {}
    for sc, _ in SCOPES:
        pool = [i for i in news if i["scope"] == sc]
        top = next((i for i in pool if i.get("image")), pool[0] if pool else None)
        rest = [i for i in pool if i is not top]
        # แถวล่าสุดฝั่งต่างประเทศ เอาข่าวจากสำนักข่าวต่างประเทศจริงๆ ก่อน
        # ไม่ใช่สื่อไทยที่รายงานเรื่องต่างประเทศ (ถ้าไม่พอค่อยเติมจากที่เหลือ)
        if sc == "intl":
            foreign = [i for i in rest if i["lang"] != "th"]
            latest = pick(foreign, PER_ROW)
            if len(latest) < PER_ROW:
                latest = pick(foreign + [i for i in rest if i["lang"] == "th"], PER_ROW)
        else:
            latest = pick(rest, PER_ROW)
        groups[sc] = {
            "top": top, "latest": latest,
            "live": pick([i for i in rest if is_live(i)], PER_ROW),
            "cats": {c: pick([i for i in pool if i["cat"] == c], PER_ROW) for c in CAT_NAMES},
            "n": len(pool),
        }
    markers = build_markers(news)
    kws = top_keywords(news)
    maxf = max([k["n"] for k in kws], default=1)
    located = sum(1 for i in news if i["place"])

    def speak_attrs(it):
        text = html.escape(f"{it['title']}. {it['summary']}", quote=True)
        lang = "th-TH" if it["lang"] == "th" else "en-US"
        return f'data-text="{text}" data-lang="{lang}"'

    def tick(m, dup=False):
        cls = "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat")
        arrow = "▲" if m["pct"] > 0 else ("▼" if m["pct"] < 0 else "▬")
        # ชุดที่สองมีไว้ให้ marquee วนต่อเนื่อง ไม่ต้องให้ screen reader/แป้น Tab อ่านซ้ำ
        extra = ' dup" tabindex="-1" aria-hidden="true' if dup else ''
        return f"""<button class="tick{extra}" type="button" data-label="{html.escape(m['label'], quote=True)}"
      title="Related news for {html.escape(m['label'], quote=True)}"><span class="t-label">{html.escape(m['label'])}</span> <span class="t-price">{m['price']}</span> <span class="t-chg {cls}">{arrow} {m.get('chg_str', '—')}</span> <span class="t-pct {cls}">{m['pct_str']}</span></button>"""

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

    def kw_tile(k, i):
        """โมเสกคำที่ถูกพูดถึง — ขนาดช่องไล่ตามความถี่ ใช้ภาพข่าวที่พูดถึงคำนั้นเป็นพื้นหลัง"""
        size = "kw-xl" if i == 0 else ("kw-lg" if i < 3 else ("kw-md" if i < 7 else "kw-sm"))
        img = (f'<img src="{html.escape(k["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if k.get("image") else ""
        tag = "a" if k.get("link") else "span"
        href = (f' href="{html.escape(k["link"])}" target="_blank" rel="noopener"'
                f' title="{html.escape(k["title"], quote=True)}"') if k.get("link") else ""
        return (f'<{tag} class="kw {size} pf-{k["cat"]}"{href}>{img}'
                f'<span class="kw-scrim"></span>'
                f'<span class="kw-t">{html.escape(k["w"])}<b>{k["n"]}</b></span></{tag}>')

    # ── ช่องที่กำลังถ่ายทอดสด — การ์ดใหญ่แบบหน้าแรกสตรีมมิ่ง ──
    streams = streams or []

    def live_card(s):
        return f"""<a class="lcard" href="{html.escape(s['url'])}" target="_blank" rel="noopener">
      <span class="lcard-thumb">
        <img src="{html.escape(s['thumb'])}" loading="lazy" alt=""
             onerror="this.onerror=null;this.src='{html.escape(s['thumb_alt'])}'">
        <span class="lcard-badge"><i></i>LIVE</span>
        <span class="lcard-play"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5l11 7-11 7z"/></svg></span>
      </span>
      <span class="lcard-ch">{html.escape(s['name'])}<span class="lcard-flag">{'TH' if s['group'] == 'th' else 'INTL'}</span></span>
      <span class="lcard-t">{html.escape(s['title'])}</span>
    </a>"""

    def live_tv_row(rid):
        if not streams:
            return ""
        return f"""<section class="row">
  <div class="row-head">
    <h2>ON AIR<span class="row-n">{len(streams)}</span></h2>
    <div class="row-tools">
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',-1)" aria-label="Scroll left">‹</button>
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',1)" aria-label="Scroll right">›</button>
    </div>
  </div>
  <div class="row-track lrow" id="{rid}">{''.join(live_card(s) for s in streams)}</div>
</section>"""

    # หน้า LIVE รวมข่าวสดของทั้งสองฝั่ง — มีเมนูให้เฉพาะตอนที่มีข่าวสดจริง
    live_all = sorted([i for i in news if is_live(i)], key=lambda x: x["dt"], reverse=True)[:30]
    live_tab = ('<button class="tab tab-icon tab-live" type="button" draggable="true" '
                'data-id="live" onclick="openLive()">'
                f'<span class="live-dot-sm"></span>LIVE'
                f'<span class="tab-n">{len(live_all) + len(streams)}</span>'
                '</button>') if (live_all or streams) else ""
    live_page = "".join(
        f"""<a class="cnews-row" href="{html.escape(i['link'])}" target="_blank" rel="noopener">
      {cat_icon(i['cat'], 'ci-sm')}
      <span class="cnews-t">{html.escape(i['title'])}
        <span class="cnews-m">{html.escape(i['source'])} · {i['age']}
          · {'THAI' if i['scope'] == 'th' else 'GLOBAL'}</span></span></a>"""
        for i in live_all)
    # ไม่มีข่าวสด ก็ไม่ต้องมีหน้า LIVE ในหน้าเว็บเลย
    live_modal = f"""<div id="lmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="Live news">
    <div class="cmodal-head">
      <button type="button" class="backbtn" onclick="closeLive()" aria-label="Back">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>
      </button>
      <div class="cmodal-title">
        <h3><span class="live live-dot">LIVE</span> NEWSFEED</h3>
        <div class="cmodal-price">{len(streams)} channels on air ·
          {len(live_all)} stories in the last {LIVE_WINDOW_MIN} minutes</div>
      </div>
    </div>
    <div class="live-page">
      {live_tv_row('row-live-tv-modal')}
      {f'<div class="cnews-head">LATEST WIRE</div><div class="cnews-list live-list">{live_page}</div>' if live_all else ''}
    </div>
  </div>
</div>""" if (live_all or streams) else ""

    # ── หน้าแรกแบบหนังสือพิมพ์ (เลือกแบบ B จากผลเทียบเลย์เอาต์) ──
    fp = {}
    for sc, _lb in SCOPES:
        pool = sorted((i for i in news if i["scope"] == sc),
                      key=lambda x: x["dt"], reverse=True)
        # ฝั่งต่างประเทศเอาข่าวจากสำนักข่าวต่างประเทศจริงๆ ขึ้นก่อน ไม่ใช่สื่อไทยที่เล่าเรื่องนอก
        # (ใช้เกณฑ์เดียวกับแถว LATEST ของ variant A — ไม่งั้นคอลัมน์ INTERNATIONAL
        #  จะขึ้นด้วยข่าวไทยภาษาไทย ซึ่งทำให้การ "แยกไทย/เทศ" ไม่มีความหมาย)
        if sc == "intl":
            pool = ([i for i in pool if i["lang"] != "th"]
                    + [i for i in pool if i["lang"] == "th"])
        lead = next((i for i in pool if i.get("image")), pool[0] if pool else None)
        rest = [i for i in pool if i is not lead]
        subs = [i for i in rest if i.get("image")][:4]
        seen = {id(x) for x in subs}
        briefs = [i for i in rest if id(i) not in seen][:10]
        fp[sc] = {"lead": lead, "subs": subs, "briefs": briefs, "n": len(pool)}

    def fp_lead(it):
        if not it:
            return ""
        img = (f'<img class="fp-lead-img" src="{html.escape(it["image"])}" loading="lazy"'
               f' alt="" onerror="this.remove()">') if it.get("image") else ""
        place = f' · {html.escape(it["place"])}' if it["place"] else ""
        deck = f'<p class="fp-deck">{html.escape(it["summary"])}</p>' if it["summary"] else ""
        return f"""<article class="fp-item fp-lead">
      <a class="fp-a" href="{html.escape(it['link'])}" target="_blank" rel="noopener">
        {img}
        <span class="fp-kicker">{cat_icon(it['cat'], 'ci-sm')}{CAT_LABELS[it['cat']]}</span>
        <h3 class="fp-lead-t">{html.escape(it['title'])}</h3>
        {deck}
        <span class="fp-meta">{html.escape(it['source'])}{place} · {it['age']}</span>
      </a>
      <button class="speak fp-speak" type="button" title="Listen" {speak_attrs(it)}>🔊</button>
    </article>"""

    def fp_sub(it):
        img = (f'<img src="{html.escape(it["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        return f"""<article class="fp-item fp-sub">
      <a class="fp-a" href="{html.escape(it['link'])}" target="_blank" rel="noopener">
        <span class="fp-sub-thumb pf-{it['cat']}">{cat_icon(it['cat'], 'ci-lg')}{img}</span>
        <span class="fp-sub-body">
          <span class="fp-kicker">{CAT_LABELS[it['cat']]}</span>
          <h4 class="fp-sub-t">{html.escape(it['title'])}</h4>
          <span class="fp-meta">{html.escape(it['source'])} · {it['age']}</span>
        </span>
      </a>
    </article>"""

    def fp_brief(it):
        # ข่าวย่อยส่วนหนึ่งไม่มีรูปมากับฟีด (Google News ส่วนใหญ่ไม่ส่งมา) —
        # ตกไปใช้ไอคอนหมวดบนพื้นไล่สีแทน ทุกบรรทัดจะได้มีภาพเท่ากันหมด ไม่แหว่งเป็นบางอัน
        img = (f'<img src="{html.escape(it["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        return (f'<a class="fp-item fp-brief" href="{html.escape(it["link"])}"'
                f' target="_blank" rel="noopener">'
                f'<span class="fp-brief-thumb pf-{it["cat"]}">'
                f'{cat_icon(it["cat"], "ci-sm")}{img}</span>'
                f'<span class="fp-brief-body">'
                f'<span class="fp-brief-t">{html.escape(it["title"])}</span>'
                f'<span class="fp-meta">{html.escape(it["source"])} · {it["age"]}</span></span></a>')

    def fp_col(sc, label):
        d = fp[sc]
        if not d["lead"]:
            return ""
        flag = "TH" if sc == "th" else "INTL"
        briefs = "".join(fp_brief(i) for i in d["briefs"])
        briefs_block = (f'<div class="fp-briefs"><h4 class="fp-briefs-h">MORE HEADLINES</h4>'
                        f'{briefs}</div>') if briefs else ""
        return f"""<section class="scope-group fp-col" data-scope="{sc}">
  <h2 class="fp-sec-h">{html.escape(label)}<span class="scope-flag">{flag}</span>
    <span class="fp-sec-n">{d['n']}</span></h2>
  {fp_lead(d['lead'])}
  <div class="fp-subs">{''.join(fp_sub(i) for i in d['subs'])}</div>
  {briefs_block}
</section>"""

    front_page = f"""<div class="fp-dateline">
  <span>{NOW.strftime('%A, %d %B %Y')}</span>
  <span class="fp-dateline-mid">DAILY EDITION</span>
  <span>{len(news)} stories in 24h · updated {NOW.strftime('%H:%M')}</span>
</div>
<div class="fp-cols">{''.join(fp_col(sc, lb) for sc, lb in SCOPES)}</div>"""

    next_run = (NOW + timedelta(minutes=REBUILD_MIN)).strftime("%H:%M")
    markers_json = json.dumps(markers, ensure_ascii=False)
    infl_json = json.dumps(infl or {}, ensure_ascii=False)
    pulse = conflict_pulse(news)
    pulse_json = json.dumps(pulse, ensure_ascii=False)
    icons_json = json.dumps({c: cat_icon(c, "ci-sm") for c in CAT_NAMES}, ensure_ascii=False)
    tnews_json = json.dumps(
        {m["label"]: {"price": m["price"], "pct": m["pct_str"], "pctv": round(m["pct"], 4),
                      "group": m.get("group", "intl"),
                      "dir": "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat"),
                      "fund": m.get("fund") or {}, "news": m.get("news") or []}
         for m in markets}, ensure_ascii=False)
    charts_json = json.dumps(charts or {}, ensure_ascii=False)
    logos_json = json.dumps(logos or {}, ensure_ascii=False)
    fin_at_str = ""
    if fin_at:
        try:
            fin_at_str = datetime.fromisoformat(fin_at).strftime("%d %b %Y · %H:%M")
        except Exception:
            pass
    fin_at_json = json.dumps(fin_at_str, ensure_ascii=False)
    page_desc = f"{len(news)} economy, politics, business and environment stories in 24h from {len(FEEDS)} sources · updated {NOW.strftime('%d %b %Y %H:%M')}"
    # ตัว T แบบเซริฟในกรอบเส้นคู่ อย่างหัวหนังสือพิมพ์
    favicon = ("data:image/svg+xml,"
               "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E"
               "%3Crect width='100' height='100' fill='%230A0E1A'/%3E"
               "%3Crect x='7' y='7' width='86' height='86' fill='none'"
               " stroke='%23C6A961' stroke-width='5'/%3E"
               "%3Crect x='16' y='16' width='68' height='68' fill='none'"
               " stroke='%23C6A961' stroke-width='1.6'/%3E"
               "%3Cg fill='%23F4EFE3'%3E"
               "%3Crect x='26' y='30' width='48' height='10'/%3E"
               "%3Crect x='45' y='30' width='10' height='40'/%3E"
               "%3Crect x='37' y='64' width='26' height='6'/%3E%3C/g%3E%3C/svg%3E")

    return f"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="900">
<title>The Tribune · Thai &amp; International Business News</title>
<meta name="description" content="{html.escape(page_desc)}">
<meta property="og:type" content="website">
<meta property="og:title" content="The Tribune · Thai &amp; International Business News">
<meta property="og:description" content="{html.escape(page_desc)}">
<meta property="og:url" content="https://netflixss266-lang.github.io/econ-monitor/">
<meta name="twitter:card" content="summary">
<meta name="theme-color" content="#0A0E1A">
<link rel="icon" href="{favicon}">
<link rel="manifest" href="manifest.json">
<link rel="apple-touch-icon" href="apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Tribune">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Noto+Serif+Thai:wght@400;500;600;700&family=Playfair+Display:ital,wght@0,600;0,700;0,900;1,700&family=UnifrakturMaguntia&display=swap" rel="stylesheet">
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
  --bg:#0A0E1A; --panel:#111726; --panel2:#0E1420; --panel3:#0C1220;
  --line:#1E2637; --line2:#2A3548; --faint:#2E3A4E;
  --ink:#E7ECF5; --mute:#7A879C; --dim:#4E5A70;
  --hover:#151C2C; --sel:#182133;
  --econ:#4C8DFF; --poli:#F5A524; --biz:#2DD4BF; --env:#4ADE80; --mixed:#9B8AFB;
  --up:#3FB68B; --down:#E5484D;
  --brass:#C6A961; --cream:#F4EFE3;
  --scrim:rgba(4,6,11,.82);
}}
*{{box-sizing:border-box;margin:0;padding:0}}
button,input{{font-family:inherit}}
/* ขนาดตัวอักษรทั้งเว็บอิงจากตรงนี้ที่เดียว — ขนาดที่เป็น rem จะขยายตามกันหมด
   ส่วนระยะห่าง/ความกว้างเป็น px จึงไม่เพี้ยนตาม (17/16 = ใหญ่ขึ้น ~6%) */
html{{font-size:17px}}
body{{background:var(--bg);color:var(--ink);
  font-family:'Noto Serif Thai',Georgia,'Times New Roman',serif;
  font-size:1rem;line-height:1.6;padding:20px;max-width:1560px;margin:0 auto;
  animation:pageIn .85s 1.9s backwards}}
a{{color:inherit;text-decoration:none}}

/* ── หัวหนังสือพิมพ์ ──────────────────────────────────── */
header{{display:flex;flex-direction:column;align-items:center;text-align:center;gap:7px;
  padding:6px 0 12px;margin-bottom:18px;
  border-top:2px solid var(--brass);border-bottom:3px double var(--brass)}}
.mast-top{{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;
  gap:9px;width:100%;padding-bottom:9px;margin-bottom:2px;
  border-bottom:1px solid var(--line);
  font-family:'Playfair Display',Georgia,serif;font-size:.63rem;font-weight:600;
  letter-spacing:.26em;text-transform:uppercase;color:var(--mute)}}
.mast-dot{{color:var(--brass);letter-spacing:0}}
.logo{{display:flex;flex-direction:column;align-items:center;gap:4px;font-weight:400}}
/* ชื่อหัวเป็นอักษรแบล็กเลตเตอร์ อย่างหัวหนังสือพิมพ์ต้นศตวรรษที่ 20 */
.logo-the{{font-family:'Playfair Display',Georgia,serif;font-style:italic;font-weight:700;
  font-size:clamp(.9rem,2.4vw,1.15rem);letter-spacing:.12em;color:var(--mute);
  margin-bottom:-4px}}
.logo-mark{{font-family:'UnifrakturMaguntia','Playfair Display',Georgia,serif;font-weight:400;
  font-size:clamp(3rem,10.5vw,5.6rem);line-height:1.02;letter-spacing:.02em;
  color:var(--cream)}}
@supports ((-webkit-background-clip:text) or (background-clip:text)){{
  .logo-mark{{background:linear-gradient(176deg,#FBF7EE 6%,var(--cream) 42%,var(--brass) 122%);
    -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}}
}}
.logo-rule{{display:flex;align-items:center;justify-content:center;gap:12px;
  width:min(760px,94vw);margin-top:2px;color:var(--brass)}}
.logo-rule::before,.logo-rule::after{{content:"";flex:1;height:1px;background:linear-gradient(90deg,transparent,var(--brass) 30%,var(--brass) 70%,transparent)}}
.logo-rule span{{font-size:.6rem;letter-spacing:.1em}}
.logo-sub{{font-family:'Playfair Display',Georgia,serif;
  font-size:clamp(.6rem,1.7vw,.78rem);font-weight:400;
  letter-spacing:.2em;text-transform:uppercase;color:var(--mute);
  max-width:900px;line-height:1.9}}
.stamp{{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:10px;
  font-family:'IBM Plex Mono',monospace;font-size:.7rem;letter-spacing:.06em;
  text-transform:uppercase;color:var(--mute)}}
.pulse{{width:7px;height:7px;border-radius:50%;background:var(--up);
  box-shadow:0 0 0 0 rgba(63,182,139,.6);animation:p 2.4s infinite}}
@keyframes p{{70%{{box-shadow:0 0 0 9px rgba(63,182,139,0)}}100%{{box-shadow:0 0 0 0 rgba(63,182,139,0)}}}}

/* แถบราคาเลื่อนไปทางซ้ายต่อเนื่องแบบรายการทีวี — วิ่งตลอด ไม่หยุดตอนชี้เมาส์ */
.tickers{{display:flex;flex-direction:column;gap:8px;margin-bottom:16px}}
.ticker-row{{display:flex;align-items:stretch;gap:8px;min-width:0}}
.ticker-tag{{flex:none;display:flex;align-items:center;padding:0 12px;border-radius:2px;
  background:var(--panel2);border:1px solid var(--line);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.06em;
  text-transform:uppercase;color:var(--mute);white-space:nowrap}}
.ticker{{flex:1;min-width:0;overflow:hidden;border:1px solid var(--line);
  border-radius:2px;background:var(--panel)}}
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
  background:var(--scrim)}}
.tmodal-box{{width:min(760px,100%);max-height:86vh;display:flex;flex-direction:column;
  background:var(--panel);border:1px solid var(--line);border-radius:2px;overflow:hidden;
  box-shadow:0 30px 80px rgba(0,0,0,.6)}}
.tmodal-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;
  padding:16px 18px 12px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.tmodal-head h3{{font-size:1.1rem;font-weight:700;letter-spacing:.02em}}
.tmodal-price{{display:flex;align-items:baseline;gap:9px;margin-top:3px;
  font-family:'IBM Plex Mono',monospace}}
#tmodal-p{{font-size:1rem}}
#tmodal-c{{font-size:.8rem}}
.tmodal-x{{flex:none;width:30px;height:30px;border-radius:2px;cursor:pointer;font-size:1.2rem;
  line-height:1;color:var(--mute);background:transparent;border:1px solid var(--line)}}
.tmodal-x:hover{{color:var(--ink);background:rgba(255,255,255,.07)}}
.backbtn{{flex:none;display:grid;place-items:center;width:34px;height:34px;border-radius:2px;
  cursor:pointer;color:var(--mute);background:var(--panel);border:1px solid var(--line)}}
.backbtn svg{{width:17px;height:17px;fill:none;stroke:currentColor;stroke-width:2.2;
  stroke-linecap:round;stroke-linejoin:round}}
.backbtn:hover{{color:var(--ink);border-color:var(--dim)}}
.tmodal-note{{padding:10px 18px;font-size:.71rem;line-height:1.5;color:var(--dim);
  border-bottom:1px solid var(--line)}}
.tmodal-note strong{{color:var(--mute);font-weight:600}}
.tmodal-list{{overflow-y:auto;padding:4px 0}}
.trow{{display:flex;align-items:center;gap:11px;padding:11px 18px;
  border-bottom:1px solid var(--line)}}
.trow:last-child{{border-bottom:0}}
.trow:hover{{background:var(--hover)}}
.trow-thumb{{width:58px;height:38px;border-radius:2px;object-fit:cover;flex:none;
  background:var(--panel2)}}
.trow-body{{flex:1;min-width:0}}
.trow-title{{font-size:.85rem;line-height:1.4;font-weight:500}}
.trow-meta{{display:flex;gap:8px;margin-top:4px;font-family:'IBM Plex Mono',monospace;
  font-size:.64rem;color:var(--dim);text-transform:uppercase}}
.score{{flex:none;min-width:52px;text-align:center;padding:5px 8px;border-radius:2px;
  font-family:'IBM Plex Mono',monospace;font-size:.78rem;font-weight:600}}
/* >50 เขียว · 10-49 เหลือง · <10 แดง */
.score.hi{{color:#8CF0C6;background:rgba(63,182,139,.16);border:1px solid rgba(63,182,139,.4)}}
.score.mid{{color:#FFD27A;background:rgba(245,165,36,.14);border:1px solid rgba(245,165,36,.38)}}
.score.low{{color:#FFA9AC;background:rgba(229,72,77,.14);border:1px solid rgba(229,72,77,.38)}}
.tmodal-empty{{padding:26px 18px;color:var(--mute);font-size:.85rem;text-align:center}}

/* ── หน้ากราฟแท่งเทียน ─────────────────────────────────── */
.chart-bar{{display:flex;align-items:center;gap:12px;margin-bottom:8px}}
.chart-open{{display:inline-flex;align-items:center;gap:8px;padding:8px 15px;border-radius:2px;
  font-family:inherit;font-size:.82rem;font-weight:600;cursor:pointer;color:var(--ink);
  background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line2);
  transition:border-color .16s,background .16s}}
.chart-open:hover{{border-color:var(--brass);background:var(--sel)}}
.chart-open svg{{width:15px;height:15px;fill:none;stroke:var(--brass);stroke-width:2;
  stroke-linecap:round}}
.chart-hint{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim)}}

#cmodal{{padding:0}}
.cmodal-box{{width:100%;height:100%;display:flex;flex-direction:column;
  background:var(--panel);border:0;border-radius:0;overflow:hidden}}
.cmodal-head{{display:flex;align-items:center;gap:14px;flex-wrap:wrap;
  padding:13px 16px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.cmodal-title{{min-width:150px}}
/* ชื่อเต็มบริษัทใต้ตัวย่อ — ตัวย่ออย่างเดียวจำไม่ได้ว่าเป็นบริษัทอะไร */
.sym-full{{margin:1px 0 3px;font-family:'Noto Serif Thai',Georgia,serif;font-size:.86rem;
  line-height:1.35;color:var(--mute);max-width:460px}}
.cmodal-title h3{{font-size:1.05rem;font-weight:700}}
.cmodal-price{{display:flex;gap:9px;font-family:'IBM Plex Mono',monospace;font-size:.78rem}}
.tfbar{{display:flex;gap:4px;flex:1;flex-wrap:wrap}}
.tfbtn{{padding:5px 11px;border-radius:2px;cursor:pointer;font-family:'IBM Plex Mono',monospace;
  font-size:.72rem;color:var(--mute);background:transparent;border:1px solid var(--line)}}
.tfbtn:hover{{color:var(--ink)}}
.tfbtn.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:600}}
.cmodal-body{{flex:1;display:flex;min-height:0}}
.cmodal-pick{{width:210px;flex:none;display:flex;flex-direction:column;min-height:0;
  border-right:1px solid var(--line)}}
.csearch{{margin:8px 10px;width:auto;flex:none}}
.cmodal-list{{flex:1;overflow-y:auto;padding:2px 0}}
.cmodal-list .cnone{{padding:16px 14px;color:var(--dim);font-size:.76rem}}
/* หัวกลุ่ม THAILAND/GLOBAL พับเก็บได้ — ลูกศรหมุนตามสถานะ */
.cgroup{{display:flex;align-items:center;gap:6px;padding:9px 14px 5px;cursor:pointer;
  user-select:none;font-family:'IBM Plex Mono',monospace;font-size:.62rem;
  letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}}
.cgroup:hover{{color:var(--mute)}}
.cgroup .fold-caret{{width:11px;height:11px;flex:none;fill:none;stroke:currentColor;
  stroke-width:2.6;transition:transform .18s}}
.cgroup.folded .fold-caret{{transform:rotate(-90deg)}}
.cgroup.folded + .cfav-group{{display:none}}
.citem{{display:flex;align-items:center;gap:8px;width:100%;padding:7px 10px 7px 14px;
  cursor:pointer;background:none;border:0;color:var(--mute);font-family:inherit;
  font-size:.79rem;text-align:left}}
/* ลากจัดลำดับได้เฉพาะโหมด FAVORITES — โหมด ALL เรียงตามราคาสด ลากไปก็คืนที่เดิมทันที */
.citem[draggable="true"]{{cursor:grab}}
.citem.dragging{{opacity:.4;cursor:grabbing}}
.citem .cname{{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
/* แถบเลือกว่าจะโชว์เฉพาะตัวโปรด หรือทั้งตลาด */
.cfav-bar{{display:flex;gap:4px;padding:9px 10px 0}}
.cfav-tab{{flex:1;padding:6px 8px;border-radius:2px;cursor:pointer;
  font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.06em;
  color:var(--mute);background:transparent;border:1px solid var(--line)}}
.cfav-tab:hover{{color:var(--ink)}}
.cfav-tab.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:700}}
.cpct{{font-family:'IBM Plex Mono',monospace;font-size:.7rem}}
/* ปุ่มสลับวิธีเรียงหุ้นไทย อยู่ในหัวข้อกลุ่มซึ่งกดพับได้ ต้องดูออกว่าเป็นปุ่มแยกอีกอัน */
.th-sort{{float:right;margin:-1px 0 0;padding:1px 6px;border:1px solid var(--line);
  border-radius:2px;background:none;cursor:pointer;color:var(--dim);
  font-family:'IBM Plex Mono',monospace;font-size:.55rem;letter-spacing:.04em}}
.th-sort:hover{{color:var(--brass);border-color:var(--brass)}}
/* อัตราปันผลข้างชื่อหุ้นไทยในแท็บรายการโปรด — บอกว่าทำไมลำดับถึงเรียงแบบนี้
   ถ้าเรียงตามค่าที่มองไม่เห็น ลำดับจะดูมั่วไปเลย */
.cyld{{flex:none;font-family:'IBM Plex Mono',monospace;font-size:.56rem;
  color:var(--brass);opacity:.75;letter-spacing:0}}
/* แถวที่มีป้ายปันผลมีตัวเลขสองชุดในแถวกว้าง ~200px — บีบช่องไฟกับตัวเลขลง
   เพื่อให้ชื่อหุ้นเต็มๆ ยังอยู่ครบ ชื่อย่อสำคัญกว่าตัวเลขที่เป็นของประกอบ */
.citem:has(.cyld){{gap:5px}}
.citem:has(.cyld) .cname{{min-width:3.6em}}
.citem:has(.cyld) .cpct{{font-size:.65rem}}
.cfav{{flex:none;width:20px;text-align:center;font-size:.82rem;line-height:1;
  color:var(--faint);cursor:pointer}}
.cfav:hover{{color:var(--brass)}}
.cfav.on{{color:var(--brass)}}
.cnone-hint{{padding:16px 14px;color:var(--dim);font-size:.72rem;line-height:1.6}}
/* โลโก้สินทรัพย์ — ถ้าไม่มีรูปจะเหลืออักษรย่อที่วางไว้ข้างล่าง */
.clogo{{position:relative;flex:none;display:grid;place-items:center;
  width:20px;height:20px;border-radius:2px;overflow:hidden;
  background:var(--panel3);font-family:'IBM Plex Mono',monospace;font-size:.56rem;
  font-weight:600;color:var(--mute)}}
.clogo img{{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;
  background:#fff}}
.citem:hover{{background:var(--hover);color:var(--ink)}}
.citem.on{{background:var(--sel);color:var(--ink);box-shadow:inset 2px 0 0 var(--brass)}}
.cmodal-chart{{flex:1;min-width:0;display:flex;flex-direction:column;padding:10px 14px 8px}}
#cchart{{flex:1;min-height:0}}
/* ปุ่มช่วงเวลา + ชนิดกราฟ อยู่มุมขวาล่างของกราฟ */
.cbottom{{display:flex;align-items:flex-end;justify-content:space-between;gap:14px;
  flex-wrap:wrap;margin-top:4px}}
.cctrl{{display:flex;gap:16px;flex-wrap:wrap;justify-content:flex-end;margin-left:auto}}
/* ติดป้ายกำกับให้ปุ่มทั้งสองชุด — ไม่งั้นสลับเป็นกราฟเส้นได้แต่ไม่มีใครหาเจอ */
.cctrl-g{{display:flex;flex-direction:column;gap:5px}}
.cctrl-lbl{{font-family:'IBM Plex Mono',monospace;font-size:.56rem;letter-spacing:.16em;
  text-transform:uppercase;color:var(--dim)}}
.cctrl-g .tfbar{{flex:none}}
.map-modal-body{{flex:1;display:flex;flex-direction:column;min-height:0}}
.map-modal-body .map-wrap{{flex:1;height:auto;min-height:0}}
.map-modal-body #hotspot-detail{{max-height:210px}}
#cchart svg{{width:100%;height:100%;display:block;cursor:crosshair;touch-action:none}}
.c-grid line{{stroke:var(--line);stroke-width:1;shape-rendering:crispEdges}}
.c-axis text{{fill:var(--dim);font-family:'IBM Plex Mono',monospace;font-size:10px}}
.c-up{{fill:var(--up);stroke:var(--up)}}
.c-down{{fill:var(--down);stroke:var(--down)}}
.c-cross{{stroke:var(--dim);stroke-width:1;stroke-dasharray:3 3;pointer-events:none}}
.creadout{{min-height:19px;font-family:'IBM Plex Mono',monospace;font-size:.7rem;
  color:var(--mute);display:flex;gap:12px;flex-wrap:wrap}}
.creadout b{{color:var(--ink);font-weight:500}}
.cmodal-note{{font-size:.65rem;line-height:1.5;color:var(--dim);margin-top:3px;max-width:560px}}
.cempty{{display:grid;place-items:center;height:100%;color:var(--mute);font-size:.85rem}}

/* ── แถบเครื่องมือเทคนิค ยื่นออกมาที่ขอบซ้ายสุด (อย่างโปรแกรมเทรด) ── */
.crail{{position:relative;flex:none;width:46px;display:flex;flex-direction:column;
  align-items:center;gap:5px;padding:9px 0;background:var(--panel3);
  border-right:1px solid var(--line)}}
.crbtn{{position:relative;width:32px;height:30px;display:grid;place-items:center;
  cursor:pointer;color:var(--mute);background:transparent;border:1px solid transparent;
  border-radius:2px;font-family:'IBM Plex Mono',monospace;font-size:.58rem;
  letter-spacing:.04em;font-weight:600}}
.crbtn b{{font-family:'Playfair Display',Georgia,serif;font-size:.95rem;font-style:italic}}
.crbtn:hover{{color:var(--ink);background:var(--hover);border-color:var(--line)}}
.crbtn.on{{color:var(--brass);border-color:var(--brass);background:var(--sel)}}
.crbadge{{position:absolute;right:-2px;top:-3px;min-width:14px;height:14px;padding:0 3px;
  display:grid;place-items:center;border-radius:7px;background:var(--brass);
  color:#0A0E1A;font-size:.52rem;font-weight:700}}
.crail-sep{{width:22px;height:1px;background:var(--line);margin:3px 0}}
.cpop{{position:absolute;left:52px;top:8px;z-index:8;width:232px;
  background:var(--panel);border:1px solid var(--line2);border-radius:2px;
  box-shadow:0 18px 44px rgba(0,0,0,.55)}}
.cpop-head{{padding:9px 12px;border-bottom:1px solid var(--line);background:var(--panel2);
  font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.16em;color:var(--mute)}}
.cpop-head span{{display:block;margin-top:3px;font-family:'Noto Serif Thai',serif;
  font-size:.58rem;letter-spacing:0;color:var(--dim)}}
/* กล่องอธิบายอินดิเคเตอร์ — ชี้ค้างบนคอม / กดค้างบนมือถือ */
.cpop-tip{{position:fixed;z-index:70;width:min(330px,86vw);
  background:var(--panel);border:1px solid var(--brass);border-radius:2px;
  box-shadow:0 22px 54px rgba(0,0,0,.62);padding:12px 14px}}
.tip-h{{font-family:'Playfair Display',Georgia,serif;font-size:.95rem;font-weight:700;
  color:var(--cream)}}
.tip-f{{font-family:'IBM Plex Mono',monospace;font-size:.62rem;color:var(--brass);
  margin:2px 0 9px;padding-bottom:8px;border-bottom:1px solid var(--line)}}
.tip-r{{font-size:.76rem;line-height:1.55;color:var(--mute);margin-bottom:7px}}
.tip-r:last-child{{margin-bottom:0}}
.tip-r b{{display:block;font-family:'IBM Plex Mono',monospace;font-size:.56rem;
  letter-spacing:.14em;text-transform:uppercase;color:var(--dim);font-weight:500}}
.cpop-body{{max-height:340px;overflow-y:auto}}
.cpop-grp{{padding:8px 12px 3px;font-family:'IBM Plex Mono',monospace;font-size:.55rem;
  letter-spacing:.14em;color:var(--dim)}}
.cpop-row{{display:flex;align-items:center;gap:9px;width:100%;padding:6px 12px;cursor:pointer;
  background:none;border:0;color:var(--mute);font-family:inherit;font-size:.76rem;text-align:left}}
.cpop-row:hover{{background:var(--hover);color:var(--ink)}}
.cpop-row .sw{{width:13px;height:3px;border-radius:2px;flex:none}}
.cpop-row .ck{{margin-left:auto;font-size:.72rem;color:var(--brass);opacity:0}}
.cpop-row.on{{color:var(--ink)}}
.cpop-row.on .ck{{opacity:1}}
.cpop-clear{{width:100%;padding:8px;cursor:pointer;color:var(--dim);background:var(--panel2);
  border:0;border-top:1px solid var(--line);font-family:'IBM Plex Mono',monospace;
  font-size:.58rem;letter-spacing:.12em}}
.cpop-clear:hover{{color:var(--down)}}

/* ── ปุ่ม + หน้าต่างงบการเงิน (เต็มจอ อย่างหน้ากราฟ) ───────── */
#finmodal{{padding:0}}
.fin-btn{{display:inline-flex;align-items:center;gap:7px;padding:8px 13px;
  margin-left:auto;flex:none;cursor:pointer;border-radius:2px;
  font-family:'IBM Plex Mono',monospace;font-size:.7rem;letter-spacing:.08em;
  font-weight:600;color:var(--mute);background:var(--panel2);border:1px solid var(--line)}}
.fin-btn:hover{{color:var(--ink);border-color:var(--brass)}}
.fin-btn svg{{width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:2;
  stroke-linecap:round;stroke-linejoin:round}}
.fin-nav{{flex:none;font-size:1.2rem}}
/* ── ขยายกราฟเต็มหน้าต่าง — พับรายชื่อหุ้นกับคอลัมน์ข่าวเก็บไว้ ─────
   ใช้ hidden ผ่าน CSS ไม่ใช่ลบ DOM ทิ้ง กราฟจะได้ไม่ต้องวาดใหม่ตอนกดกลับ */
.expand-btn{{margin-left:0}}
.cmodal-box.chart-full .cmodal-pick,
.cmodal-box.chart-full .cmodal-side,
.cmodal-box.chart-full .cmodal-tape{{display:none}}
.cmodal-box.chart-full .cmodal-chart{{padding:12px 18px 10px}}
.expand-btn.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass)}}
/* แถบสรุปช่วง 10 ปี โผล่เฉพาะตอนขยายเต็ม */
.cfull-bar{{display:none;flex-wrap:wrap;gap:22px;padding:10px 18px;
  border-bottom:1px solid var(--line);background:var(--panel2)}}
.cmodal-box.chart-full .cfull-bar{{display:flex}}
.cfull-stat{{display:flex;flex-direction:column;gap:2px}}
.cfull-stat b{{font-family:'IBM Plex Mono',monospace;font-size:1.02rem;font-weight:700;
  color:var(--ink);line-height:1.15}}
.cfull-stat span{{font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.13em;
  text-transform:uppercase;color:var(--dim)}}
.cfull-stat b.up{{color:var(--up)}}
.cfull-stat b.down{{color:var(--down)}}
.cfull-note{{margin-left:auto;align-self:center;font-size:.76rem;color:var(--dim);
  max-width:420px;line-height:1.5}}
.fin-tabs{{display:flex;gap:4px;flex:none;margin-left:12px}}
.fin-tab{{padding:9px 16px;border-radius:2px;cursor:pointer;
  font-family:'IBM Plex Mono',monospace;font-size:.72rem;letter-spacing:.08em;
  color:var(--mute);background:transparent;border:1px solid var(--line)}}
.fin-tab:hover{{color:var(--ink)}}
.fin-tab.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:700}}
/* ทั้งแดชบอร์ดเลื่อนแนวตั้งเป็นก้อนเดียว ส่วนตารางเป็นแผงย่อยที่เลื่อนในตัวเอง —
   หัวตารางค้างได้เพราะ .fin-table-wrap เป็น scroll container ของตัวมันเอง
   (ตั้ง overflow แค่แกนเดียวบน element เดียวกันใช้ไม่ได้ เบราว์เซอร์จะปัดอีกแกนเป็น auto
   ให้เองเสมอ ทำให้ตัวเลื่อนจริงกลายเป็น element นั้นแทน sticky header เลยพังตาม) */
.fin-body{{flex:1;min-height:0;overflow-y:auto;
  padding:0 clamp(16px,4vw,48px) 30px}}
.fin-inner{{max-width:1460px;margin:0 auto}}
.fin-body>.cempty,.fin-body>.fin-empty{{min-height:60vh}}
.fin-note{{padding:10px 18px;font-size:.72rem;line-height:1.6;color:var(--dim);
  border-top:1px solid var(--line);background:var(--panel2)}}
#fin-checked{{display:block;margin-top:3px;color:var(--mute);font-family:'IBM Plex Mono',monospace;
  font-size:.66rem;letter-spacing:.04em}}
.fin-empty{{display:grid;place-items:center;color:var(--mute);
  font-size:.95rem;text-align:center;gap:8px;padding:30px}}

/* ── แถบเครื่องมือแดชบอร์ด — ค้างบนสุดตอนเลื่อน ─────────────── */
.fin-toolbar{{position:sticky;top:0;z-index:6;display:flex;align-items:center;
  flex-wrap:wrap;gap:10px;padding:15px 0 13px;margin-bottom:22px;
  background:var(--panel);border-bottom:1px solid var(--line)}}
.fin-jump{{display:flex;gap:6px;flex-wrap:wrap}}
.fin-jump button{{padding:7px 13px;border-radius:2px;cursor:pointer;
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.09em;
  color:var(--mute);background:var(--panel2);border:1px solid var(--line)}}
.fin-jump button:hover{{color:var(--ink);border-color:var(--brass)}}
.fin-cmp{{display:flex;align-items:center;gap:8px;margin-left:auto;
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.06em;
  color:var(--mute)}}
.fin-cmp select{{padding:7px 11px;border-radius:2px;background:var(--panel2);
  border:1px solid var(--line);color:var(--ink);font-family:inherit;font-size:.72rem;
  cursor:pointer;max-width:210px}}
.fin-cmp select:hover{{border-color:var(--brass)}}
.fin-cmpbar{{display:flex;align-items:center;gap:18px;flex-wrap:wrap;
  padding:11px 15px;margin:-8px 0 22px;border-radius:2px;
  background:var(--panel2);border:1px solid var(--line);border-left:2px solid var(--econ)}}
.fin-cmpbar em{{font-style:normal;font-size:.72rem;color:var(--dim);margin-left:auto}}

/* ── หัวข้อวิเคราะห์แต่ละกลุ่ม — ตัวใหญ่ อ่านแล้วรู้ทันทีว่ากำลังดูอะไร ── */
.fin-section{{margin-bottom:40px;scroll-margin-top:74px}}
.fin-section-h{{display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;
  padding-bottom:11px;border-bottom:2px solid var(--brass);margin-bottom:20px}}
.fin-section-h b{{font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;
  font-size:1.45rem;letter-spacing:.05em;text-transform:uppercase;
  color:var(--cream);font-weight:700}}
.fin-section-h span{{font-size:.86rem;color:var(--mute);letter-spacing:0}}

/* ── การ์ด KPI ตัวเลขงวดล่าสุด + กล่องบันทึกอ่านงบข้างๆ ───────── */
.fin-kpi-wrap{{display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:16px;
  align-items:start}}
@media(max-width:1150px){{.fin-kpi-wrap{{grid-template-columns:1fr}}}}
.fin-kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}}
/* กล่องความเห็น — ทำให้ดูเป็น "บันทึกของกอง บก." ไม่ใช่ตัวเลขอีกก้อน
   เส้นทองด้านซ้าย + ตัวเอียงเซริฟ ให้อ่านออกทันทีว่าเป็นคำอธิบาย ไม่ใช่ข้อมูลดิบ */
.fin-read{{background:var(--panel2);border:1px solid var(--line);
  border-left:3px solid var(--brass);border-radius:2px;padding:15px 17px 16px}}
.fin-read-h{{display:flex;align-items:center;gap:8px;padding-bottom:9px;margin-bottom:11px;
  border-bottom:1px solid var(--line2);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.15em;
  text-transform:uppercase;color:var(--brass);font-weight:700}}
.fin-read-h::before{{content:'§';font-size:.9rem;line-height:1}}
.fin-lang-sw{{display:flex;gap:0;margin-left:auto;border:1px solid var(--line);
  border-radius:2px;overflow:hidden}}
.fin-lang{{padding:3px 9px;cursor:pointer;background:transparent;border:0;
  font-family:'IBM Plex Mono',monospace;font-size:.58rem;letter-spacing:.08em;
  color:var(--mute)}}
.fin-lang:hover{{color:var(--ink)}}
.fin-lang.on{{color:#0A0E1A;background:var(--brass);font-weight:700}}
.fin-read p{{font-size:.95rem;line-height:1.62;color:var(--ink);margin-bottom:10px}}
.fin-read p:last-of-type{{margin-bottom:0}}
.fin-read b{{color:var(--cream);font-weight:700}}
.fin-read .up{{color:var(--up);font-weight:600}}
.fin-read .down{{color:var(--down);font-weight:600}}
.fin-read-watch{{margin-top:13px;padding-top:11px;border-top:1px solid var(--line2)}}
.fin-read-watch span{{display:block;margin-bottom:6px;
  font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.15em;
  text-transform:uppercase;color:var(--mute)}}
.fin-read-watch li{{list-style:none;position:relative;padding-left:15px;margin-bottom:6px;
  font-size:.88rem;line-height:1.55;color:var(--mute)}}
.fin-read-watch li::before{{content:'›';position:absolute;left:2px;color:var(--brass)}}
.fin-read-foot{{margin-top:13px;padding-top:10px;border-top:1px solid var(--line2);
  font-size:.76rem;line-height:1.55;color:var(--dim)}}
.fin-kpi{{position:relative;background:var(--panel2);border:1px solid var(--line);
  border-radius:2px;padding:15px 17px 16px;overflow:hidden}}
.fin-kpi::before{{content:'';position:absolute;left:0;top:0;bottom:0;width:2px;
  background:var(--brass);opacity:.55}}
.fin-kpi-lbl{{font-family:'IBM Plex Mono',monospace;font-size:.64rem;letter-spacing:.11em;
  text-transform:uppercase;color:var(--mute);margin-bottom:10px}}
.fin-kpi-val{{font-family:'IBM Plex Mono',monospace;font-size:1.55rem;font-weight:700;
  color:var(--ink);line-height:1.1;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}}
.fin-kpi .fin-delta{{font-size:.76rem;margin-top:8px}}
.fin-kpi-cmp{{display:flex;justify-content:space-between;gap:8px;margin-top:10px;
  padding-top:9px;border-top:1px solid var(--line);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--mute)}}
.fin-kpi-cmp b{{color:var(--mixed);font-weight:600;white-space:nowrap}}
.fin-kpi-cmp span{{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}

/* ── แผงกราฟ ────────────────────────────────────────────── */
.fin-panel-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));
  gap:16px}}
/* container-type ทำให้ตัวเลขในแท่งย่อตามความกว้าง "ของการ์ด" ได้ ไม่ใช่ตามขนาดจอ —
   ปัญหาจริงคือการ์ดในกริด 4 คอลัมน์กว้าง 322px ช่องต่องวดเหลือ 50px ไม่ใช่จอเล็ก */
.fin-chart{{background:var(--panel2);border:1px solid var(--line);border-radius:2px;
  padding:15px 17px 14px;min-width:0;container-type:inline-size}}
.fin-chart-wide{{grid-column:1/-1}}
/* การ์ดที่กดเปิดฉบับเต็มได้ — ยกขึ้นตอนชี้ อย่างการ์ดหนังที่กดแล้วขยาย */
.fin-chart.tap{{cursor:pointer;position:relative;
  transition:transform .18s,border-color .18s,box-shadow .18s}}
.fin-chart.tap:hover,.fin-chart.tap:focus-visible{{transform:translateY(-2px);
  border-color:var(--brass);box-shadow:0 12px 30px rgba(0,0,0,.45);outline:none}}
.fin-chart-more{{position:absolute;right:12px;bottom:10px;display:flex;align-items:center;
  gap:5px;opacity:0;transition:opacity .18s;
  font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.12em;
  text-transform:uppercase;color:var(--brass);pointer-events:none}}
.fin-chart.tap:hover .fin-chart-more,
.fin-chart.tap:focus-visible .fin-chart-more{{opacity:1}}

/* ── ฉบับเต็มรายหมวด ─────────────────────────────────────── */
.met-body{{flex:1;min-height:0;overflow-y:auto;padding:0 clamp(16px,4vw,48px) 34px}}
.met-inner{{max-width:1460px;margin:0 auto}}
.met-stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;
  padding:20px 0 24px}}
.met-stat{{background:var(--panel2);border:1px solid var(--line);border-left:2px solid var(--brass);
  border-radius:2px;padding:13px 15px}}
.met-stat span{{display:block;margin-bottom:7px;font-family:'IBM Plex Mono',monospace;
  font-size:.62rem;letter-spacing:.11em;text-transform:uppercase;color:var(--mute)}}
.met-stat b{{font-family:'IBM Plex Mono',monospace;font-size:1.32rem;font-weight:700;
  color:var(--ink);line-height:1.15}}
.met-stat b.up{{color:var(--up)}}
.met-stat b.down{{color:var(--down)}}
.met-stat i{{display:block;margin-top:4px;font-style:normal;font-size:.7rem;color:var(--dim)}}
/* กราฟในฉบับเต็มสูงกว่าปกติ ให้เห็นความต่างของแต่ละงวดชัดขึ้น */
.met-body .fin-chart-plot{{height:280px}}
.met-body .fin-bar-track{{height:250px}}
.met-body .fin-chart{{margin-bottom:22px}}
.met-empty{{padding:22px 0;color:var(--dim);font-size:.9rem}}
.met-tbl{{width:100%;border-collapse:separate;border-spacing:0;margin-bottom:26px;
  font-family:'IBM Plex Mono',monospace;font-variant-numeric:tabular-nums;
  border:1px solid var(--line);border-radius:2px;overflow:hidden}}
.met-tbl th{{padding:11px 16px;text-align:right;background:var(--panel3);color:var(--mute);
  font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;font-weight:600;
  border-bottom:1px solid var(--line2)}}
.met-tbl th:first-child,.met-tbl td:first-child{{text-align:left}}
.met-tbl td{{padding:11px 16px;text-align:right;border-bottom:1px solid var(--line);
  font-size:.95rem;color:var(--ink)}}
.met-tbl tbody tr:last-child td{{border-bottom:0}}
.met-tbl tbody tr:hover td{{background:var(--hover)}}
.met-tbl .met-na{{color:var(--dim)}}
.met-tbl .up{{color:var(--up)}}
.met-tbl .down{{color:var(--down)}}
.met-sub-h{{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;
  padding-bottom:9px;border-bottom:2px solid var(--brass);margin:6px 0 18px}}
.met-sub-h b{{font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;
  font-size:1.28rem;letter-spacing:.04em;text-transform:uppercase;color:var(--cream)}}
.met-sub-h span{{font-size:.84rem;color:var(--mute)}}
.met-note{{padding:14px 16px;margin-bottom:8px;border-radius:2px;background:var(--panel2);
  border:1px solid var(--line);border-left:2px solid var(--econ);
  font-size:.86rem;line-height:1.6;color:var(--mute)}}

/* ── เครื่องคำนวณผลตอบแทนปันผล ─────────────────────────────── */
.div-calc{{background:var(--panel2);border:1px solid var(--line);border-radius:2px;
  padding:16px 18px;margin:18px 0 24px;max-width:1000px}}
.div-calc-h{{font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;
  font-size:1.08rem;font-weight:700;color:var(--ink);margin-bottom:6px}}
.div-calc-note{{font-size:.8rem;line-height:1.55;color:var(--dim);margin-bottom:14px;max-width:60ch}}
.div-calc-row{{display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end}}
.div-calc-row label{{display:flex;flex-direction:column;gap:6px;flex:1;min-width:140px;
  font-family:'IBM Plex Mono',monospace;font-size:.62rem;letter-spacing:.08em;
  text-transform:uppercase;color:var(--mute)}}
.div-calc-row input{{padding:9px 11px;border-radius:2px;background:var(--panel);
  border:1px solid var(--line);color:var(--ink);font-family:'IBM Plex Mono',monospace;
  font-size:.92rem;font-variant-numeric:tabular-nums;width:100%}}
.div-calc-row input:focus{{outline:none;border-color:var(--brass)}}
/* งบลงทุนเป็นช่องหลัก — กว้างกว่าและมีกรอบทองเด่นกว่าอีกสองช่องที่เป็นแค่ค่าเริ่มต้นให้แก้ */
.div-calc-primary{{flex:1.6;min-width:200px}}
.div-calc-primary input{{border-color:var(--brass);font-size:1.05rem;font-weight:600}}
.div-calc-reset{{flex:none;padding:9px 16px;border-radius:2px;cursor:pointer;
  background:var(--panel);border:1px solid var(--line);color:var(--mute);
  font-family:'IBM Plex Mono',monospace;font-size:.68rem;letter-spacing:.08em}}
.div-calc-reset:hover{{color:var(--ink);border-color:var(--brass)}}
.div-calc-out{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
  gap:12px;margin-top:16px;padding-top:16px;border-top:1px solid var(--line2)}}
.dcalc-cell{{display:flex;flex-direction:column;gap:5px}}
.dcalc-cell span{{font-family:'IBM Plex Mono',monospace;font-size:.62rem;letter-spacing:.09em;
  text-transform:uppercase;color:var(--mute)}}
.dcalc-cell b{{font-family:'IBM Plex Mono',monospace;font-size:1.15rem;font-weight:700;
  color:var(--brass)}}
.dcalc-cell i{{font-style:normal;font-family:'IBM Plex Mono',monospace;font-size:.66rem;
  color:var(--dim)}}

/* ── กรอบทองวิ้งๆ — เน้นตัวเลขที่สำคัญที่สุดในหน้า ขอบทองเรืองแสงเต้นจังหวะเฉยๆ
   (ตัดแสงกวาดผ่าน/sheen ออกแล้วตามที่ขอ — เอาแค่ pulse ไม่มีเงาวิ่งผ่าน) */
@keyframes goldPulse{{
  0%,100%{{box-shadow:0 0 0 1px rgba(198,169,97,.55),0 0 9px 1px rgba(198,169,97,.22)}}
  50%{{box-shadow:0 0 0 1px rgba(244,239,227,.85),0 0 20px 4px rgba(198,169,97,.58)}}
}}
.gold-frame{{position:relative;border-color:var(--brass) !important;
  animation:goldPulse 2.6s ease-in-out infinite}}
@media(prefers-reduced-motion:reduce){{
  .gold-frame{{animation:none;
    box-shadow:0 0 0 1px rgba(198,169,97,.6),0 0 12px 2px rgba(198,169,97,.3)}}
}}
.div-pmt-n{{display:inline-block;margin-left:7px;font-size:.68rem;color:var(--dim)}}
/* ยอดแปลงเป็นบาท — เป็นของรอง ต้องอ่านออกแต่ห้ามแย่งสายตาไปจากตัวเลขสกุลจริง */
/* วางเป็นพี่น้องของ .fin-kpi-val ไม่ใช่ลูก — ตัวนั้น nowrap+overflow:hidden ยอดบาทที่ยาวกว่า
   กรอบจะโดนตัดหายไปเงียบๆ บนจอมือถือ ตรงนี้จึงยอมให้ตัดบรรทัดได้แทนที่จะโดนซ่อน */
.thb-eq{{display:block;font-family:'IBM Plex Mono',monospace;font-size:.68rem;
  color:var(--dim);font-weight:400;letter-spacing:.01em;margin-top:4px;
  overflow-wrap:anywhere}}
.fin-kpi .thb-eq{{font-size:.7rem}}
.div-hist{{margin-top:8px}}
/* หัวการ์ดกราฟ — ชื่อเรื่องเด่นชัด + บอกหน่วยไว้ใต้ชื่อ จะได้ไม่ต้องเดาว่าตัวเลขคืออะไร */
.fin-chart-h{{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;
  padding-bottom:10px;margin-bottom:14px;border-bottom:1px solid var(--line2)}}
.fin-chart-h .fin-chart-t{{display:block;font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;
  font-size:1.08rem;font-weight:700;line-height:1.25;color:var(--ink);
  letter-spacing:.01em}}
.fin-chart-h .fin-chart-u{{display:block;margin-top:3px;font-family:'IBM Plex Mono',monospace;
  font-size:.62rem;letter-spacing:.12em;text-transform:uppercase;color:var(--dim)}}
/* สถิติสรุปมุมขวาหัวการ์ด เช่น CAGR — คำนวณจากงวดแรกถึงงวดสุดท้ายที่โชว์เท่านั้น */
.fin-chart-stat{{flex:none;padding:3px 9px;border-radius:2px;
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.04em;
  font-weight:600;color:var(--brass);background:var(--sel);
  border:1px solid var(--line2);white-space:nowrap}}
.fin-chart-plot{{position:relative;height:172px;display:flex;gap:9px}}
.fin-zero-line{{position:absolute;left:0;right:0;height:1px;background:var(--line2)}}
.fin-bar-col{{flex:1;display:flex;flex-direction:column;align-items:center;gap:6px;
  min-width:0}}
/* ตัวเลขอยู่ "ในแท่ง" ไม่ใช่ลอยอยู่ข้างบน — อ่านง่ายกว่าเพราะเลขติดกับแท่งที่มันอธิบาย
   แท่งเตี้ยเกินจะใส่ตัวเลขข้างในไม่ได้ ตกไปวางเหนือแท่งแทน (คลาส .out) */
.fin-bar-num{{position:absolute;left:0;right:0;text-align:center;
  font-family:'IBM Plex Mono',monospace;font-weight:700;
  letter-spacing:-.01em;white-space:nowrap;pointer-events:none;
  font-size:.72rem;                        /* เผื่อเบราว์เซอร์ที่ยังไม่รู้จัก cqw */
  font-size:clamp(.6rem,3.5cqw,.86rem)}}
/* หมึกเข้มบนแท่งทุกสี — วัดแล้วชนะทุกกรณี (ทอง 8.5:1, น้ำเงินเทียบ 6.0:1, แดง 4.9:1)
   ส่วนตัวอักษรสว่างบนน้ำเงินได้แค่ 3.0:1 ซึ่งตกเกณฑ์ จึงไม่ใช้ */
.fin-bar-num.in{{top:4px;color:#0A0E1A}}
.fin-bar-num.out{{bottom:calc(100% + 3px);color:var(--ink)}}
.fin-bar-num.out.dn{{bottom:auto;top:calc(100% + 3px)}}
/* แท่งเรียงในราง — 1 ช่องปกติ, 2 ช่องเวลาเทียบกับอีกหุ้น (แกนเดียวกัน สเกลเดียวกัน) */
.fin-bar-track{{position:relative;width:100%;height:150px;display:flex;gap:3px}}
.fin-bar-slot{{position:relative;flex:1;min-width:0}}
.fin-bar-na{{position:absolute;inset:0;display:grid;place-items:center;
  font-family:'IBM Plex Mono',monospace;font-size:.8rem;color:var(--dim)}}
.fin-bar{{position:absolute;left:0;right:0;border-radius:2px 2px 0 0;min-height:2px;
  transition:top .25s,height .25s}}
.fin-bar.dn{{border-radius:0 0 2px 2px}}   /* แท่งติดลบ ปัดมุมด้านล่างแทน */
.fin-bar.pos{{background:var(--brass)}}
.fin-bar.neg{{background:var(--down)}}
.fin-bar.cmp{{background:var(--econ)}}
.fin-bar-lbl{{font-family:'IBM Plex Mono',monospace;font-size:.75rem;font-weight:600;
  color:var(--mute);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%}}
/* ป้ายงวดใต้กราฟเส้น — วางเป็น HTML ไม่ใช่ <text> ใน SVG เพราะ SVG ยืดเต็มความกว้าง
   (preserveAspectRatio=none) ตัวอักษรข้างในจะถูกยืดตามจนเบี้ยว */
.fin-xaxis{{display:flex;justify-content:space-between;gap:6px;margin-top:7px}}
.fin-xaxis span{{font-family:'IBM Plex Mono',monospace;font-size:.63rem;color:var(--dim);
  white-space:nowrap}}
/* เส้นแนวโน้มมาร์จิ้น % — สีตายตัวตามลำดับเดิมเสมอ ไม่สลับสีตามการกรอง */
.fin-legend{{display:flex;gap:15px;flex-wrap:wrap;margin-bottom:11px}}
.fin-legend-item{{display:flex;align-items:center;gap:6px;font-family:'IBM Plex Mono',monospace;
  font-size:.65rem;color:var(--mute)}}
.fin-legend-item i{{width:10px;height:10px;border-radius:2px;flex:none}}
.fin-line-svg{{width:100%;height:170px;display:block;overflow:visible}}
.fin-line-path{{stroke-width:2;stroke-linecap:round;stroke-linejoin:round}}
.fin-line-dot{{cursor:pointer}}
.fin-zero-ln{{stroke:var(--line2);stroke-width:1;stroke-dasharray:3 3}}
.fin-grid-ln{{stroke:var(--line);stroke-width:1}}
.fin-axis-t{{fill:var(--dim);font-family:'IBM Plex Mono',monospace;font-size:9.5px}}

/* ตารางเป็นแผงสูงจำกัด เลื่อนในตัวเอง หัวตารางเลยค้างได้จริงเวลาไล่บรรทัดยาวๆ */
.fin-table-wrap{{max-height:min(72vh,720px);overflow:auto;
  border:1px solid var(--line);border-radius:2px;background:var(--panel)}}
/* separate ไม่ใช่ collapse — sticky บน <th> พังกับ border-collapse:collapse ในเบราว์เซอร์
   ตระกูล Chromium หลายรุ่น (แถวหัวตารางไม่ยอมค้างเวลาเลื่อน) */
.fin-table{{width:100%;border-collapse:separate;border-spacing:0;
  font-family:'IBM Plex Mono',monospace;font-size:.92rem;font-variant-numeric:tabular-nums}}
.fin-table th,.fin-table td{{padding:13px 22px;text-align:right;white-space:nowrap;
  border-bottom:1px solid var(--line)}}
.fin-table thead{{position:sticky;top:0;z-index:2}}
.fin-table thead th{{position:sticky;top:0;background:var(--panel3);z-index:2;
  color:var(--mute);font-size:.68rem;letter-spacing:.08em;font-weight:600;
  border-bottom:1px solid var(--line2);cursor:pointer;user-select:none}}
.fin-table thead th:hover{{color:var(--ink)}}
.fin-table thead th.sorted{{color:var(--brass)}}
.fin-table .fin-sort-ic{{margin-left:5px;opacity:.8}}
.fin-table th:first-child,.fin-table td:first-child{{position:sticky;left:0;
  background:var(--panel);z-index:1;text-align:left;font-family:'Noto Serif Thai',Georgia,serif;
  font-size:1rem;color:var(--mute);white-space:normal;min-width:210px;cursor:default;
  border-right:1px solid var(--line)}}
.fin-table thead th:first-child{{background:var(--panel3);z-index:3}}
.fin-table tbody tr:hover td,.fin-table tbody tr:hover td:first-child{{background:var(--hover)}}
.fin-table .fin-val{{color:var(--ink);font-weight:600}}
.fin-table .fin-na{{color:var(--dim)}}
.fin-table .fin-delta{{display:block;font-size:.71rem;font-weight:500;margin-top:4px}}
.fin-table .fin-delta.up{{color:var(--up)}}
.fin-table .fin-delta.down{{color:var(--down)}}
.fin-table .fin-delta.flat{{color:var(--dim)}}
.fin-sec td{{padding:10px 22px;background:var(--panel2);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;
  letter-spacing:.15em;text-transform:uppercase;color:var(--brass);font-weight:700;
  border-bottom:1px solid var(--line2);border-top:1px solid var(--line2)}}
.fin-sec td:first-child{{background:var(--panel2);font-family:'IBM Plex Mono',monospace}}
@media(max-width:900px){{
  .cmodal-head{{position:relative;flex-wrap:wrap}}
  .fin-btn{{margin-left:0}}
  .fin-tabs{{width:100%;order:3;margin-left:0}}
  .fin-toolbar{{padding:12px 0 10px}}
  .fin-cmp{{margin-left:0;width:100%}}
  .fin-cmp select{{flex:1;max-width:none}}
  .fin-kpis{{grid-template-columns:repeat(auto-fit,minmax(148px,1fr))}}
  /* การ์ดแคบลงเหลือ ~125px ตัวเลขพร้อมหน่วยอย่าง "7.207 USD" เลยยาวเกินจนโดนตัดเป็น "7.207 …"
     ย่อขนาดฟอนต์ตามความกว้างการ์ดแทนการตัดทิ้ง — ตัวเลขต้องอ่านครบก่อนสวย */
  .fin-kpi-val{{font-size:clamp(1.05rem,7.5cqw,1.55rem)}}
  .fin-kpi{{container-type:inline-size}}
  .fin-panel-grid{{grid-template-columns:1fr}}
  .fin-table th,.fin-table td{{padding:11px 15px}}
  .fin-table th:first-child,.fin-table td:first-child{{min-width:150px;font-size:.9rem}}
}}

/* ป้ายค่าอินดิเคเตอร์มุมซ้ายบนของแต่ละแพเนล */
.c-leg text{{font-family:'IBM Plex Mono',monospace;font-size:10px}}
.c-pane-sep{{stroke:var(--line);stroke-width:1;shape-rendering:crispEdges}}
.c-tag rect{{fill:var(--brass)}}
.c-tag text{{fill:#0A0E1A;font-family:'IBM Plex Mono',monospace;font-size:9.5px;font-weight:600}}
.c-tag.now rect{{fill:var(--ink)}}
.c-fib line{{stroke-dasharray:5 4;stroke-width:1}}
.c-fib text{{font-family:'IBM Plex Mono',monospace;font-size:9px}}
.c-vol rect{{opacity:.5}}

.ctype{{flex:none}}
.cmodal-tape{{margin:0;padding:7px 10px;gap:6px;border-bottom:1px solid var(--line);
  background:var(--panel3)}}
.cmodal-tape .ticker{{background:transparent;border-color:var(--line)}}

/* ค่าคำนวณ + ข่าว อยู่คอลัมน์ขวาของกราฟ */
.cmodal-side{{width:310px;flex:none;display:flex;flex-direction:column;
  border-left:1px solid var(--line);min-height:0}}
.calc{{flex:none;max-height:44%;overflow-y:auto}}
.calc-row{{display:flex;justify-content:space-between;align-items:baseline;gap:10px;
  padding:8px 13px;border-bottom:1px solid var(--line)}}
.calc-k{{font-size:.74rem;color:var(--mute);min-width:0}}
.calc-k small{{display:block;font-size:.6rem;color:var(--dim);font-family:'IBM Plex Mono',monospace;
  overflow-wrap:anywhere}}
.calc-v{{font-family:'IBM Plex Mono',monospace;font-size:.86rem;font-weight:500;
  text-align:right;white-space:nowrap;min-width:0}}
/* ตัวเลขหลักห้ามตัดบรรทัด แต่คำอธิบายใต้ตัวเลขยาวได้ ต้องยอมให้ตัดบรรทัด
   ไม่งั้นบรรทัดอย่าง "ahead of inflation" จะดันแถวล้นออกนอกแผงบนจอแคบ */
.calc-v small{{display:block;font-size:.6rem;font-weight:400;color:var(--dim);
  white-space:normal}}
.calc-na{{color:var(--dim)}}
.calc-on-able{{cursor:pointer}}
.calc-on-able:hover{{background:var(--hover)}}
.calc-on-able::after{{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;
  background:transparent}}
.calc-on-able{{position:relative}}
.calc-on-able.on{{background:var(--sel)}}
.calc-on-able.on::after{{background:var(--brass)}}
.calc-note{{padding:8px 13px;font-size:.6rem;line-height:1.5;color:var(--dim);
  border-bottom:1px solid var(--line)}}
.cnews-head{{padding:10px 14px;border-bottom:1px solid var(--line);background:var(--panel2);
  font-size:.72rem;font-weight:600;letter-spacing:.05em;text-transform:uppercase;
  color:var(--mute)}}
/* หัวข้อ METRICS พับเก็บได้ */
.cfold{{display:flex;align-items:center;gap:9px;width:100%;cursor:pointer;text-align:left;
  font-family:inherit;border:0;border-bottom:1px solid var(--line)}}
.cfold:hover{{color:var(--ink)}}
.cfold .scope-caret{{transition:transform .2s}}
.calc-folded .cfold .scope-caret{{transform:rotate(-90deg)}}
.calc-folded .calc{{display:none}}
.cnews-list{{flex:1;overflow-y:auto}}
.cnews-row{{display:flex;gap:9px;align-items:flex-start;padding:9px 13px;
  border-bottom:1px solid var(--line)}}
.cnews-row:hover{{background:var(--hover)}}
.cnews-row .score{{min-width:44px;padding:3px 6px;font-size:.68rem}}
.cnews-t{{flex:1;min-width:0;font-size:.78rem;line-height:1.38}}
.cnews-m{{display:block;margin-top:3px;font-family:'IBM Plex Mono',monospace;
  font-size:.6rem;color:var(--dim);text-transform:uppercase}}
@media(max-width:1100px){{.cmodal-side{{width:264px}}}}
@media(max-width:860px){{
  .cmodal-body{{flex-direction:column;overflow-y:auto}}
  .cmodal-pick{{width:auto;border-right:0;border-bottom:1px solid var(--line)}}
  .cmodal-list{{display:flex;overflow-x:auto;padding:6px;max-height:none}}
  .cgroup{{display:none}}
  /* กลุ่ม th/intl ต้องหายไปจากเลย์เอาต์ ไม่ใช่แค่ซ่อนหัวข้อ ไม่งั้นการ์ดจะเรียงเป็น
     สองแถวตั้งแทนที่จะเลื่อนแนวนอนแถวเดียวต่อกัน (contents = ลูกข้างในนับเป็นลูกของ
     .cmodal-list โดยตรง ตัว .cfav-group เองไม่มีผลกับเลย์เอาต์อีกต่อไป) */
  .cfav-group{{display:contents}}
  .cgroup.folded + .cfav-group{{display:contents}}
  .citem{{width:auto;white-space:nowrap}}
  .cmodal-chart{{min-height:340px}}
  .cmodal-side{{width:auto;border-left:0;border-top:1px solid var(--line)}}
  .calc{{max-height:none}}
  /* จอแคบ: แถบเครื่องมือวางแนวนอนใต้กราฟแทน */
  .crail{{flex-direction:row;width:auto;padding:7px 9px;justify-content:flex-start;
    border-left:0;border-right:0;border-top:1px solid var(--line);overflow-x:auto}}
  .crail-sep{{width:1px;height:22px;margin:0 3px}}
  .cpop{{right:auto;left:8px;top:46px}}
}}
.t-label{{color:var(--ink);font-weight:600;letter-spacing:.03em}}
.t-price{{color:var(--ink);font-weight:500}}
.t-pct{{font-size:.76rem}}
.up{{color:var(--up)}} .down{{color:var(--down)}} .flat{{color:var(--mute)}}

.panel{{background:var(--panel);border:1px solid var(--line);border-radius:2px;overflow:hidden}}
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
  border-radius:2px;cursor:pointer}}
.zoom-ctl button:hover{{color:var(--ink);border-color:var(--dim)}}
.mk{{cursor:pointer}}
.mk .halo{{opacity:.16;transition:opacity .15s}}
.mk .core{{stroke:var(--bg);stroke-width:1.2}}
.mk:hover .halo{{opacity:.34}}
.mk-label{{font-family:'Noto Serif Thai',Georgia,serif;font-size:10px;
  fill:var(--mute);pointer-events:none}}
#tip{{position:absolute;pointer-events:none;opacity:0;transition:opacity .12s;
  background:rgba(10,14,26,.95);border:1px solid var(--line);border-radius:2px;
  padding:7px 10px;font-size:.74rem;display:flex;flex-direction:column;gap:2px;z-index:5}}
#tip span{{color:var(--mute);font-family:'IBM Plex Mono',monospace;font-size:.66rem}}
/* ภาพข่าวในกล่องชี้ — ให้เห็นว่าจุดนั้นกำลังเกิดอะไร ไม่ใช่แค่ตัวเลข */
#tip{{max-width:260px}}
.tip-img{{width:100%;aspect-ratio:16/9;object-fit:cover;border-radius:2px;
  margin-bottom:6px;display:block;background:var(--panel2)}}
.tip-head{{font-family:'Noto Serif Thai',Georgia,serif;font-size:.8rem;line-height:1.4;
  color:var(--ink);margin-bottom:3px}}

/* ── ลูกโลกวัดสัดส่วนข่าวความขัดแย้ง ────────────────────────── */
.tension{{display:flex;align-items:center;gap:9px;margin-left:auto;flex:none;cursor:pointer;
  padding:7px 13px;border-radius:2px;background:var(--panel);
  border:1px solid var(--line);transition:border-color .18s}}
.tension:hover,.tension[aria-expanded="true"]{{border-color:var(--brass)}}
.tension-globe{{width:26px;height:26px;flex:none;fill:none;stroke:currentColor;
  stroke-width:1.5;color:var(--mute)}}
.tension[aria-expanded="true"] .tension-globe{{color:var(--brass)}}
.tension-n{{font-family:'IBM Plex Mono',monospace;font-size:1.32rem;font-weight:700;
  line-height:1;color:var(--ink)}}
.tension-n.hot{{color:var(--down)}}
.tension-n.warm{{color:var(--poli)}}
.tension-lbl{{font-family:'IBM Plex Mono',monospace;font-size:.52rem;letter-spacing:.12em;
  line-height:1.25;color:var(--dim);text-align:left}}
.tension-panel{{flex:none;max-height:46%;overflow-y:auto;padding:14px clamp(14px,3vw,22px) 16px;
  border-bottom:1px solid var(--line);background:var(--panel2)}}
.tension-panel[hidden]{{display:none}}
.tension-note{{padding:11px 13px;margin-bottom:14px;border-radius:2px;
  background:var(--panel);border:1px solid var(--line);border-left:2px solid var(--poli);
  font-size:.84rem;line-height:1.6;color:var(--mute)}}
.tension-note b{{color:var(--ink)}}
.tension-list{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}}
.tension-row{{display:flex;gap:10px;align-items:flex-start;padding:9px;border-radius:2px;
  background:var(--panel);border:1px solid var(--line)}}
.tension-row:hover{{border-color:var(--brass)}}
.tension-row img{{width:64px;aspect-ratio:4/3;object-fit:cover;border-radius:2px;flex:none;
  background:var(--panel2)}}
.tension-row-t{{font-size:.86rem;line-height:1.4;color:var(--ink);margin-bottom:3px}}
.tension-row-m{{font-family:'IBM Plex Mono',monospace;font-size:.62rem;color:var(--dim)}}
@media(max-width:700px){{
  .tension-lbl{{display:none}}
  .tension-panel{{max-height:56%}}
}}
.legend{{position:absolute;left:14px;bottom:12px;display:flex;flex-wrap:wrap;gap:6px 14px;
  font-size:.68rem;color:var(--mute);background:rgba(10,14,26,.8);
  border:1px solid var(--line);border-radius:2px;padding:6px 11px}}
.legend span{{display:flex;align-items:center;gap:5px}}

#hotspot-detail{{border-top:1px solid var(--line);max-height:172px;overflow-y:auto}}
.hd-top{{display:flex;justify-content:space-between;padding:10px 15px 6px;align-items:baseline}}
.hd-top h4{{font-size:.85rem;font-weight:600}}
.hd-top span{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hd-row{{display:flex;align-items:center;gap:9px;
  padding:8px 15px;border-top:1px solid var(--line);font-size:.8rem}}
.hd-row:hover{{background:var(--hover)}}
.hd-row > span:nth-last-child(2){{flex:1;min-width:0}}
.hd-thumb{{width:36px;height:36px;border-radius:2px;object-fit:cover;flex:none;background:var(--panel2)}}
.hd-age{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--dim);white-space:nowrap}}

/* ข่าวล่าสุดอยู่เต็มความกว้างเหนือแผนที่ → จัดเป็นหลายคอลัมน์ */
.feed{{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr))}}
.feed-row{{display:flex;align-items:center;gap:10px;min-width:0;
  padding:9px 15px;border-bottom:1px solid var(--line);transition:background .12s}}
.feed-row:hover{{background:var(--hover)}}
.feed-thumb{{width:44px;height:44px;border-radius:2px;object-fit:cover;flex:none;background:var(--panel2)}}
.feed-title{{flex:1;min-width:0;font-size:.82rem;line-height:1.4}}
.feed-age{{flex:none;font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim);white-space:nowrap}}

.hot{{display:grid;grid-template-columns:18px 1fr 62px 26px;gap:9px;align-items:center;
  padding:7px 15px;border-bottom:1px solid var(--line);font-size:.8rem}}
.hot:last-child{{border-bottom:0}}
.rank{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hot-bars{{display:flex;height:5px;border-radius:3px;overflow:hidden;background:var(--panel3)}}
.hot-bars .hb-econ{{background:var(--econ)}} .hot-bars .hb-poli{{background:var(--poli)}}
.hot-bars .hb-biz{{background:var(--biz)}} .hot-bars .hb-env{{background:var(--env)}}
.hot-n{{font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--mute);text-align:right}}

.search-wrap{{padding:11px 15px;border-bottom:1px solid var(--line)}}
.search{{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:2px;
  color:var(--ink);font-family:inherit;font-size:.82rem;padding:8px 12px}}
.search::placeholder{{color:var(--dim)}}
.search:focus{{outline:none;border-color:var(--econ)}}

.grid-side{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:700px){{.grid-side{{grid-template-columns:1fr}}}}

.btn{{display:inline-flex;align-items:center;gap:7px;padding:9px 19px;border-radius:2px;
  font-family:inherit;font-size:.86rem;font-weight:600;cursor:pointer;
  border:1px solid transparent;transition:background .16s}}
.btn-main{{background:#fff;color:#0A0E1A}}
.btn-main:hover{{background:#D7E0EF}}
.btn-ghost{{background:rgba(246,241,227,.16);color:#F6F1E3;
  border-color:rgba(246,241,227,.45)}}
.btn-ghost:hover{{background:rgba(246,241,227,.3)}}

/* ── แถวข่าวแบบเลื่อนแนวนอน ───────────────────────────── */
.row{{margin-bottom:28px}}
.row-head{{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:11px}}
.row-head h2{{display:flex;align-items:center;gap:9px;font-size:1.05rem;font-weight:700}}
.row-n{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim);font-weight:400}}
.live{{font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.08em;
  color:#fff;background:var(--down);border-radius:2px;padding:2px 6px}}
/* ป้าย LIVE บนหัวข้อกลุ่ม — เห็นชัดแม้ตอนพับเก็บ */
.live-dot{{display:inline-flex;align-items:center;gap:5px;font-size:.62rem;
  font-weight:700;padding:3px 9px;
  box-shadow:0 0 0 0 rgba(229,72,77,.55);animation:livePulse 2.2s infinite}}
.live-dot::before{{content:"";width:6px;height:6px;border-radius:50%;background:#fff}}
@keyframes livePulse{{
  70%{{box-shadow:0 0 0 8px rgba(229,72,77,0)}}
  100%{{box-shadow:0 0 0 0 rgba(229,72,77,0)}}
}}
.row-tools{{display:flex;align-items:center;gap:7px}}
.row-nav{{width:30px;height:30px;flex:none;border-radius:50%;cursor:pointer;font-size:1.05rem;
  line-height:1;color:var(--ink);background:rgba(255,255,255,.06);border:1px solid var(--line)}}
.row-nav:hover{{background:rgba(255,255,255,.15)}}


/* ── หน้าหนึ่งหนังสือพิมพ์ (variant B) ───────────────────── */
.fp-dateline{{display:flex;justify-content:space-between;align-items:center;gap:12px;
  flex-wrap:wrap;padding:9px 0;margin-bottom:22px;
  border-top:1px solid var(--line2);border-bottom:1px solid var(--line2);
  font-family:'IBM Plex Mono',monospace;font-size:.63rem;letter-spacing:.13em;
  text-transform:uppercase;color:var(--mute)}}
.fp-dateline-mid{{color:var(--brass);font-weight:700}}
/* auto-fit ทำให้เหลือคอลัมน์เดียวเต็มความกว้างอัตโนมัติ ตอนกรองเหลือไทย/เทศฝั่งเดียว */
.fp-cols{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr));
  gap:0}}
.fp-col{{padding:0 clamp(0px,2vw,32px);min-width:0}}
.fp-col:first-child{{padding-left:0}}
.fp-col:last-child{{padding-right:0}}
.fp-col+.fp-col{{border-left:1px solid var(--line)}}
/* ซ่อนฝั่งหนึ่งอยู่ (เลือกดูไทย/เทศอย่างเดียว) อีกฝั่งไม่ต้องมีเส้นคั่นลอยๆ */
.fp-col[hidden]+.fp-col{{border-left:0;padding-left:0}}
.fp-sec-h{{display:flex;align-items:center;gap:10px;padding-bottom:8px;margin-bottom:18px;
  border-bottom:2px solid var(--brass);
  font-family:'Playfair Display',Georgia,serif;font-size:1rem;font-weight:700;
  letter-spacing:.17em;text-transform:uppercase;color:var(--cream)}}
.fp-sec-n{{margin-left:auto;font-family:'IBM Plex Mono',monospace;font-size:.64rem;
  letter-spacing:.08em;color:var(--dim)}}
.fp-a{{display:block;color:inherit}}
.fp-item{{position:relative}}
.fp-lead{{padding-bottom:19px;margin-bottom:4px;border-bottom:1px solid var(--line)}}
.fp-lead-img{{width:100%;aspect-ratio:16/9;object-fit:cover;display:block;
  margin-bottom:14px;border-radius:2px}}
.fp-kicker{{display:inline-flex;align-items:center;gap:5px;margin-bottom:8px;
  font-family:'IBM Plex Mono',monospace;font-size:.59rem;letter-spacing:.15em;
  text-transform:uppercase;color:var(--brass)}}
.fp-kicker .cicon{{width:13px;height:13px}}
/* Playfair ไม่มีสระ/วรรณยุกต์ไทย เบราว์เซอร์จะตกมาใช้ Noto Serif Thai ให้เองต่อตัวอักษร */
.fp-lead-t{{font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;
  font-size:clamp(1.35rem,2.2vw,1.95rem);font-weight:700;line-height:1.16;
  color:var(--cream);margin-bottom:10px}}
.fp-a:hover .fp-lead-t{{color:var(--brass)}}
.fp-deck{{font-size:.9rem;line-height:1.62;color:var(--mute);margin-bottom:10px}}
.fp-meta{{display:block;font-family:'IBM Plex Mono',monospace;font-size:.62rem;
  letter-spacing:.05em;color:var(--dim)}}
.fp-speak{{position:absolute;right:9px;top:9px;z-index:2;width:28px;height:28px;padding:0;
  border-radius:2px;background:rgba(5,7,13,.72);opacity:0;transition:opacity .2s}}
.fp-lead:hover .fp-speak,.fp-speak:focus{{opacity:1}}
.fp-sub{{padding:14px 0;border-bottom:1px solid var(--line)}}
.fp-sub .fp-a{{display:flex;gap:13px;align-items:flex-start}}
.fp-sub-thumb{{position:relative;flex:none;width:104px;aspect-ratio:4/3;border-radius:2px;
  overflow:hidden;display:grid;place-items:center}}
.fp-sub-thumb img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}}
.fp-sub-thumb .cicon{{opacity:.5}}
.fp-sub-body{{flex:1;min-width:0}}
.fp-sub-body .fp-kicker{{margin-bottom:4px}}
.fp-sub-t{{font-family:'Playfair Display',Georgia,'Noto Serif Thai',serif;font-size:1rem;
  font-weight:700;line-height:1.3;color:var(--ink);margin-bottom:6px}}
.fp-a:hover .fp-sub-t{{color:var(--brass)}}
.fp-briefs{{margin-top:18px;padding-top:15px;border-top:2px solid var(--line2)}}
.fp-briefs-h{{font-family:'IBM Plex Mono',monospace;font-size:.61rem;letter-spacing:.17em;
  color:var(--mute);margin-bottom:11px}}
.fp-brief{{display:flex;gap:11px;align-items:flex-start;padding:10px 0;
  border-bottom:1px dotted var(--line2)}}
/* ภาพย่อของข่าวย่อย — เล็กกว่าการ์ดข้างบนแต่ใช้ทรงเดียวกัน ให้คอลัมน์ยังดูเป็นชุด */
.fp-brief-thumb{{position:relative;flex:none;width:62px;aspect-ratio:4/3;border-radius:2px;
  overflow:hidden;display:grid;place-items:center}}
.fp-brief-thumb img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}}
.fp-brief-thumb .cicon{{opacity:.5}}
.fp-brief-body{{flex:1;min-width:0}}
.fp-brief-t{{display:block;font-family:'Noto Serif Thai',Georgia,serif;font-size:.92rem;
  line-height:1.45;color:var(--ink);margin-bottom:4px}}
.fp-brief:hover .fp-brief-t{{color:var(--brass)}}
.fp-col.no-match,.fp-item.hidden{{display:none}}

@media(max-width:900px){{
  .fp-col{{padding:0}}
  .fp-col+.fp-col{{border-left:0;border-top:1px solid var(--line);
    margin-top:26px;padding-top:24px}}
}}
/* padding + margin ติดลบ เพื่อให้การ์ดที่ขยายตอน hover ไม่โดนตัดขอบ */
.row-track{{display:flex;gap:12px;overflow-x:auto;scroll-behavior:smooth;
  scroll-snap-type:x proximity;padding:24px 4px 28px;margin:-24px -4px -28px}}
.row-track::-webkit-scrollbar{{height:0}}
.row-empty{{color:var(--mute);font-size:.82rem;padding:22px 4px}}

/* ── การ์ดช่องที่ถ่ายทอดสด ─────────────────────────────── */
.lrow{{gap:14px}}
.lcard{{display:flex;flex-direction:column;gap:8px;flex:0 0 306px;scroll-snap-align:start;
  transition:transform .28s cubic-bezier(.2,.7,.3,1)}}
.lcard:hover{{transform:scale(1.055);z-index:3}}
.lcard-thumb{{position:relative;display:block;aspect-ratio:16/9;overflow:hidden;
  border-radius:2px;background:var(--panel3);border:1px solid var(--line);
  transition:border-color .28s,box-shadow .28s}}
.lcard:hover .lcard-thumb{{border-color:var(--down);box-shadow:0 18px 42px rgba(0,0,0,.62)}}
.lcard-thumb img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}}
.lcard-badge{{position:absolute;left:9px;top:9px;z-index:2;display:inline-flex;align-items:center;
  gap:5px;padding:3px 8px;border-radius:2px;background:var(--down);color:#fff;
  font-family:'IBM Plex Mono',monospace;font-size:.58rem;font-weight:700;letter-spacing:.1em}}
.lcard-badge i{{width:5px;height:5px;border-radius:50%;background:#fff;
  animation:livePulse 2.2s infinite}}
.lcard-play{{position:absolute;inset:0;display:grid;place-items:center;z-index:2;
  opacity:0;transition:opacity .22s;background:rgba(5,7,13,.34)}}
.lcard:hover .lcard-play{{opacity:1}}
.lcard-play svg{{width:44px;height:44px;fill:#fff;filter:drop-shadow(0 3px 10px rgba(0,0,0,.6))}}
.lcard-ch{{display:flex;align-items:center;gap:8px;font-family:'IBM Plex Mono',monospace;
  font-size:.66rem;letter-spacing:.08em;text-transform:uppercase;color:var(--cream)}}
.lcard-flag{{font-size:.55rem;letter-spacing:.08em;color:var(--dim);
  border:1px solid var(--line);border-radius:2px;padding:1px 5px}}
.lcard-t{{font-size:.83rem;line-height:1.45;color:var(--mute);
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}}
.lcard:hover .lcard-t{{color:var(--ink)}}
/* หน้า LIVE: การ์ดถ่ายทอดสดอยู่บน แล้วต่อด้วยรายการข่าว */
.live-page{{flex:1;min-height:0;overflow-y:auto;display:flex;flex-direction:column}}
.live-page .row{{padding:16px 18px 4px;margin:0}}
.live-page .live-list{{flex:none;overflow:visible}}

.loc{{margin-left:6px;padding:1px 5px;border:1px solid var(--line);border-radius:2px;
  color:var(--dim);text-transform:none;letter-spacing:0}}
.age{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--dim);white-space:nowrap}}
.speak{{font-family:inherit;font-size:.7rem;color:var(--mute);background:var(--panel2);
  border:1px solid var(--line);border-radius:2px;padding:4px 9px;cursor:pointer}}
.speak:hover{{color:var(--ink);border-color:var(--dim)}}
.speak.playing{{color:var(--econ);border-color:var(--econ)}}

/* ── อินโทรตอนเข้าเว็บ ─────────────────────────────────── */
#intro{{position:fixed;inset:0;z-index:99;background:var(--bg);display:grid;place-items:center;
  animation:introFade .6s 1.95s forwards}}
.intro-inner{{display:flex;flex-direction:column;align-items:center;gap:12px;
  padding:26px 34px;border-top:2px solid var(--brass);border-bottom:3px double var(--brass);
  animation:introIn 1.05s cubic-bezier(.2,.7,.3,1) backwards,
            introZoom .8s 1.72s ease-in forwards}}
.intro-mark{{font-family:'UnifrakturMaguntia','Playfair Display',Georgia,serif;font-weight:400;
  font-size:clamp(3rem,12vw,5.6rem);line-height:1.04;color:var(--cream);
  text-shadow:0 0 42px rgba(198,169,97,.4),0 0 110px rgba(198,169,97,.16)}}
.intro-rule{{width:min(320px,58vw);height:1px;background:linear-gradient(90deg,transparent,var(--brass) 24%,var(--brass) 76%,transparent)}}
.intro-inner b{{font-family:'Playfair Display',Georgia,serif;
  font-size:clamp(.55rem,1.9vw,.74rem);letter-spacing:.24em;
  font-weight:600;text-transform:uppercase;color:var(--mute);text-align:center;
  max-width:min(640px,86vw);line-height:1.8}}
@keyframes introIn{{from{{opacity:0;transform:scale(.84);filter:blur(13px)}}}}
@keyframes introZoom{{to{{opacity:0;transform:scale(1.55)}}}}
@keyframes introFade{{to{{opacity:0;visibility:hidden}}}}
@keyframes pageIn{{from{{opacity:0;transform:translateY(15px)}}}}
.no-intro #intro{{display:none}}
.no-intro body{{animation:none}}

/* ── เมนูหลัก: ปุ่ม 3 ขีด เปิดแถบยาวทางซ้ายมือ ───────────── */
.navbar{{display:flex;align-items:center;gap:13px;margin-bottom:18px;
  border-bottom:1px solid var(--line);padding-bottom:10px}}
.burger{{display:inline-flex;align-items:center;gap:10px;height:38px;padding:0 14px;
  cursor:pointer;background:var(--panel);border:1px solid var(--line);border-radius:2px;
  transition:border-color .16s,background .16s}}
.burger:hover{{border-color:var(--brass);background:var(--hover)}}
.burger-bars{{display:flex;flex-direction:column;justify-content:center;gap:4px;width:18px}}
.burger-bars span{{display:block;height:2px;border-radius:2px;background:var(--mute);
  transition:transform .2s,opacity .2s}}
.burger:hover .burger-bars span{{background:var(--ink)}}
.burger-txt{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.14em;
  font-weight:600;color:var(--mute)}}
.burger:hover .burger-txt{{color:var(--ink)}}
.burger[aria-expanded="true"] .burger-bars span:nth-child(1){{transform:translateY(6px) rotate(45deg)}}
.burger[aria-expanded="true"] .burger-bars span:nth-child(2){{opacity:0}}
.burger[aria-expanded="true"] .burger-bars span:nth-child(3){{transform:translateY(-6px) rotate(-45deg)}}
.navbar-now{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;letter-spacing:.12em;
  color:var(--dim);white-space:nowrap}}
.navbar-now::before{{content:"› "}}

/* ── ช่องค้นหาข่าวทั้งเว็บ อยู่ในแถบบนสุดเสมอ อย่างเว็บข่าวลงทุน ── */
.gsearch{{position:relative;flex:1;min-width:80px;max-width:380px;margin-left:auto}}
.gsearch-ic{{position:absolute;left:10px;top:50%;transform:translateY(-50%);
  width:14px;height:14px;fill:none;stroke:var(--dim);stroke-width:2;pointer-events:none}}
.gsearch input{{width:100%;padding:8px 30px 8px 32px;border-radius:2px;
  background:var(--panel2);border:1px solid var(--line);color:var(--ink);
  font-family:inherit;font-size:.82rem}}
.gsearch input:focus{{outline:none;border-color:var(--brass)}}
.gsearch input::placeholder{{color:var(--dim)}}
.gsearch-x{{position:absolute;right:6px;top:50%;transform:translateY(-50%);
  width:22px;height:22px;font-size:1rem;line-height:1;cursor:pointer;
  color:var(--dim);background:none;border:0}}
.gsearch-x:hover{{color:var(--ink)}}
.gsearch-empty{{padding:34px 18px;text-align:center;color:var(--mute);font-size:.9rem;
  border:1px solid var(--line);border-radius:2px;background:var(--panel);margin-bottom:20px}}
/* ตอนค้นหาทั้งเว็บ ต้องดึงข่าวที่ถูกซ่อนไว้ (พับ/สลับแท็บ) ออกมาให้เจอด้วย */
body.searching .scope-group[hidden]{{display:block!important}}
.row.no-match{{display:none}}

.navdim{{position:fixed;inset:0;z-index:58;background:var(--scrim);
  opacity:0;visibility:hidden;transition:opacity .2s,visibility .2s}}
.navdim.on{{opacity:1;visibility:visible}}
.navpanel{{position:fixed;left:0;top:0;bottom:0;z-index:59;width:252px;max-width:84vw;
  display:flex;flex-direction:column;background:var(--panel);
  border-right:1px solid var(--line);box-shadow:18px 0 46px rgba(0,0,0,.45);
  transform:translateX(-100%);visibility:hidden;
  transition:transform .22s cubic-bezier(.4,0,.2,1),visibility .22s}}
.navpanel.open{{transform:none;visibility:visible}}
.nav-head{{display:flex;align-items:center;justify-content:space-between;
  padding:15px 14px 13px 18px;border-bottom:1px solid var(--line);background:var(--panel2);
  font-family:'IBM Plex Mono',monospace;font-size:.66rem;letter-spacing:.16em;
  font-weight:600;color:var(--dim)}}
.nav-x{{width:28px;height:28px;font-size:1.15rem;line-height:1;cursor:pointer;
  color:var(--mute);background:none;border:1px solid var(--line);border-radius:2px}}
.nav-x:hover{{color:var(--ink);border-color:var(--brass)}}
.nav-foot{{padding:11px 18px;border-top:1px solid var(--line);
  font-family:'IBM Plex Mono',monospace;font-size:.58rem;letter-spacing:.1em;color:var(--dim)}}
.tabs{{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:2px;padding:9px}}
.tab{{position:relative;display:flex;align-items:center;width:100%;
  font-family:inherit;font-size:.86rem;font-weight:600;cursor:pointer;text-align:left;
  color:var(--mute);background:none;border:0;padding:11px 13px;border-radius:2px;
  transition:color .16s,background .16s}}
.tab .tab-n{{margin-left:auto}}
.tab:hover{{color:var(--ink);background:rgba(255,255,255,.05)}}
.tab.active{{color:var(--ink);background:var(--sel)}}
.tab.active::after{{content:"";position:absolute;left:0;top:9px;bottom:9px;width:3px;
  border-radius:0 3px 3px 0;background:linear-gradient(180deg,var(--econ),var(--poli))}}
.tab[draggable]{{cursor:grab}}
.tab.dragging{{opacity:.4;cursor:grabbing}}
.tab-icon{{display:flex;align-items:center;gap:9px;color:var(--brass)}}
.tab-icon svg{{width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:2;
  stroke-linecap:round;stroke-linejoin:round}}
.tab-icon:hover{{color:var(--cream)}}

/* ── จอกว้าง: เมนูหลักเป็นแถบแนวนอนที่เห็นตลอด ไม่ต้องกดปุ่ม 3 ขีด ──
   อย่างเว็บข่าวลงทุนทั่วไป (Bloomberg/Investing.com) — hamburger เหลือไว้
   เฉพาะจอแคบที่ไม่มีที่พอให้วางแท็บทั้งหมด */
@media(min-width:861px){{
  .navbar{{border-bottom:0;margin-bottom:8px;padding-bottom:0}}
  .burger,.navdim{{display:none}}
  .navpanel{{position:static;transform:none;visibility:visible;width:auto;max-width:none;
    height:auto;box-shadow:none;border:0;background:transparent;
    border-bottom:1px solid var(--line);margin-bottom:20px}}
  .nav-head,.nav-foot{{display:none}}
  .tabs{{flex-direction:row;flex-wrap:wrap;overflow:visible;padding:0 0 12px}}
  .tab{{width:auto;padding:9px 15px;border-radius:2px}}
  .tab .tab-n{{margin-left:8px}}
  .tab.active::after{{left:12px;right:12px;top:auto;bottom:-9px;width:auto;height:2px;
    border-radius:2px 2px 0 0}}
}}

.scope-caret{{width:14px;height:14px;flex:none;fill:none;stroke:currentColor;
  stroke-width:2.4;stroke-linecap:round;stroke-linejoin:round;color:var(--dim);
  transition:transform .2s}}
.tab-live{{color:var(--down)}}
.live-dot-sm{{width:7px;height:7px;border-radius:50%;background:var(--down);
  box-shadow:0 0 0 0 rgba(229,72,77,.55);animation:livePulse 2.2s infinite}}
.live-list{{flex:1;overflow-y:auto}}
.live-list .cnews-row{{padding:11px 16px}}
.tab-n{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;font-weight:400;
  color:var(--dim);margin-left:6px}}
.scope-flag{{font-size:.66rem;font-weight:500;letter-spacing:.08em;color:var(--dim);
  font-family:'IBM Plex Mono',monospace;border:1px solid var(--line);
  border-radius:2px;padding:2px 7px}}
[hidden]{{display:none!important}}

/* ── โมเสกคำที่ถูกพูดถึง — ภาพข่าวเป็นพื้น ตัวหนังสือทับกลางช่อง ── */
.kws{{display:grid;grid-template-columns:repeat(6,1fr);grid-auto-rows:52px;
  grid-auto-flow:dense;gap:2px;padding:2px}}
.kw{{position:relative;display:grid;place-items:center;overflow:hidden;
  border-radius:2px;background:var(--panel2);text-align:center;
  transition:transform .25s cubic-bezier(.2,.7,.3,1)}}
.kw:hover{{transform:scale(1.04);z-index:2}}
.kw img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}}
.kw-scrim{{position:absolute;inset:0;
  background:linear-gradient(180deg,rgba(5,7,13,.34),rgba(5,7,13,.72))}}
.kw-t{{position:relative;z-index:2;display:flex;flex-direction:column;align-items:center;
  gap:2px;padding:4px 6px;font-weight:600;line-height:1.15;color:#fff;
  text-shadow:0 1px 10px rgba(0,0,0,.85)}}
.kw-t b{{font-family:'IBM Plex Mono',monospace;font-size:.58rem;font-weight:500;
  letter-spacing:.06em;color:var(--brass);text-shadow:none}}
.kw-xl{{grid-column:span 3;grid-row:span 3;font-size:1.5rem}}
.kw-lg{{grid-column:span 3;grid-row:span 2;font-size:1.12rem}}
.kw-md{{grid-column:span 2;grid-row:span 2;font-size:.94rem}}
.kw-sm{{grid-column:span 2;grid-row:span 1;font-size:.8rem}}
@media(max-width:560px){{
  .kws{{grid-template-columns:repeat(4,1fr);grid-auto-rows:46px}}
  .kw-xl,.kw-lg{{grid-column:span 4}}
  .kw-md,.kw-sm{{grid-column:span 2}}
}}

footer{{margin-top:22px;padding-top:14px;border-top:3px double var(--line2);
  display:flex;justify-content:space-between;flex-wrap:wrap;gap:12px;
  font-family:'IBM Plex Mono',monospace;font-size:.68rem;letter-spacing:.05em;
  text-transform:uppercase;color:var(--dim)}}
.foot-mark{{font-family:'UnifrakturMaguntia','Playfair Display',Georgia,serif;
  font-size:1.05rem;letter-spacing:0;text-transform:none;color:var(--cream)}}

/* ── หัวข้อทุกระดับใช้ตัวเซริฟแบบหนังสือพิมพ์ ─────────────── */
.row-head h2,.panel-head h2,.cmodal-title h3,.tmodal-head h3,
.cnews-head,.hd-top h4{{
  font-family:'Playfair Display','Noto Serif Thai',Georgia,serif}}
.row-head h2{{letter-spacing:.06em;text-transform:uppercase;font-size:1rem}}
.panel-head h2,.cnews-head{{letter-spacing:.14em}}
/* เส้นคั่นหัวแถวข่าว อย่างคอลัมน์หนังสือพิมพ์ */
.row-head{{border-bottom:1px solid var(--line);padding-bottom:8px}}
::-webkit-scrollbar{{width:8px;height:8px}}
::-webkit-scrollbar-track{{background:transparent}}
::-webkit-scrollbar-thumb{{background:var(--line2);border-radius:2px}}
@media(prefers-reduced-motion:reduce){{
  *{{animation:none!important;transition:none!important}}
  #intro{{display:none!important}}   /* กัน overlay ค้างเมื่ออนิเมชันถูกปิด */
  .row-track{{scroll-behavior:auto}}
}}
</style>
</head>
<body>
<script>
// เก็บกวาดค่าที่ค้างจากตอนทดสอบเลย์เอาต์ A/B/C — ตอนนี้เหลือแบบเดียวแล้ว
try {{ localStorage.removeItem('layoutVariant'); }} catch(e) {{}}
</script>

<div id="intro" aria-hidden="true"><div class="intro-inner">
  <span class="intro-mark">The Tribune</span><span class="intro-rule"></span>
  <b>Thai Regional &amp; International Business Updates, News and Exchange</b></div></div>

<div id="tmodal" class="tmodal" hidden>
  <div class="tmodal-box" role="dialog" aria-modal="true" aria-labelledby="tmodal-name">
    <div class="tmodal-head">
      <div>
        <h3 id="tmodal-name"></h3>
        <div class="tmodal-price"><span id="tmodal-p"></span><span id="tmodal-c"></span></div>
      </div>
      <button type="button" class="tmodal-x" onclick="closeTicker()" aria-label="Close">×</button>
    </div>
    <p class="tmodal-note">Ranked by <strong>content relevance</strong> to this asset
      (keyword match on headline and body) — not a measure of price impact</p>
    <div class="tmodal-list" id="tmodal-list"></div>
  </div>
</div>

<header>
  <div class="mast-top">
    <span>Est. 2026</span><span class="mast-dot">❧</span>
    <span>Bangkok · Digital Edition</span><span class="mast-dot">❧</span>
    <span>Published every three hours</span>
  </div>
  <h1 class="logo">
    <span class="logo-the">The</span>
    <span class="logo-mark">Tribune</span>
  </h1>
  <div class="logo-rule"><span>◆ ◆ ◆</span></div>
  <div class="logo-sub">Thai Regional &amp; International Business Updates, News and Exchange</div>
  <div class="stamp">
    <span class="pulse"></span>
    <span>{NOW.strftime('%A, %d %B %Y')}</span>
    <span style="color:var(--dim)">·</span>
    <span>Updated {NOW.strftime('%H:%M')}</span>
    <span style="color:var(--dim)">· next edition {next_run}</span>
  </div>
</header>

<div class="cpop-tip" id="ind-tip" hidden></div>

{live_modal}

<div id="mmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="News map">
    <div class="cmodal-head">
      <button type="button" class="backbtn" onclick="closeMap()" aria-label="Back">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>
      </button>
      <div class="cmodal-title"><h3>NEWS MAP</h3>
        <div class="cmodal-price">{len(markers)} places · click a dot · zoomable</div>
      </div>
      <!-- ลูกโลกมุมขวาบน อย่างหน้าจอเกมยุทธศาสตร์ — กดแล้วเห็นข่าวที่ทำให้ตัวเลขขึ้น -->
      <button class="tension" type="button" id="tension-btn" onclick="toggleTension()"
              aria-expanded="false" aria-controls="tension-panel">
        <svg class="tension-globe" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="9"/>
          <path d="M3 12h18M12 3c2.6 2.8 2.6 15.2 0 18M12 3c-2.6 2.8-2.6 15.2 0 18"/>
        </svg>
        <span class="tension-n" id="tension-n">—</span>
        <span class="tension-lbl">CONFLICT<br>COVERAGE</span>
      </button>
    </div>
    <div class="tension-panel" id="tension-panel" hidden></div>
    <div class="map-modal-body">
      <div class="map-wrap">
        <svg id="map"></svg>
        <div id="tip"></div>
        <div class="zoom-ctl">
          <button type="button" onclick="zoomBy(1.6)" aria-label="Zoom in">+</button>
          <button type="button" onclick="zoomBy(1/1.6)" aria-label="Zoom out">−</button>
          <button type="button" onclick="zoomReset()" aria-label="Reset map">⟲</button>
        </div>
        <div class="legend">
          {''.join(f'<span>{cat_icon(c, "ci-sm")}{CAT_LABELS[c]}</span>' for c in CAT_NAMES + ["mixed"])}
        </div>
      </div>
      <div id="hotspot-detail"></div>
    </div>
  </div>
</div>

<div id="cmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="Price charts">
    <div class="cmodal-head">
      <button type="button" class="backbtn" onclick="closeCharts()" aria-label="Back">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>
      </button>
      <div class="cmodal-title">
        <h3 id="cmodal-name">—</h3>
        <div class="sym-full" id="cmodal-full" hidden></div>
        <div class="cmodal-price"><span id="cmodal-p"></span><span id="cmodal-c"></span></div>
        <div class="cmodal-note" id="cnote" hidden></div>
      </div>
      <button class="fin-btn" type="button" id="fin-btn" onclick="openFinancials()" hidden>
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 19h16M7 19V9M12 19V5M17 19v-7"/></svg>
        <span id="fin-btn-lbl">FINANCIALS</span></button>
      <button class="fin-btn expand-btn" type="button" id="cexpand" onclick="toggleChartFull()"
              aria-pressed="false" title="Expand the chart to the full window (Esc to exit)">
        <svg viewBox="0 0 24 24" aria-hidden="true" id="cexpand-ic">
          <path d="M4 9V4h5M20 15v5h-5M15 4h5v5M9 20H4v-5"/></svg>
        <span id="cexpand-t">FULL VIEW</span></button>
    </div>

    <div class="tickers cmodal-tape">
      {ticker_row("th", "THAI")}
      {ticker_row("intl", "GLOBAL")}
    </div>
    <div class="cfull-bar" id="cfull-bar"></div>
    <div class="cmodal-body">
      <!-- แถบเครื่องมือเทคนิค ยื่นออกมาที่ขอบซ้ายสุดของหน้าต่าง (ก่อนรายชื่อหุ้น) -->
      <div class="crail" id="crail">
        <button class="crbtn" type="button" data-pop="ind" onclick="togglePop('ind')"
                title="Indicators"><b>ƒ</b><span class="crbadge" id="crn" hidden>0</span></button>
        <button class="crbtn" type="button" data-tool="fib" onclick="toggleTool('fib')"
                title="Fibonacci retracement">FIB</button>
        <button class="crbtn" type="button" data-tool="log" onclick="toggleTool('log')"
                title="Logarithmic price scale">LOG</button>
        <button class="crbtn" type="button" data-tool="grid" onclick="toggleTool('grid')"
                title="Grid">GRD</button>
        <div class="crail-sep"></div>
        <button class="crbtn" type="button" onclick="resetZoom()" title="Reset zoom">⟲</button>
        <div class="cpop" id="pop-ind" hidden>
          <div class="cpop-head">INDICATORS
            <span>ชี้ค้าง / กดค้าง เพื่อดูคำอธิบาย</span></div>
          <div class="cpop-body" id="ind-list"></div>
          <button class="cpop-clear" type="button" onclick="clearInd()">CLEAR ALL</button>
        </div>
      </div>
      <div class="cmodal-pick">
        <div class="cfav-bar">
          <button class="cfav-tab on" type="button" data-mode="fav"
                  onclick="setAssetMode('fav')">★ FAVORITES</button>
          <button class="cfav-tab" type="button" data-mode="all"
                  onclick="setAssetMode('all')">ALL</button>
        </div>
        <input class="search csearch" id="csearch" type="search" autocomplete="off"
               placeholder="Search symbol…" oninput="filterAssets(this.value)">
        <div class="cmodal-list" id="cmodal-list"></div>
      </div>
      <div class="cmodal-chart">
        <div id="cchart"></div>
        <div class="cbottom">
          <div id="creadout" class="creadout"></div>
          <div class="cctrl">
            <div class="cctrl-g">
              <span class="cctrl-lbl">RANGE</span>
              <div class="tfbar" id="cmodal-tf"></div>
            </div>
            <div class="cctrl-g">
              <span class="cctrl-lbl">CHART TYPE</span>
              <div class="tfbar ctype">
                <button class="tfbtn on" type="button" data-ct="candle"
                        onclick="pickType('candle')">CANDLES</button>
                <button class="tfbtn" type="button" data-ct="line"
                        onclick="pickType('line')">LINE</button>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="cmodal-side" id="cmodal-side">
        <button class="cnews-head cfold" type="button" onclick="toggleCalc()"
                aria-expanded="true" aria-controls="ccalc">
          <svg class="scope-caret" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg>METRICS</button>
        <div class="calc" id="ccalc"></div>
        <div class="cnews-head">RELATED NEWS</div>
        <div class="cnews-list" id="cnews-list"></div>
      </div>
    </div>
  </div>
</div>

<div id="finmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="Financial statements">
    <div class="cmodal-head">
      <button type="button" class="backbtn" onclick="closeFinancials()" aria-label="Back">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>
      </button>
      <button class="row-nav fin-nav" type="button" onclick="finNav(-1)"
              aria-label="Previous symbol">‹</button>
      <div class="cmodal-title">
        <h3 id="fin-name">—</h3>
        <div class="sym-full" id="fin-full" hidden></div>
        <div class="cmodal-price"><span id="fin-currency"></span></div>
      </div>
      <button class="row-nav fin-nav" type="button" onclick="finNav(1)"
              aria-label="Next symbol">›</button>
      <div class="fin-tabs" role="tablist">
        <button class="fin-tab on" type="button" data-span="annual"
                onclick="pickFinSpan('annual')">ANNUAL</button>
        <button class="fin-tab" type="button" data-span="quarterly"
                onclick="pickFinSpan('quarterly')">QUARTERLY · THIS YEAR</button>
      </div>
    </div>
    <div class="fin-body" id="fin-body"><div class="cempty">Loading…</div></div>
    <p class="fin-note"><span id="fin-note-stmt">Figures are company-reported financial
      statements sourced from Yahoo Finance, up to the last {FIN_YEARS} fiscal years / most
      recent reported quarters. Five years is all this source carries — anything older simply
      isn't published in the feed, so it is left out rather than estimated. Banks and insurers
      often show "—" for cost of revenue / gross profit, which doesn't apply to their business
      model. </span>Indicators only, not investment advice.
      <span id="fin-checked"></span></p>
  </div>
</div>

<!-- ฉบับเต็มของแต่ละหมวดงบ — กดที่การ์ดในหน้างบการเงินเพื่อเปิด -->
<div id="metmodal" class="tmodal" hidden>
  <div class="cmodal-box" role="dialog" aria-modal="true" aria-label="Full metric detail">
    <div class="cmodal-head">
      <button type="button" class="backbtn" onclick="closeMetric()" aria-label="Back">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7"/></svg>
      </button>
      <button class="row-nav fin-nav" type="button" onclick="metNav(-1)"
              aria-label="Previous metric">‹</button>
      <div class="cmodal-title">
        <h3 id="met-name">—</h3>
        <div class="cmodal-price"><span id="met-sub"></span></div>
      </div>
      <button class="row-nav fin-nav" type="button" onclick="metNav(1)"
              aria-label="Next metric">›</button>
    </div>
    <div class="met-body" id="met-body"></div>
  </div>
</div>

<div class="navbar">
  <button class="burger" type="button" onclick="toggleNav()" aria-expanded="false"
          aria-controls="navpanel" aria-label="Menu">
    <span class="burger-bars"><span></span><span></span><span></span></span>
    <span class="burger-txt">MENU</span>
  </button>
  <span class="navbar-now" id="navbar-now">HOME</span>
  <div class="gsearch">
    <svg class="gsearch-ic" viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg>
    <input id="gsearch" type="search" autocomplete="off" placeholder="Search all news…"
           oninput="globalSearch(this.value)">
    <button class="gsearch-x" type="button" id="gsearch-x" hidden
            onclick="document.getElementById('gsearch').value='';globalSearch('');"
            aria-label="Clear search">×</button>
  </div>
</div>

<div class="navdim" id="navdim" onclick="toggleNav(false)"></div>
<aside class="navpanel" id="navpanel" aria-label="Menu">
  <div class="nav-head">
    <span>MENU</span>
    <button class="nav-x" type="button" onclick="toggleNav(false)" aria-label="Close">×</button>
  </div>
  <nav class="tabs" id="tabs" role="tablist" title="Drag to reorder">
    <button class="tab active" type="button" role="tab" draggable="true" data-id="all" data-scope="all" onclick="setScope('all')">HOME<span class="tab-n">{len(news)}</span></button>
    {''.join(f'''<button class="tab" type="button" role="tab" draggable="true" data-id="{sc}" data-scope="{sc}" onclick="setScope('{sc}')">{lb}<span class="tab-n">{groups[sc]["n"]}</span></button>''' for sc, lb in SCOPES)}
    <button class="tab tab-icon" type="button" draggable="true" data-id="chart" onclick="openCharts()">
      <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg>CHARTS</button>
    <button class="tab tab-icon" type="button" draggable="true" data-id="map" onclick="openMap()">
      <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c2.5 2.7 2.5 15.3 0 18M12 3c-2.5 2.7-2.5 15.3 0 18"/></svg>NEWS MAP</button>
    {live_tab}
  </nav>
  <div class="nav-foot">drag to reorder</div>
</aside>

<div class="gsearch-empty" id="gsearch-empty" hidden>No stories match your search.</div>

{front_page}

<div class="grid-side">
  <section class="panel">
    <div class="panel-head"><h2>TOP LOCATIONS</h2></div>
    <div>{''.join(hot_row(m, i) for i, m in enumerate(markers[:10])) or '<div class="hot"><span></span><span>—</span></div>'}</div>
  </section>
  <section class="panel">
    <div class="panel-head"><h2>TOP KEYWORDS</h2></div>
    <div class="kws">{''.join(kw_tile(k, i) for i, k in enumerate(kws))}</div>
  </section>
</div>

<footer>
  <span><span class="foot-mark">The Tribune</span> · {len(FEEDS)} sources ·
    {len(news)} stories in 24h · {located} geolocated</span>
  <span>auto-refresh every 15 min · rebuilt every {REBUILD_MIN} min</span>
</footer>

<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
<script>window.__MARKERS__ = {markers_json}; window.__ICONS__ = {icons_json};
window.__TNEWS__ = {tnews_json}; window.__CHARTS__ = {charts_json};
window.__LOGOS__ = {logos_json}; window.__FIN_AT__ = {fin_at_json};
window.__INFL__ = {infl_json}; window.__PULSE__ = {pulse_json};</script>
<script>{MAP_JS}</script>
<script>
// ── ค้นหาข่าวทั้งเว็บ (แถบบนสุด) ───────────────────────────
// ระหว่างค้นหา ต้องดึงข่าวที่พับ/ซ่อนไว้ในแท็บหมวดออกมาด้วย (คุมด้วย body.searching ใน CSS)
let gsearchT = null;
function globalSearch(q){{
  clearTimeout(gsearchT);
  gsearchT = setTimeout(() => applyGlobalSearch(q.trim().toLowerCase()), 90);
}}
function applyGlobalSearch(q){{
  document.body.classList.toggle('searching', !!q);
  document.getElementById('gsearch-x').hidden = !q;
  let anySite = false;
  document.querySelectorAll('.fp-col').forEach(col => {{
    let any = false;
    col.querySelectorAll('.fp-item').forEach(it => {{
      const hit = !q || it.textContent.toLowerCase().includes(q);
      it.classList.toggle('hidden', !hit);
      if (hit) any = true;
    }});
    col.classList.toggle('no-match', !!q && !any);
    if (any) anySite = true;
  }});
  document.getElementById('gsearch-empty').hidden = !q || anySite;
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
    : '<p class="tmodal-empty">No related stories this round</p>';
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
const CH_TF = ['1D','1M','3M','6M','1Y','3Y','5Y','10Y'];
const LOGOS = window.__LOGOS__ || {{}};

// อักษรย่ออยู่ข้างหลังเสมอ ถ้าโลโก้โหลดไม่ขึ้นก็ยังเห็นตัวย่อ
function assetLogo(label){{
  const ini = label.replace(/[^A-Za-z0-9]/g, '').slice(0, 2).toUpperCase() || '•';
  const img = LOGOS[label]
    ? `<img src="${{esc(LOGOS[label])}}" alt="" loading="lazy" onerror="this.remove()">` : '';
  return `<span class="clogo">${{ini}}${{img}}</span>`;
}}
let chCur = null, chTf = '3M', chCache = {{}}, chData = null, chZoom = null, chType = 'candle';
// ── เครื่องมือวิเคราะห์ทางเทคนิค ──────────────────────────
// d = คำอธิบาย: f ชื่อเต็ม · w วิเคราะห์อะไร · t ใช้เมื่อไร · u มักใช้ทำอะไร
const IND = [
  {{g: 'TREND', items: [
    {{k: 'sma20',  n: 'SMA 20',        c: '#4C8DFF', d: {{
      f: 'Simple Moving Average (20)',
      w: 'ค่าเฉลี่ยราคาปิด 20 แท่งล่าสุด ถ่วงน้ำหนักเท่ากันทุกแท่ง เกลี่ยความผันผวนระยะสั้นออกให้เห็นทิศทางหลัก',
      t: 'ใช้กับกรอบเวลาสั้น–กลาง เวลาอยากรู้ว่าราคาปัจจุบันสูงหรือต่ำกว่าค่าเฉลี่ยของช่วงที่ผ่านมา',
      u: 'ราคายืนเหนือเส้น = ฝั่งซื้อคุมเกม · ตัวเส้นเองมักถูกใช้เป็นแนวรับ-แนวต้านแบบเคลื่อนที่'}}}},
    {{k: 'sma50',  n: 'SMA 50',        c: '#F5A524', d: {{
      f: 'Simple Moving Average (50)',
      w: 'ค่าเฉลี่ยราคาปิด 50 แท่ง เป็นเส้นแนวโน้มระยะกลางที่นักลงทุนสถาบันอ้างถึงบ่อย',
      t: 'ใช้ยืนยันแนวโน้มระยะกลาง หรือคู่กับ SMA 200 เพื่อดูจุดตัด',
      u: 'SMA 50 ตัดขึ้นเหนือ SMA 200 เรียก golden cross · ตัดลงเรียก death cross'}}}},
    {{k: 'sma200', n: 'SMA 200',       c: '#9B8AFB', d: {{
      f: 'Simple Moving Average (200)',
      w: 'ค่าเฉลี่ยราคาปิด 200 แท่ง เป็นเส้นแบ่งตลาดกระทิง–หมีที่ใช้กันแพร่หลายที่สุด',
      t: 'ใช้กับกราฟรายวันขึ้นไป ดูภาพใหญ่ว่าสินทรัพย์ยังอยู่ในขาขึ้นระยะยาวหรือไม่',
      u: 'ราคาต่ำกว่าเส้นนี้ต่อเนื่อง มักถูกตีความว่าเข้าสู่ตลาดขาลงระยะยาว'}}}},
    {{k: 'ema12',  n: 'EMA 12',        c: '#2DD4BF', d: {{
      f: 'Exponential Moving Average (12)',
      w: 'ค่าเฉลี่ยที่ให้น้ำหนักราคาล่าสุดมากกว่าราคาย้อนหลัง จึงตอบสนองการเปลี่ยนแปลงไวกว่า SMA',
      t: 'ใช้เมื่อต้องการจับการกลับตัวเร็ว เช่นเทรดรอบสั้นหรือกราฟรายชั่วโมง',
      u: 'เป็นขาสั้นของ MACD · ใช้คู่กับ EMA 26 ดูจุดตัดของโมเมนตัม'}}}},
    {{k: 'ema26',  n: 'EMA 26',        c: '#E5484D', d: {{
      f: 'Exponential Moving Average (26)',
      w: 'ค่าเฉลี่ยแบบถ่วงน้ำหนักระยะกลาง ใช้เป็นเส้นอ้างอิงที่นิ่งกว่า EMA 12',
      t: 'ใช้คู่กับ EMA 12 เสมอ เพื่อดูว่าโมเมนตัมสั้นเร็วกว่าหรือช้ากว่าระยะกลาง',
      u: 'เป็นขายาวของ MACD · ระยะห่างระหว่างสองเส้นบอกความแรงของแนวโน้ม'}}}},
    {{k: 'sr',     n: 'Support / Resistance', c: '#C6A961', d: {{
      f: 'Auto Support & Resistance (pivot clustering)',
      w: 'หาจุดกลับตัวเฉพาะที่ (pivot high / pivot low) ในกรอบที่มองเห็น แล้วรวมจุดที่ราคาใกล้กันเป็นระดับเดียว',
      t: 'ใช้ก่อนเข้าออเดอร์ เพื่อรู้ว่าราคามีโอกาสชะลอหรือกลับตัวแถวไหน',
      u: 'ตั้งจุดเข้า–จุดตัดขาดทุน · เส้นที่ราคาเคยชนหลายครั้ง (เลข × สูง) ถือว่าเชื่อถือได้มากกว่า'}}}},
    {{k: 'regr',   n: 'Regression Channel', c: '#2DD4BF', d: {{
      f: 'Linear Regression Channel (±2σ)',
      w: 'เส้นถดถอยกำลังสองน้อยสุดของราคาปิดในกรอบที่มองเห็น พร้อมช่องเบี่ยงเบนมาตรฐาน 2 เท่า',
      t: 'ใช้เมื่ออยากวัดว่าเทรนด์ชันขึ้นหรือลงจริงไหม และตอนนี้ราคาแพงหรือถูกเทียบกับเส้นเทรนด์',
      u: 'ราคาชนขอบบน = ยืดเกินเทรนด์ · ชนขอบล่าง = ต่ำกว่าเทรนด์ · ความชันบอกทิศทาง'}}}},
    {{k: 'trend',  n: 'Trend Map', c: '#F5A524', d: {{
      f: 'Trend Map — ZigZag + trend channel + volatility cone',
      w: 'ลากขาขึ้น-ขาลงจริงของราคาจากจุดกลับตัว (ZigZag) แล้วต่อยอดสามอย่าง: ' +
         'ช่องแนวโน้มของขาปัจจุบัน · แนวรับแนวต้านที่ราคากำลังจะไปชน · ' +
         'และกรวยความผันผวนข้างหน้า',
      t: 'ใช้ตอนอยากเห็นภาพรวมว่าตอนนี้อยู่ขาไหน ขามาแล้วกี่ % กี่แท่ง ' +
         'และถ้าไปต่อจะไปชนอะไร ถ้าหลุดจะหลุดที่ไหน',
      u: 'ดูโครงสร้างเทรนด์ (ยอดสูงขึ้น-ฐานสูงขึ้น = ขาขึ้นจริง) · ' +
         'ตั้งจุดตัดขาดทุนใต้ฐานล่าสุด · ' +
         'กรวยบอกแค่ "ช่วงที่ราคาแกว่งได้" ไม่ได้บอกว่าจะขึ้นหรือลง'}}}},
    {{k: 'dc',     n: 'Donchian 20',   c: '#8FA0BC', d: {{
      f: 'Donchian Channel (20) — Richard Donchian',
      w: 'ราคาสูงสุดและต่ำสุดของ 20 แท่งล่าสุด วาดเป็นกรอบครอบราคา',
      t: 'ใช้กับระบบเทรดตามแนวโน้ม (trend following) โดยเฉพาะเวลาหาจังหวะเบรกเอาต์',
      u: 'ราคาทะลุขอบบน = เบรกขาขึ้น (หัวใจของระบบ Turtle Trading) · หลุดขอบล่าง = เบรกขาลง'}}}}]}},
  {{g: 'VOLATILITY', items: [
    {{k: 'bb',     n: 'Bollinger 20·2σ', c: '#8FA0BC', d: {{
      f: 'Bollinger Bands (20, 2σ) — John Bollinger',
      w: 'เส้นกลางคือ SMA 20 ขอบบน-ล่างคือบวกลบ 2 ส่วนเบี่ยงเบนมาตรฐาน จึงกว้างแคบตามความผันผวน',
      t: 'ใช้เมื่ออยากรู้ว่าราคาผันผวนมากผิดปกติไหม และตอนนี้อยู่ปลายกรอบหรือกลางกรอบ',
      u: 'กรอบบีบแคบ (squeeze) มักตามด้วยการเคลื่อนไหวแรง · ราคาชนขอบไม่ได้แปลว่าต้องกลับตัวเสมอ'}}}},
    {{k: 'atr',    n: 'ATR 14',        c: '#C6A961', pane: 'ATR 14', d: {{
      f: 'Average True Range (14) — J. Welles Wilder',
      w: 'ช่วงแกว่งจริงเฉลี่ยต่อแท่ง รวมช่องว่างราคาเปิด วัดเฉพาะ "ความแรง" ไม่บอกทิศทาง',
      t: 'ใช้ตอนวางแผนความเสี่ยงก่อนเข้าเทรด ไม่ใช่ตอนหาสัญญาณซื้อขาย',
      u: 'ตั้งจุดตัดขาดทุนเป็นกี่เท่าของ ATR · กำหนดขนาดโพซิชันให้เท่ากันในทุกสินทรัพย์'}}}}]}},
  {{g: 'MOMENTUM', items: [
    {{k: 'rsi',    n: 'RSI 14',        c: '#9B8AFB', pane: 'RSI 14', d: {{
      f: 'Relative Strength Index (14) — J. Welles Wilder',
      w: 'เทียบขนาดการขึ้นเฉลี่ยกับการลงเฉลี่ย 14 แท่ง ออกมาเป็นค่า 0–100',
      t: 'ใช้ตอนอยากรู้ว่าราคาวิ่งไปทางเดียวจนสุดโต่งหรือยัง',
      u: 'เกิน 70 = ซื้อมากเกินไป · ต่ำกว่า 30 = ขายมากเกินไป · ราคาทำจุดสูงใหม่แต่ RSI ไม่ทำตาม (divergence) เตือนโมเมนตัมอ่อน'}}}},
    {{k: 'macd',   n: 'MACD 12·26·9',  c: '#4C8DFF', pane: 'MACD', d: {{
      f: 'Moving Average Convergence Divergence — Gerald Appel',
      w: 'ผลต่าง EMA 12 กับ EMA 26 (เส้น MACD) เทียบกับ EMA 9 ของตัวมันเอง (เส้น signal) ส่วนแท่งคือผลต่างของสองเส้น',
      t: 'ใช้จับจังหวะที่โมเมนตัมเริ่มเปลี่ยนทิศ ก่อนที่เส้นค่าเฉลี่ยราคาจะตัดกัน',
      u: 'MACD ตัดขึ้นเหนือ signal = สัญญาณซื้อ · ตัดลง = สัญญาณขาย · แท่งฮิสโตแกรมหดตัวเตือนว่าแรงกำลังหมด'}}}}]}},
  {{g: 'VOLUME', items: [
    {{k: 'vol',    n: 'Volume',        c: '#7A879C', pane: 'VOLUME', d: {{
      f: 'Volume (ปริมาณการซื้อขาย)',
      w: 'จำนวนหน่วยที่ซื้อขายจริงในแต่ละแท่ง แท่งเขียว/แดงตามทิศทางราคาของแท่งนั้น',
      t: 'ดูควบคู่กับราคาเสมอ โดยเฉพาะตอนราคาทะลุแนวรับแนวต้าน',
      u: 'ราคาขึ้นพร้อมวอลุ่มหนา = การเคลื่อนไหวมีคนสนับสนุนจริง · ขึ้นแต่วอลุ่มบาง มักไปไม่ไกล'}}}},
    {{k: 'vwap',   n: 'VWAP 20',       c: '#F5A524', d: {{
      f: 'Volume Weighted Average Price (20)',
      w: 'ราคาเฉลี่ยที่ถ่วงน้ำหนักด้วยปริมาณซื้อขาย 20 แท่งล่าสุด จึงสะท้อนราคาที่เงินส่วนใหญ่ซื้อขายกันจริง',
      t: 'ใช้ตอนอยากรู้ว่าตัวเองได้ราคาดีกว่าหรือแย่กว่าตลาด',
      u: 'สถาบันใช้เป็นเกณฑ์วัดคุณภาพการส่งคำสั่ง · ราคาเหนือ VWAP = ผู้ซื้อยอมจ่ายแพงกว่าค่าเฉลี่ย'}}}}]}},
];
const IND_MAP = {{}};
IND.forEach(g => g.items.forEach(i => {{ IND_MAP[i.k] = i; }}));

let chInd = new Set(), chTools = new Set(['grid']);
try {{
  const a = JSON.parse(localStorage.getItem('chInd') || '[]');
  if (Array.isArray(a)) chInd = new Set(a.filter(k => IND_MAP[k]));
  const t = JSON.parse(localStorage.getItem('chTools') || '["grid"]');
  if (Array.isArray(t)) chTools = new Set(t);
}} catch(e) {{}}

// ค่าเฉลี่ย/โมเมนตัม — คืนอาร์เรย์ยาวเท่าข้อมูล ช่วงที่ยังคำนวณไม่ได้เป็น null
const smaA = (a, n) => {{
  const o = []; let s = 0;
  for (let i = 0; i < a.length; i++) {{
    s += a[i]; if (i >= n) s -= a[i - n];
    o.push(i >= n - 1 ? s / n : null);
  }}
  return o;
}};
const emaA = (a, n) => {{
  const k = 2 / (n + 1), o = []; let e = null;
  for (let i = 0; i < a.length; i++) {{
    e = e == null ? a[i] : a[i] * k + e * (1 - k);
    o.push(i >= n - 1 ? e : null);
  }}
  return o;
}};
const stdA = (a, n) => {{
  const m = smaA(a, n), o = [];
  for (let i = 0; i < a.length; i++) {{
    if (i < n - 1) {{ o.push(null); continue; }}
    let s = 0;
    for (let j = i - n + 1; j <= i; j++) s += (a[j] - m[i]) * (a[j] - m[i]);
    o.push(Math.sqrt(s / n));
  }}
  return o;
}};
// RSI แบบ Wilder (ค่าเฉลี่ยถ่วงน้ำหนักแบบเรียบ ไม่ใช่ SMA ธรรมดา)
const rsiA = (a, n) => {{
  const o = [null]; let g = 0, l = 0;
  for (let i = 1; i < a.length; i++) {{
    const d = a[i] - a[i - 1], up = Math.max(d, 0), dn = Math.max(-d, 0);
    if (i <= n) {{ g += up / n; l += dn / n; o.push(i < n ? null : 100 - 100 / (1 + g / (l || 1e-9))); }}
    else {{ g = (g * (n - 1) + up) / n; l = (l * (n - 1) + dn) / n;
            o.push(100 - 100 / (1 + g / (l || 1e-9))); }}
  }}
  return o;
}};
const macdA = a => {{
  const f = emaA(a, 12), s = emaA(a, 26);
  const m = a.map((_, i) => (f[i] != null && s[i] != null) ? f[i] - s[i] : null);
  const sig = emaA(m.map(v => v == null ? 0 : v), 9).map((v, i) => m[i] == null ? null : v);
  return {{m, sig, h: m.map((v, i) => (v == null || sig[i] == null) ? null : v - sig[i])}};
}};
const atrA = (rows, n) => emaA(rows.map((r, i) => i
  ? Math.max(r[2] - r[3], Math.abs(r[2] - rows[i - 1][4]), Math.abs(r[3] - rows[i - 1][4]))
  : r[2] - r[3]), n);
const vwapA = (rows, n) => {{
  const o = [], q = []; let pv = 0, vv = 0;
  for (let i = 0; i < rows.length; i++) {{
    const tp = (rows[i][2] + rows[i][3] + rows[i][4]) / 3, v = rows[i][5] || 1;
    q.push([tp * v, v]); pv += tp * v; vv += v;
    if (q.length > n) {{ const g = q.shift(); pv -= g[0]; vv -= g[1]; }}
    o.push(i >= n - 1 ? pv / vv : null);
  }}
  return o;
}};
const FIB = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1];
// สูงสุด/ต่ำสุดแบบเลื่อนหน้าต่าง ใช้กับ Donchian
const rollA = (a, n, pick) => a.map((_, i) =>
  i < n - 1 ? null : pick.apply(null, a.slice(i - n + 1, i + 1)));

// แนวรับ-แนวต้านอัตโนมัติ: หาจุดกลับตัวเฉพาะที่ แล้วรวมจุดที่ราคาใกล้กันเป็นระดับเดียว
function srLevels(vis){{
  // ช่วงสั้นมีแท่งน้อย ต้องผ่อนเงื่อนไขจุดกลับตัวลง ไม่งั้นจะไม่เจอเลย
  const k = vis.length < 45 ? 2 : 3, piv = [];
  for (let i = k; i < vis.length - k; i++) {{
    let ph = true, pl = true;
    for (let j = i - k; j <= i + k; j++) {{
      // ใช้ > ไม่ใช่ >= เพราะราคาปิดเท่ากันหลายแท่งเป็นเรื่องปกติ
      if (j === i) continue;
      if (vis[j][2] > vis[i][2]) ph = false;
      if (vis[j][3] < vis[i][3]) pl = false;
    }}
    if (ph) piv.push(vis[i][2]);
    if (pl) piv.push(vis[i][3]);
  }}
  if (piv.length < 2) return [];
  piv.sort((a, b) => a - b);
  const hi = d3.max(vis, r => r[2]), lo = d3.min(vis, r => r[3]);
  const tol = Math.max((hi - lo) * 0.02, ((hi + lo) / 2) * 0.004) || 1e-6;
  const groups = [[piv[0]]];
  for (let i = 1; i < piv.length; i++) {{
    const g = groups[groups.length - 1];
    if (piv[i] - g[g.length - 1] <= tol) g.push(piv[i]);
    else groups.push([piv[i]]);
  }}
  const all = groups.map(g => ({{v: g.reduce((a, b) => a + b, 0) / g.length, n: g.length}}));
  const strong = all.filter(l => l.n >= 2).sort((a, b) => b.n - a.n).slice(0, 6);
  if (strong.length >= 2) return strong;
  // ช่วงสั้นๆ มีจุดกลับตัวน้อย ใช้จุดเดี่ยวที่ใกล้ราคาปัจจุบันที่สุดแทน
  const last = vis[vis.length - 1][4];
  return all.sort((a, b) => Math.abs(a.v - last) - Math.abs(b.v - last)).slice(0, 4);
}}

// ช่องแนวโน้มจากเส้นถดถอยกำลังสองน้อยสุด ± 2 ส่วนเบี่ยงเบนมาตรฐาน
function regrChannel(vis){{
  const n = vis.length;
  if (n < 5) return null;
  let sx = 0, sy = 0, sxy = 0, sxx = 0;
  for (let i = 0; i < n; i++) {{
    const y = vis[i][4];
    sx += i; sy += y; sxy += i * y; sxx += i * i;
  }}
  const den = n * sxx - sx * sx;
  if (!den) return null;
  const b = (n * sxy - sx * sy) / den, a = (sy - b * sx) / n;
  let se = 0;
  for (let i = 0; i < n; i++) {{ const d = vis[i][4] - (a + b * i); se += d * d; }}
  return {{a, b, sd: 2 * Math.sqrt(se / n), n}};
}}
// ── โมเดลเทรนด์: ลากขาขึ้น-ขาลงจริงจากจุดกลับตัว ─────────────
// เกณฑ์การกลับตัวปรับตามความผันผวนของช่วงที่ดูอยู่ ไม่ใช้ค่าคงที่
// เพราะ 5% ของบิตคอยน์กับ 5% ของหุ้นธนาคารไม่ใช่เรื่องเดียวกัน
function zigzagPivots(vis){{
  const n = vis.length;
  if (n < 12) return [];
  const rets = [];
  for (let i = 1; i < n; i++) {{
    const a = vis[i - 1][4], b = vis[i][4];
    if (a > 0 && b > 0) rets.push(Math.abs(Math.log(b / a)));
  }}
  if (!rets.length) return [];
  rets.sort((x, y) => x - y);
  const med = rets[Math.floor(rets.length / 2)] || 0.01;
  const thr = Math.max(0.028, Math.min(0.15, med * 2.2 * Math.sqrt(10)));

  const piv = [];
  let dir = 0, hiI = 0, hiV = vis[0][2], loI = 0, loV = vis[0][3];
  for (let i = 1; i < n; i++) {{
    const hi = vis[i][2], lo = vis[i][3];
    if (dir === 0) {{
      if (hi > hiV) {{ hiV = hi; hiI = i; }}
      if (lo < loV) {{ loV = lo; loI = i; }}
      if (hi >= loV * (1 + thr)) {{
        piv.push({{i: loI, v: loV, t: 'L'}}); dir = 1; hiV = hi; hiI = i;
      }} else if (lo <= hiV * (1 - thr)) {{
        piv.push({{i: hiI, v: hiV, t: 'H'}}); dir = -1; loV = lo; loI = i;
      }}
    }} else if (dir === 1) {{
      if (hi > hiV) {{ hiV = hi; hiI = i; }}
      else if (lo <= hiV * (1 - thr)) {{
        piv.push({{i: hiI, v: hiV, t: 'H'}}); dir = -1; loV = lo; loI = i;
      }}
    }} else {{
      if (lo < loV) {{ loV = lo; loI = i; }}
      else if (hi >= loV * (1 + thr)) {{
        piv.push({{i: loI, v: loV, t: 'L'}}); dir = 1; hiV = hi; hiI = i;
      }}
    }}
  }}
  if (!piv.length) return [];
  // ปลายขาปัจจุบันยังไม่ถูกยืนยันว่าเป็นจุดกลับตัว แต่ต้องมีไว้ลากขาที่กำลังวิ่งอยู่
  piv.push(dir === 1 ? {{i: hiI, v: hiV, t: 'H', open: true}}
                     : {{i: loI, v: loV, t: 'L', open: true}});
  return piv.length >= 2 ? piv : [];
}}

// ช่องแนวโน้มของขาปัจจุบัน + กรวยความผันผวนข้างหน้า
function trendModel(vis){{
  const piv = zigzagPivots(vis);
  if (piv.length < 2) return null;

  const legs = [];
  for (let k = 0; k + 1 < piv.length; k++) {{
    const a = piv[k], b = piv[k + 1];
    if (b.i <= a.i) continue;
    legs.push({{a, b, up: b.v > a.v, pct: (b.v / a.v - 1) * 100, bars: b.i - a.i}});
  }}
  if (!legs.length) return null;
  const cur = legs[legs.length - 1];

  // เส้นช่อง: ขาขึ้นลากผ่านจุดต่ำของแท่งในขานั้น ขาลงลากผ่านจุดสูง
  const s = cur.a.i, e = cur.b.i;
  const idx = [];
  for (let i = s; i <= e; i++) idx.push(i);
  const pick = i => cur.up ? vis[i][3] : vis[i][2];
  let sx = 0, sy = 0, sxy = 0, sxx = 0;
  for (const i of idx) {{
    const y = pick(i);
    sx += i; sy += y; sxy += i * y; sxx += i * i;
  }}
  const m = idx.length, den = m * sxx - sx * sx;
  const slope = den ? (m * sxy - sx * sy) / den : 0;
  const inter = (sy - slope * sx) / m;
  // ยกเส้นคู่ขนานไปแตะฝั่งตรงข้ามที่ไกลที่สุด
  let off = 0;
  for (const i of idx) {{
    const other = cur.up ? vis[i][2] : vis[i][3];
    const d = other - (inter + slope * i);
    if (cur.up ? d > off : d < off) off = d;
  }}

  // โครงสร้างเทรนด์: ยอดสูงขึ้น + ฐานสูงขึ้น = ขาขึ้นจริง
  const highs = piv.filter(p => p.t === 'H').slice(-3);
  const lows = piv.filter(p => p.t === 'L').slice(-3);
  const rising = a => a.length >= 2 && a[a.length - 1].v > a[a.length - 2].v;
  const falling = a => a.length >= 2 && a[a.length - 1].v < a[a.length - 2].v;
  let structure = 'ไม่ชัด';
  if (rising(highs) && rising(lows)) structure = 'ยอดสูงขึ้น ฐานสูงขึ้น';
  else if (falling(highs) && falling(lows)) structure = 'ยอดต่ำลง ฐานต่ำลง';

  // ความผันผวนต่อแท่ง ใช้กางกรวยข้างหน้า (ส่วนนี้ทำนายได้จริง ต่างจากทิศทาง)
  const tail = vis.slice(Math.max(0, vis.length - 20));
  const lr = [];
  for (let i = 1; i < tail.length; i++) {{
    const a = tail[i - 1][4], b = tail[i][4];
    if (a > 0 && b > 0) lr.push(Math.log(b / a));
  }}
  const mu = lr.reduce((x, y) => x + y, 0) / (lr.length || 1);
  const sd = Math.sqrt(lr.reduce((x, y) => x + (y - mu) * (y - mu), 0) / Math.max(1, lr.length - 1));

  return {{piv, legs, cur, slope, inter, off, structure, sd,
           last: vis[vis.length - 1][4], lastI: vis.length - 1}};
}}

let chLevels = {{}}, chOverlays = new Set();
const OV_STYLE = {{
  fair:   {{color: 'var(--biz)',   label: 'FAIR VALUE'}},
  target: {{color: 'var(--mixed)', label: 'TARGET'}},
  base:   {{color: 'var(--poli)',  label: 'BASE'}},
  trend:  {{color: 'var(--econ)',  label: 'TREND'}},
}};

function pickType(t){{
  chType = t;
  document.querySelectorAll('.ctype .tfbtn').forEach(b => b.classList.toggle('on', b.dataset.ct === t));
  renderChart();
}}

// ชื่อเต็มบริษัทมาจาก meta ของกราฟที่ Yahoo แนบมาให้อยู่แล้ว — ดัชนี/ค่าเงิน/ทอง
// ไม่มีชื่อเต็มที่ต่างจากตัวย่อ ก็ไม่ต้องโชว์บรรทัดเปล่า
function symFull(label){{
  const n = (CHARTS[label] || {{}}).n;
  return (n && n.toLowerCase() !== String(label).toLowerCase()) ? n : '';
}}
function setSymFull(id, label){{
  const el = document.getElementById(id);
  if (!el) return;
  const n = symFull(label);
  el.textContent = n;
  el.hidden = !n;
}}

// ── ขยายกราฟเต็มหน้าต่าง (กดอีกทีหรือ Esc เพื่อย่อกลับ) ──────────
// ตอนขยายจะเด้งไปช่วง 10 ปีให้เลย เพราะจุดประสงค์คือ "ดูฉบับเต็ม"
// แต่ถ้าผู้ใช้เลือกช่วงอื่นเองอยู่แล้วก็เคารพของเดิม ไม่ไปเปลี่ยนให้
let chFullPrevTf = null;
function chartFullOpen(){{
  return document.querySelector('#cmodal .cmodal-box').classList.contains('chart-full');
}}
function toggleChartFull(force){{
  const box = document.querySelector('#cmodal .cmodal-box');
  const want = (force === undefined) ? !box.classList.contains('chart-full') : !!force;
  box.classList.toggle('chart-full', want);
  const btn = document.getElementById('cexpand');
  btn.classList.toggle('on', want);
  btn.setAttribute('aria-pressed', want ? 'true' : 'false');
  document.getElementById('cexpand-t').textContent = want ? 'EXIT FULL VIEW' : 'FULL VIEW';
  document.getElementById('cexpand-ic').innerHTML = want
    ? '<path d="M9 4v5H4M15 20v-5h5M20 9h-5V4M4 15h5v5"/>'
    : '<path d="M4 9V4h5M20 15v5h-5M15 4h5v5M9 20H4v-5"/>';
  if (want) {{
    chFullPrevTf = chTf;
    if (chData && chData.tf && chData.tf['10Y'] && chData.tf['10Y'].length) pickTf('10Y');
  }} else if (chFullPrevTf) {{
    pickTf(chFullPrevTf);
    chFullPrevTf = null;
  }}
  renderFullBar();
  renderChart();
}}

// แถบสรุปช่วงยาว — คำนวณจากแท่งที่มีจริงในชุด 10 ปี ถ้าหุ้นเพิ่งเข้าตลาดก็บอกตามจริงว่าสั้นกว่า
function renderFullBar(){{
  const bar = document.getElementById('cfull-bar');
  if (!bar) return;
  const rows = (chData && chData.tf && chData.tf['10Y']) || null;
  if (!rows || rows.length < 2) {{ bar.innerHTML = ''; return; }}
  const closes = rows.map(r => r[4]).filter(v => v != null && isFinite(v));
  if (closes.length < 2) {{ bar.innerHTML = ''; return; }}
  const first = closes[0], last = closes[closes.length - 1];
  const hi = Math.max(...rows.map(r => r[2]).filter(isFinite));
  const lo = Math.min(...rows.map(r => r[3]).filter(isFinite));
  const spanYrs = (rows[rows.length - 1][0] - rows[0][0]) / 31557600;
  const chg = (last / first - 1) * 100;
  const cagr = spanYrs >= 1 && first > 0 && last > 0
    ? (Math.pow(last / first, 1 / spanYrs) - 1) * 100 : null;
  const fmtP = v => v >= 1000 ? v.toFixed(0) : v.toFixed(2);
  const cls = v => v >= 0 ? 'up' : 'down';
  const sgn = v => (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  // สองรูป: "10-year change" (ขยายคำนาม) กับ "10 years of monthly bars" (เป็นคำนามเอง)
  const yrTxt = spanYrs >= 9.5 ? '10-year' : spanYrs.toFixed(1) + '-year';
  const yrNoun = spanYrs >= 9.5 ? '10 years' : spanYrs.toFixed(1) + ' years';
  bar.innerHTML =
    `<div class="cfull-stat"><span>${{esc(yrTxt)}} change</span>` +
      `<b class="${{cls(chg)}}">${{sgn(chg)}}</b></div>` +
    (cagr != null ? `<div class="cfull-stat"><span>annualised</span>` +
      `<b class="${{cls(cagr)}}">${{sgn(cagr)}}</b></div>` : '') +
    `<div class="cfull-stat"><span>period high</span><b>${{fmtP(hi)}}</b></div>` +
    `<div class="cfull-stat"><span>period low</span><b>${{fmtP(lo)}}</b></div>` +
    `<div class="cfull-stat"><span>monthly bars</span><b>${{rows.length}}</b></div>` +
    `<div class="cfull-note">Longest price history this source carries for ` +
      `${{esc(chCur || '')}} — ${{esc(yrNoun)}} of monthly bars. Financial statements ` +
      `only go back 5 years, so the two spans differ.</div>`;
}}

// ความชันเฉลี่ยของชุดตัวเลข (least squares) ใช้บอกทิศทางเทรนด์
function linSlope(ys){{
  const n = ys.length;
  if (n < 2) return 0;
  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (let i = 0; i < n; i++) {{ sx += i; sy += ys[i]; sxx += i * i; sxy += i * ys[i]; }}
  const den = n * sxx - sx * sx;
  return den ? (n * sxy - sx * sy) / den : 0;
}}

// เงินเฟ้อของประเทศที่หุ้นตัวนั้นจดทะเบียนอยู่ — เป็นตัวเลขมหภาค ไม่ใช่ของบริษัท
// จึงเขียนงวดกับแหล่งกำกับไว้ด้วย ฝั่งสหรัฐฯ เป็นรายเดือน ฝั่งไทยเป็นรายปี ไม่ได้สดเท่ากัน
const INFL = window.__INFL__ || {{}};
function inflRow(){{
  const grp = (TNEWS[chCur] || {{}}).group || (CHARTS[chCur] || {{}}).g || 'intl';
  const d = INFL[grp] || INFL.intl;
  if (!d) return '';
  const cls = d.rate > 3 ? 'down' : d.rate < 0 ? 'up' : '';
  return `<div class="calc-row"><span class="calc-k">INFLATION` +
    `<small>${{esc(d.label)}} · ${{esc(d.freq)}} · ${{esc(d.src)}}</small></span>` +
    `<span class="calc-v ${{cls}}">${{(d.rate >= 0 ? '+' : '') + d.rate.toFixed(2)}}%` +
    `<small>${{esc(d.period)}}</small></span></div>`;
}}

// อัตราผลตอบแทนขั้นต่ำที่ต้องทำให้ได้เพื่อ "ไม่จน" — เงินเฟ้อกินกำลังซื้อไปเท่าไหร่
// ต้องได้คืนอย่างน้อยเท่านั้น และถ้าผลตอบแทนโดนหักภาษี ต้องทำได้มากกว่านั้นอีก
//   ได้ก่อนภาษี r → เหลือจริง r(1-t) ; ให้เท่าเงินเฟ้อ i ⇒ r = i / (1 - t)
// อัตราภาษีของแต่ละคนไม่เท่ากัน จึงไม่เดาให้ แต่โชว์ทั้ง 0% / 10% / 20% ให้เทียบเอง
function hurdleRow(){{
  const grp = (TNEWS[chCur] || {{}}).group || (CHARTS[chCur] || {{}}).g || 'intl';
  const d = INFL[grp] || INFL.intl;
  if (!d) return '';
  const i = d.rate / 100;
  const pc = v => (v * 100).toFixed(2) + '%';

  // ผลตอบแทนจริงของหุ้นตัวนี้รอบปีที่ผ่านมา เอามาเทียบกับเส้นที่ต้องข้าม
  let actual = null;
  const daily = (chData && chData.tf && chData.tf['1Y']) || null;
  if (daily && daily.length > 20) {{
    const c = daily.map(b => b[4]).filter(v => v != null && isFinite(v));
    if (c.length > 20 && c[0] > 0) actual = c[c.length - 1] / c[0] - 1;
  }}

  if (i <= 0) {{
    // เงินเฟ้อติดลบ = ของถูกลง ไม่มีเส้นให้ข้าม ผลตอบแทนบวกนิดเดียวก็ได้กำลังซื้อเพิ่ม
    return `<div class="calc-row"><span class="calc-k">BEAT INFLATION` +
      `<small>prices fell ${{pc(Math.abs(i))}} — any positive return gains purchasing power</small></span>` +
      `<span class="calc-v up">0.00%` +
      (actual != null ? `<small>this stock ${{actual >= 0 ? '+' : ''}}${{(actual * 100).toFixed(1)}}% over 1Y</small>` : '') +
      `</span></div>`;
  }}
  const t10 = i / 0.9, t20 = i / 0.8;
  const beats = actual != null ? actual > i : null;
  return `<div class="calc-row"><span class="calc-k">BEAT INFLATION` +
    `<small>break-even before tax · ${{pc(t10)}} if taxed 10% · ${{pc(t20)}} if taxed 20%</small></span>` +
    `<span class="calc-v ${{beats === null ? '' : (beats ? 'up' : 'down')}}">${{pc(i)}}` +
    (actual != null
      ? `<small>this stock ${{actual >= 0 ? '+' : ''}}${{(actual * 100).toFixed(1)}}% over 1Y — ` +
        `${{beats ? 'ahead of' : 'behind'}} inflation</small>`
      : '') +
    `</span></div>`;
}}

function renderCalc(){{
  const box = document.getElementById('ccalc');
  const rows = (chData?.tf || {{}})[chTf] || [];
  const f = (TNEWS[chCur] || {{}}).fund || {{}};
  if (!rows.length) {{ box.innerHTML = ''; return; }}
  const closes = rows.map(r => r[4]);
  const last = closes[closes.length - 1];
  const fmt = n => d3.format(Math.abs(n) >= 1000 ? ',.0f' : ',.2f')(n);

  const total = closes[0] ? linSlope(closes) * (closes.length - 1) / closes[0] * 100 : 0;
  const tr = total > 3 ? ['RISING', 'up'] : total < -3 ? ['FALLING', 'down'] : ['SIDEWAYS', 'flat'];

  // ราคาฐาน = ค่าเฉลี่ยของจุดต่ำสุด 20% ล่างในกรอบเวลานี้
  const lows = rows.map(r => r[3]).slice().sort((a, b) => a - b);
  const k = Math.max(1, Math.round(lows.length * 0.2));
  const base = lows.slice(0, k).reduce((a, b) => a + b, 0) / k;

  // มูลค่าเหมาะสมแบบ Graham ใช้ได้เฉพาะหุ้นที่มีกำไรและมูลค่าตามบัญชีเป็นบวก
  const gr = (f.eps > 0 && f.bvps > 0) ? Math.sqrt(22.5 * f.eps * f.bvps) : null;

  // เก็บค่าไว้ให้กราฟวาดเส้นทับ (คลิกที่แถวเพื่อเปิด/ปิด)
  chLevels = {{
    fair: gr, target: f.target != null ? f.target : null, base: base,
    trend: {{first: closes[0], slope: linSlope(closes)}},
  }};

  const row = (label, sub, value, cls, vsub, key) =>
    `<div class="calc-row${{key ? ' calc-on-able' : ''}}"${{key ? ` data-ov="${{key}}"` : ''}}>` +
    `<span class="calc-k">${{label}}<small>${{sub}}</small></span>` +
    `<span class="calc-v ${{cls || ''}}">${{value}}${{vsub ? `<small>${{vsub}}</small>` : ''}}</span></div>`;

  box.innerHTML =
    row('P/E', 'trailing 12 months', f.pe != null ? fmt(f.pe) : '—',
        f.pe != null ? '' : 'calc-na', f.fpe != null ? 'forward ' + fmt(f.fpe) : '') +
    row('FAIR VALUE', gr ? 'Graham √(22.5×EPS×BVPS)' : 'needs positive EPS and book value',
        gr ? fmt(gr) : '—', gr ? (gr > last ? 'up' : 'down') : 'calc-na',
        gr ? ((gr / last - 1) * 100).toFixed(1) + '% vs current price' : '',
        gr ? 'fair' : '') +
    row('ANALYST TARGET', 'mean of broker targets', f.target != null ? fmt(f.target) : '—',
        f.target != null ? (f.target > last ? 'up' : 'down') : 'calc-na', '',
        f.target != null ? 'target' : '') +
    row('TREND', 'slope of closes over ' + chTf, tr[0], tr[1],
        (total >= 0 ? '+' : '') + total.toFixed(1) + '%', 'trend') +
    row('BASE PRICE', 'mean of lowest 20% of lows over ' + chTf, fmt(base), '',
        ((last / base - 1) * 100).toFixed(1) + '% above base', 'base') +
    inflRow() + hurdleRow() +
    '<p class="calc-note">Click a highlighted row to plot it on the chart · ' +
    'computed from price history and available fundamentals · ' +
    'indicators only, not investment advice</p>';

  box.querySelectorAll('.calc-on-able').forEach(r => {{
    r.classList.toggle('on', chOverlays.has(r.dataset.ov));
    r.addEventListener('click', () => {{
      const k = r.dataset.ov;
      chOverlays.has(k) ? chOverlays.delete(k) : chOverlays.add(k);
      r.classList.toggle('on', chOverlays.has(k));
      renderChart();
    }});
  }});
}}

// ── รายชื่อสินทรัพย์ + รายการโปรด ────────────────────────
// ค่าเริ่มต้นของรายการโปรด = ตัวที่มีราคาสดในแถบราคา จะได้ไม่ขึ้นทั้งตลาดตั้งแต่แรก
let chFavs = new Set(Object.keys(TNEWS).filter(l => CHARTS[l]));
let chMode = 'fav';
try {{
  const saved = JSON.parse(localStorage.getItem('chFavs') || 'null');
  if (Array.isArray(saved)) chFavs = new Set(saved);
  chMode = localStorage.getItem('chMode') === 'all' ? 'all' : 'fav';
}} catch(e) {{}}
const saveFavs = () => {{
  try {{ localStorage.setItem('chFavs', JSON.stringify([...chFavs])); }} catch(e) {{}}
}};

// ลำดับที่ลากจัดเองในแท็บ FAVORITES — ตัวที่ยังไม่เคยลากจะเรียงตามค่าเริ่มต้น
// (ราคาสดก่อน แล้วค่อย % เปลี่ยนแปลง) ต่อท้ายตัวที่ลากจัดไว้แล้วเสมอ
let favOrder = [];
try {{ favOrder = JSON.parse(localStorage.getItem('favOrder') || '[]'); }} catch(e) {{}}
function saveFavOrder(){{
  favOrder = [...document.querySelectorAll('#cmodal-list .citem')].map(el => el.dataset.label);
  try {{ localStorage.setItem('favOrder', JSON.stringify(favOrder)); }} catch(e) {{}}
}}

// กลุ่ม THAILAND/GLOBAL พับเก็บได้ทีละกลุ่ม จำไว้ข้ามเซสชัน
// วิธีเรียงหุ้นไทยในแท็บรายการโปรด: 'yield' เรียงตามปันผลอัตโนมัติ · 'manual' ลากจัดเอง
// สองอย่างนี้อยู่ด้วยกันไม่ได้ เพราะการเรียงอัตโนมัติจะทับลำดับที่ลากไว้ทุกครั้งที่วาดใหม่
// จึงให้เลือกเอาว่าจะใช้แบบไหน แล้วจำไว้ข้ามเซสชัน
let thSort = 'yield';
try {{ thSort = localStorage.getItem('thSort') === 'manual' ? 'manual' : 'yield'; }} catch(e) {{}}
function toggleThSort(ev){{
  ev.stopPropagation();                 // อย่าให้ไปโดนปุ่มพับกลุ่มที่ครอบอยู่
  thSort = thSort === 'yield' ? 'manual' : 'yield';
  try {{ localStorage.setItem('thSort', thSort); }} catch(e) {{}}
  renderAssetList(document.getElementById('csearch').value);
}}

let favFolded = new Set();
try {{ favFolded = new Set(JSON.parse(localStorage.getItem('favFold') || '[]')); }} catch(e) {{}}
function toggleFavFold(g){{
  favFolded.has(g) ? favFolded.delete(g) : favFolded.add(g);
  try {{ localStorage.setItem('favFold', JSON.stringify([...favFolded])); }} catch(e) {{}}
  document.querySelector(`.cgroup[data-g="${{g}}"]`)?.classList.toggle('folded', favFolded.has(g));
}}

function toggleFav(ev, label){{
  ev.stopPropagation();
  if (chFavs.has(label)) {{
    chFavs.delete(label);
  }} else {{
    chFavs.add(label);
    // ติดดาวปุ๊บดึงงบเตรียมไว้เลย พอกดเข้าไปดูจะได้ไม่ต้องรอโหลด
    if (!netIsFrugal()) loadFin(label);
  }}
  saveFavs();
  renderAssetList(document.getElementById('csearch').value);
}}

function setAssetMode(m){{
  chMode = m;
  try {{ localStorage.setItem('chMode', m); }} catch(e) {{}}
  document.querySelectorAll('.cfav-tab').forEach(b =>
    b.classList.toggle('on', b.dataset.mode === m));
  renderAssetList(document.getElementById('csearch').value);
}}

// เรียงตัวที่อยู่ในแถบราคาก่อน แล้วค่อยเรียง % มากไปน้อย
function renderAssetList(q){{
  const term = (q || '').trim().toLowerCase();
  const groups = {{th: 'THAILAND', intl: 'GLOBAL', rate: 'INTEREST RATE'}};
  let html = '', shown = 0;
  for (const [g, title] of Object.entries(groups)) {{
    const rows = Object.entries(CHARTS)
      // กลุ่มดอกเบี้ยมีแค่สองตัวและไม่ได้อยู่ในรายการโปรดของใครโดยปริยาย — ถ้าให้ซ่อนตาม
      // โหมด FAVORITES ก็จะไม่มีใครหาเจอ จึงโชว์ตลอดทั้งสองโหมด (ยังค้นหาได้ตามปกติ)
      .filter(([l, c]) => (c.g || 'intl') === g
        && (chMode === 'all' || g === 'rate' || chFavs.has(l))
        && (!term || l.toLowerCase().includes(term)
            || (c.n || '').toLowerCase().includes(term)))
      .sort((a, b) => {{
        // กลุ่มไทยในแท็บรายการโปรด เรียงตามอัตราปันผลมากไปน้อย ตัวที่ไม่มีปันผลไปท้ายกลุ่ม
        // กฎนี้ต้องมาก่อนลำดับที่ลากเอง เพราะ saveFavOrder เก็บ "ทุกตัวที่เห็นในลิสต์" ตั้งแต่
        // ลากครั้งแรก ถ้าให้ลำดับลากชนะ คนที่เคยลากสักครั้งจะไม่มีวันเห็นการเรียงตามปันผลเลย
        // (กลุ่ม GLOBAL ยังลากจัดเองได้ตามเดิม ไม่ได้แตะ)
        if (chMode === 'fav' && g === 'th' && thSort === 'yield') {{
          const ya = CHARTS[a[0]].y, yb = CHARTS[b[0]].y;
          if (ya != null || yb != null) {{
            if (ya == null) return 1;
            if (yb == null) return -1;
            if (yb !== ya) return yb - ya;
          }}
        }}
        // โหมด FAVORITES: เคารพลำดับที่ลากจัดเองก่อน ตัวที่ยังไม่เคยลากตกไปท้ายกลุ่ม
        if (chMode === 'fav' && favOrder.length) {{
          const ia = favOrder.indexOf(a[0]), ib = favOrder.indexOf(b[0]);
          if (ia !== -1 || ib !== -1) {{
            if (ia === -1) return 1;
            if (ib === -1) return -1;
            return ia - ib;
          }}
        }}
        const A = TNEWS[a[0]], B = TNEWS[b[0]];
        if (!!A !== !!B) return A ? -1 : 1;             // ตัวที่มีราคาสดขึ้นก่อน
        if (A && B) return (B.pctv ?? 0) - (A.pctv ?? 0);
        return a[0].localeCompare(b[0]);
      }});
    if (!rows.length) continue;
    shown += rows.length;
    // ลากได้เฉพาะตอนที่ลำดับจะอยู่จริง — กลุ่มไทยโหมดเรียงตามปันผลจะทับลำดับที่ลากทุกครั้ง
    // ที่วาดใหม่ ปล่อยให้ลากได้แล้วเด้งกลับแย่กว่าปิดไปเลย (โหมด ALL ปิดด้วยเหตุผลเดียวกัน)
    // กลุ่มดอกเบี้ยโชว์ตลอดไม่ขึ้นกับรายการโปรด การลากจัดลำดับจึงไม่มีความหมาย
    // (ซ้ำร้ายจะไปปนอยู่ใน favOrder ที่เก็บทุกแถวที่มองเห็น) ปิดไปเลย
    const thAuto = g === 'th' && thSort === 'yield';
    const draggable = chMode === 'fav' && !thAuto && g !== 'rate';
    const showYld = chMode === 'fav' && thAuto;
    const folded = favFolded.has(g) ? ' folded' : '';
    html += `<div class="cgroup${{folded}}" data-g="${{g}}" role="button" tabindex="0"
        onclick="toggleFavFold('${{g}}')" onkeydown="if(event.key==='Enter')toggleFavFold('${{g}}')">
        <svg class="fold-caret" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg>
        ${{title}} · ${{rows.length}}${{
          chMode === 'fav' && g === 'th'
            ? `<button type="button" class="th-sort" onclick="toggleThSort(event)"
                 title="${{thSort === 'yield'
                   ? 'เรียงตามปันผลอยู่ — กดเพื่อกลับไปลากจัดลำดับเอง'
                   : 'ลากจัดลำดับเองอยู่ — กดเพื่อเรียงตามปันผลอัตโนมัติ'}}">${{
                 thSort === 'yield' ? 'ปันผล' : 'ลากเอง'}}</button>` : ''
        }}</div><div class="cfav-group" data-group="${{g}}">` +
      rows.map(([l]) => {{
        const d = TNEWS[l], f = chFavs.has(l);
        return `<div class="citem" role="button" tabindex="0" data-label="${{esc(l)}}"
          ${{draggable ? 'draggable="true"' : ''}}
          onclick="pickChart('${{esc(l)}}')" onkeydown="if(event.key==='Enter')pickChart('${{esc(l)}}')">
          ${{assetLogo(l)}}<span class="cname">${{esc(l)}}</span>
          ${{showYld && CHARTS[l].y != null
              ? `<span class="cyld" title="Dividend yield, trailing 12 months">${{CHARTS[l].y}}%</span>` : ''}}
          <span class="cpct ${{d ? d.dir : ''}}">${{d ? d.pct : ''}}</span>
          <span class="cfav${{f ? ' on' : ''}}" role="button" tabindex="-1"
            title="${{f ? 'Remove from favorites' : 'Add to favorites'}}"
            onclick="toggleFav(event,'${{esc(l)}}')">${{f ? '★' : '☆'}}</span></div>`;
      }}).join('') + `</div>`;
  }}
  if (!shown) {{
    html = term ? '<p class="cnone">No symbol matches</p>'
      : (chMode === 'fav'
        ? '<p class="cnone-hint">No favorites yet.<br>Open <b>ALL</b> and tap ☆ next to a symbol to pin it here.</p>'
        : '<p class="cnone">No symbols</p>');
  }}
  document.getElementById('cmodal-list').innerHTML = html;
  document.querySelectorAll('.citem').forEach(b =>
    b.classList.toggle('on', b.dataset.label === chCur));
}}

function filterAssets(q){{ renderAssetList(q); }}

function toggleCalc(){{
  const side = document.getElementById('cmodal-side');
  const folded = side.classList.toggle('calc-folded');
  side.querySelector('.cfold').setAttribute('aria-expanded', folded ? 'false' : 'true');
  try {{ localStorage.setItem('calcFold', folded ? '1' : '0'); }} catch(e) {{}}
}}
try {{ if (localStorage.getItem('calcFold') === '1') toggleCalc(); }} catch(e) {{}}

function openCharts(){{
  const modal = document.getElementById('cmodal');
  if (!Object.keys(CHARTS).length) return;
  if (!document.getElementById('cmodal-list').childElementCount) {{
    setAssetMode(chMode);           // เปิดมาที่โหมดเดิมที่ผู้ใช้เลือกไว้
    renderIndList();
    syncTools();
    document.getElementById('cmodal-tf').innerHTML = CH_TF.map(t =>
      `<button class="tfbtn" type="button" data-tf="${{t}}" onclick="pickTf('${{t}}')">${{t}}</button>`).join('');
  }}
  modal.hidden = false;
  document.body.style.overflow = 'hidden';
  pickChart(chCur || [...chFavs].find(l => CHARTS[l]) || Object.keys(CHARTS)[0]);
}}

function closeCharts(){{
  if (chartFullOpen()) toggleChartFull(false);   // เปิดครั้งหน้าจะได้ไม่ค้างสภาพขยาย
  document.getElementById('cmodal').hidden = true;
  document.getElementById('finmodal').hidden = true;
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
  setSymFull('cmodal-full', label);
  // สินทรัพย์นอกแถบราคาไม่มีราคาสด ต้องล้างของตัวก่อนหน้าออก
  const c = document.getElementById('cmodal-c');
  document.getElementById('cmodal-p').textContent = d ? d.price : '';
  c.textContent = d ? d.pct : '';
  c.className = d ? d.dir : '';
  document.getElementById('cnews-list').innerHTML = (d?.news || []).length
    ? d.news.map(n => `<a class="cnews-row" href="${{esc(n.link)}}" target="_blank" rel="noopener">
        <span class="score ${{scoreClass(n.score)}}">${{n.score}}%</span>
        <span class="cnews-t">${{esc(n.title)}}
          <span class="cnews-m">${{esc(n.source)}} · ${{esc(n.age)}}</span></span></a>`).join('')
    : '<p class="tmodal-empty">No related stories</p>';
  if (!chCache[label]) {{
    document.getElementById('cchart').innerHTML = '<div class="cempty">Loading…</div>';
    try {{
      chCache[label] = await fetch('{CHART_DIR}/' + CHARTS[label].s + '.json').then(r => r.json());
    }} catch (e) {{ chCache[label] = {{tf: {{}}}}; }}
  }}
  if (chCur !== label) return;        // ผู้ใช้กดตัวอื่นระหว่างรอไฟล์กราฟ
  chData = chCache[label];
  // สินทรัพย์บางตัว (เช่นทองไทย) ต้องบอกที่มาของตัวเลขไว้ด้วย
  const note = document.getElementById('cnote');
  note.textContent = chData.note || '';
  note.hidden = !chData.note;
  // ปุ่มนี้โชว์ได้สองเหตุผล: มีงบการเงิน (.f) หรือมีแต่ปันผลจริง (.d — เช่น ETF ที่ไม่ยื่นงบ
  // แบบบริษัทแต่จ่ายปันผลจริง อย่าง JEPQ) ป้ายข้อความจึงต้องเปลี่ยนตามว่ามีอะไรให้ดูจริง
  const hasFin = !!CHARTS[label].f, hasDiv = !!CHARTS[label].d;
  document.getElementById('fin-btn').hidden = !hasFin && !hasDiv;
  document.getElementById('fin-btn-lbl').textContent = hasFin ? 'FINANCIALS' : 'DIVIDENDS';
  renderFullBar();               // เปลี่ยนหุ้นแล้วสถิติ 10 ปีต้องเปลี่ยนตาม
  renderChart();
}}

// ── ฉบับเต็มรายหมวดงบ (กดที่การ์ดกราฟในหน้างบการเงิน) ──────
// ชื่อ/หน่วย/รูปแบบตัวเลขของแต่ละหมวด ใช้ร่วมกันทั้งการ์ดเล็กและหน้าฉบับเต็ม
const FIN_METRIC_META = {{
  rev:    {{t: 'Revenue',            th: 'รายได้',                u: 'reported currency'}},
  ni:     {{t: 'Net Income',         th: 'กำไรสุทธิ',              u: 'reported currency'}},
  nm:     {{t: 'Net Margin',         th: 'อัตรากำไรสุทธิ',          u: 'percent of revenue', f: v => v.toFixed(1) + '%'}},
  roa:    {{t: 'Return on Assets',   th: 'ผลตอบแทนต่อสินทรัพย์',    u: 'percent return',     f: v => v.toFixed(1) + '%'}},
  assets: {{t: 'Total Assets',       th: 'สินทรัพย์รวม',            u: 'reported currency'}},
  liab:   {{t: 'Total Liabilities',  th: 'หนี้สินรวม',              u: 'reported currency'}},
  equity: {{t: 'Total Equity',       th: 'ส่วนของผู้ถือหุ้น',        u: 'reported currency'}},
  cash:   {{t: 'Cash & Equivalents', th: 'เงินสดและรายการเทียบเท่า', u: 'reported currency'}},
  de:     {{t: 'Debt / Equity',      th: 'หนี้สินต่อทุน',           u: 'times equity',       f: v => v.toFixed(2) + 'x'}},
  roe:    {{t: 'Return on Equity',   th: 'ผลตอบแทนต่อส่วนผู้ถือหุ้น', u: 'percent return',    f: v => v.toFixed(1) + '%'}},
  debt:   {{t: 'Total Debt',         th: 'หนี้สินรวมที่มีดอกเบี้ย',   u: 'reported currency'}},
  gp:     {{t: 'Gross Profit',       th: 'กำไรขั้นต้น',             u: 'reported currency'}},
  opinc:  {{t: 'Operating Income',   th: 'กำไรจากการดำเนินงาน',      u: 'reported currency'}},
  eps:    {{t: 'Diluted EPS',        th: 'กำไรต่อหุ้นปรับลด',        u: 'per share',          f: v => (v < 0 ? '-' : '') + '$' + Math.abs(v).toFixed(2)}},
}};
// ลำดับของลูกศร ‹ › ในหน้าฉบับเต็ม — ไล่ตามที่การ์ดเรียงอยู่บนแดชบอร์ด
const FIN_METRIC_ORDER = ['rev', 'ni', 'nm', 'roa', 'assets', 'liab',
                          'equity', 'cash', 'de', 'roe', 'debt'];
const MET_T = {{
  th: {{annual: 'รายปี', quarterly: 'รายไตรมาส', latest: 'งวดล่าสุด', cagr: 'อัตราโตทบต้น',
        high: 'สูงสุด', low: 'ต่ำสุด', avg: 'ค่าเฉลี่ย', period: 'งวด', value: 'ค่า',
        chg: 'เปลี่ยนแปลง', noQ: 'ยังไม่มีงบรายไตรมาสของหมวดนี้',
        allA: n => `ครบทุกงวดที่มี · ${{n}} ปี`, allQ: n => `ครบทุกงวดที่มี · ${{n}} ไตรมาส`,
        note: 'หน้านี้รวมทุกงวดที่แหล่งข้อมูลมีให้ ไม่ได้ตัดให้เหลือเฉพาะปีปัจจุบันเหมือนหน้าสรุป — ' +
              'งบการเงินย้อนได้ 5 ปี ส่วนราคาย้อนได้ 10 ปี (ดูได้ที่ปุ่ม FULL VIEW ในหน้ากราฟ)'}},
  en: {{annual: 'Annual', quarterly: 'Quarterly', latest: 'Latest', cagr: 'CAGR',
        high: 'High', low: 'Low', avg: 'Average', period: 'Period', value: 'Value',
        chg: 'Change', noQ: 'No quarterly figures reported for this line.',
        allA: n => `every period on record · ${{n}} years`, allQ: n => `every period on record · ${{n}} quarters`,
        note: 'This page carries every period the source holds, rather than the current year only ' +
              'as the summary does — statements reach back 5 years, prices 10 (see FULL VIEW on the chart).'}},
}};

let metKey = null;
function metMeta(k){{ return FIN_METRIC_META[k] || {{t: k, th: k, u: ''}}; }}

function openMetric(key){{
  if (!FIN_METRIC_META[key] || !finCache[chCur]) return;
  metKey = key;
  document.getElementById('metmodal').hidden = false;
  document.body.style.overflow = 'hidden';
  renderMetric();
}}
function closeMetric(){{
  document.getElementById('metmodal').hidden = true;
  if (document.getElementById('finmodal').hidden &&
      document.getElementById('cmodal').hidden) document.body.style.overflow = '';
}}
function metNav(step){{
  const avail = FIN_METRIC_ORDER.filter(k => {{
    const rows = withRatios(finCache[chCur]?.annual || []);
    return rows.some(r => r[k] != null && isFinite(r[k]));
  }});
  if (avail.length < 2) return;
  const i = avail.indexOf(metKey);
  metKey = avail[((i < 0 ? 0 : i) + step + avail.length) % avail.length];
  renderMetric();
  document.getElementById('met-body').scrollTop = 0;
}}

function renderMetric(){{
  const data = finCache[chCur];
  const body = document.getElementById('met-body');
  if (!data || !metKey) {{ body.innerHTML = ''; return; }}
  const L = MET_T[finReadLang] || MET_T.en;
  const m = metMeta(metKey);
  const fmt = m.f || finFmt;
  const title = finReadLang === 'th' ? m.th : m.t;
  document.getElementById('met-name').textContent = title;
  document.getElementById('met-sub').textContent =
    `${{symFull(chCur) ? chCur + ' · ' + symFull(chCur) : chCur}} · ${{m.u}}`;

  const annual = withRatios(data.annual || []);
  const quarterly = withRatios(data.quarterly || []);
  const vals = annual.map(r => r[metKey]).filter(v => v != null && isFinite(v));
  const cmpRows = finCompareRows();

  // สรุปสถิติจากงวดรายปีทั้งหมดที่มี
  let stats = '';
  if (vals.length) {{
    const last = vals[vals.length - 1], firstV = vals[0];
    const hi = Math.max(...vals), lo = Math.min(...vals);
    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
    const yrs = vals.length - 1;
    // CAGR ใช้ได้เฉพาะค่าบวกทั้งหัวและท้าย ไม่งั้นไม่มีนิยาม
    const cagr = (yrs > 0 && firstV > 0 && last > 0)
      ? (Math.pow(last / firstV, 1 / yrs) - 1) * 100 : null;
    const cell = (lbl, val, cls, sub) =>
      `<div class="met-stat"><span>${{esc(lbl)}}</span>` +
      `<b${{cls ? ` class="${{cls}}"` : ''}}>${{esc(val)}}</b>` +
      (sub ? `<i>${{esc(sub)}}</i>` : '') + `</div>`;
    stats = `<div class="met-stats">` +
      cell(L.latest, fmt(last), '', periodLabel(annual[annual.length - 1].date, 'annual')) +
      (cagr != null ? cell(L.cagr, (cagr >= 0 ? '+' : '') + cagr.toFixed(1) + '%',
        cagr >= 0 ? 'up' : 'down', `${{yrs}}Y`) : '') +
      cell(L.high, fmt(hi)) + cell(L.low, fmt(lo)) + cell(L.avg, fmt(avg)) +
      `</div>`;
  }}

  // กราฟ + ตาราง ของทั้งรายปีและรายไตรมาส (ไตรมาสเอาครบทุกงวดที่มี ไม่ตัดเหลือปีปัจจุบัน)
  const block = (rows, span, heading) => {{
    const have = rows.filter(r => r[metKey] != null && isFinite(r[metKey]));
    const head = `<div class="met-sub-h"><b>${{esc(heading)}}</b>` +
      `<span>${{esc(span === 'annual' ? L.allA(rows.length) : L.allQ(rows.length))}}</span></div>`;
    if (!have.length) return head + `<p class="met-empty">${{esc(L.noQ)}}</p>`;
    const periods = rows.map(r => periodLabel(r.date, span));
    const prevSpan = finSpan;
    finSpan = span;                       // ป้ายงวด/หน่วยในกราฟอิงค่านี้
    const aligned = cmpRows ? finAlign(periods, cmpRows) : null;
    const chart = divergingBarChart(title, periods, rows.map(r => r[metKey]),
      {{fmt: fmt, unit: m.u, cmp: aligned ? aligned.map(r => r && r[metKey]) : null}});
    finSpan = prevSpan;
    const tbl = `<table class="met-tbl"><thead><tr><th>${{esc(L.period)}}</th>` +
      `<th>${{esc(L.value)}}</th><th>${{esc(L.chg)}}</th></tr></thead><tbody>` +
      rows.map((r, i) => {{
        const v = r[metKey];
        const txt = (v == null || !isFinite(v)) ? null : fmt(v);
        const d = i > 0 ? finDelta(metKey, v, rows[i - 1][metKey]) : '';
        return `<tr><td>${{esc(periods[i])}}</td>` +
          `<td>${{txt == null ? '<span class="met-na">—</span>' : esc(txt)}}</td>` +
          `<td>${{d || '<span class="met-na">—</span>'}}</td></tr>`;
      }}).join('') + `</tbody></table>`;
    return head + chart + tbl;
  }};

  body.innerHTML = `<div class="met-inner">${{stats}}` +
    `<p class="met-note">${{esc(L.note)}}</p>` +
    block(annual, 'annual', L.annual) +
    block(quarterly, 'quarterly', L.quarterly) +
    `</div>`;
}}
document.getElementById('metmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'metmodal') closeMetric();
}});

// ── งบการเงิน (เปิดจากปุ่ม FINANCIALS ในหน้ากราฟ) ─────────
// รายการฟิลด์ต้องตรงกับ FIN_FIELDS ฝั่ง build.py — คีย์สั้นในไฟล์ JSON เหมือนกัน
const FIN_FIELDS = [
  ['rev',    'Revenue',              'income'],
  ['cogs',   'Cost of Revenue',      'income'],
  ['gp',     'Gross Profit',         'income'],
  ['opex',   'Operating Expense',    'income'],
  ['opinc',  'Operating Income',     'income'],
  ['ni',     'Net Income',           'income'],
  ['eps',    'Diluted EPS',          'income'],
  ['assets', 'Total Assets',         'balance'],
  ['liab',   'Total Liabilities',    'balance'],
  ['equity', 'Total Equity',         'balance'],
  ['cash',   'Cash & Equivalents',   'balance'],
  ['debt',   'Total Debt',           'balance'],
  // อัตราส่วนคำนวณฝั่งไคลเอนต์เอง ไม่ได้ยิงขอ Yahoo เพิ่ม — มาจาก 12 ฟิลด์ด้านบนล้วนๆ
  ['gm',     'Gross Margin',         'ratios'],
  ['om',     'Operating Margin',     'ratios'],
  ['nm',     'Net Margin',           'ratios'],
  ['roe',    'Return on Equity',     'ratios'],
  ['roa',    'Return on Assets',     'ratios'],
  ['de',     'Debt / Equity',        'ratios'],
];
const FIN_SEC = {{income: 'INCOME STATEMENT', balance: 'BALANCE SHEET', ratios: 'KEY RATIOS'}};
const FIN_PCT_KEYS = new Set(['gm', 'om', 'nm', 'roe', 'roa']);
let finCache = {{}}, finSpan = 'annual', finSortCol = -1, finSortDir = -1, finCompareSym = null;

// โหลดงบการเงินของตัวเดียว — ใช้ร่วมกันทั้งตอนกดเปิดจริงและตอนโหลดล่วงหน้าให้รายการโปรด
// เก็บตัว promise ไว้ด้วย ไม่ใช่แค่ผลลัพธ์ ถ้าผู้ใช้กดเข้าไปพอดีตอนโหลดล่วงหน้ายังค้างอยู่
// จะได้เกาะสายเดิม ไม่ยิงขอไฟล์เดิมซ้ำสองรอบ
const finInflight = {{}};
const FIN_NONE = () => ({{annual: [], quarterly: []}});
function loadFin(label){{
  if (finCache[label]) return Promise.resolve(finCache[label]);
  if (finInflight[label]) return finInflight[label];
  // ไม่มีงบการเงิน (.f) ไม่ต้องยิงขอไฟล์เลย — รู้อยู่แล้วว่าไม่มี (เช่น ETF อย่าง JEPQ)
  if (!CHARTS[label]?.f) {{ finCache[label] = FIN_NONE(); return Promise.resolve(finCache[label]); }}
  const p = fetch('{FIN_DIR}/' + CHARTS[label].s + '.json')
    .then(r => {{ if (!r.ok) throw new Error('http ' + r.status); return r.json(); }})
    .then(d => {{ finCache[label] = d; delete finInflight[label]; return d; }})
    // เน็ตสะดุดตอนโหลดล่วงหน้าห้ามจำว่า "ไม่มีงบ" ถาวร — ไม่ cache ไว้ พอกดเข้าไปจริงจะได้ลองใหม่
    .catch(() => {{ delete finInflight[label]; return FIN_NONE(); }});
  finInflight[label] = p;
  return p;
}}

// โหมดประหยัดเน็ต/เน็ตช้า ไม่ต้องโหลดของ "เผื่อกด" ให้เปลืองแทน
const netIsFrugal = () => {{
  const c = navigator.connection;
  return !!(c && (c.saveData || /(^|-)2g$/.test(c.effectiveType || '')));
}};

// ดึงงบของรายการโปรดมาพักไว้ตอนเครื่องว่าง กด FINANCIALS แล้วขึ้นทันทีไม่ต้องรอโหลด
// ไฟล์ละ ~3 KB ทั้งชุดรายการโปรดยังไม่ถึง 100 KB — เดินทีละ 3 สาย ไม่แย่งเน็ตกับข่าว/กราฟ
function prefetchFavFin(){{
  if (netIsFrugal()) return;
  const queue = [...chFavs].filter(l => CHARTS[l]?.f && !finCache[l] && !finInflight[l]);
  const pump = () => {{ if (queue.length) loadFin(queue.shift()).then(pump); }};
  for (let k = 0; k < 3; k++) pump();
}}

const finFmt = v => {{
  if (v == null || !isFinite(v)) return null;
  const a = Math.abs(v), sign = v < 0 ? '-' : '';
  if (a >= 1e12) return sign + (a / 1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return sign + (a / 1e9).toFixed(2) + 'B';
  if (a >= 1e6)  return sign + (a / 1e6).toFixed(1) + 'M';
  if (a >= 1e3)  return sign + (a / 1e3).toFixed(0) + 'K';
  return sign + a.toFixed(0);
}};
// ตัวเลือกฟอร์แมตต่อฟิลด์ — เงิน / EPS ($) / มาร์จิ้น-ผลตอบแทน (%) / หนี้สินต่อทุน (x)
function finCellText(key, v){{
  if (v == null || !isFinite(v)) return null;
  if (key === 'eps') return (v < 0 ? '-' : '') + '$' + Math.abs(v).toFixed(2);
  if (FIN_PCT_KEYS.has(key)) return v.toFixed(1) + '%';
  if (key === 'de') return v.toFixed(2) + 'x';
  return finFmt(v);
}}
// ฟิลด์ที่เป็น % อยู่แล้วต้องโชว์ผลต่างเป็นจุดเปอร์เซ็นต์ (pp) ไม่ใช่ % เปลี่ยนแปลง —
// ไม่งั้นมาร์จิ้นขยับ 20%→22% จะโชว์ "+10%" ทำให้เข้าใจผิดว่ากำไรพุ่งแรงกว่าที่เป็นจริง
function finDelta(key, v, prev){{
  if (v == null || prev == null || !isFinite(v) || !isFinite(prev)) return '';
  if (FIN_PCT_KEYS.has(key)) {{
    const d = v - prev;
    const dir = d > 0.05 ? 'up' : d < -0.05 ? 'down' : 'flat';
    const dtxt = dir === 'flat' ? '0.0' : (d >= 0 ? '+' : '') + d.toFixed(1);
    return `<span class="fin-delta ${{dir}}">${{dtxt}}pp</span>`;
  }}
  if (prev === 0) return '';
  const pct = (v / prev - 1) * 100;
  const dir = pct > 0.5 ? 'up' : pct < -0.5 ? 'down' : 'flat';
  return `<span class="fin-delta ${{dir}}">${{pct >= 0 ? '+' : ''}}${{pct.toFixed(1)}}%</span>`;
}}
// เติมอัตราส่วนจากฟิลด์ดิบ — คืนแถวใหม่ ไม่แก้ของเดิมใน finCache
function withRatios(rows){{
  return rows.map(r => ({{
    ...r,
    gm:  (r.gp != null && r.rev)    ? r.gp / r.rev * 100    : null,
    om:  (r.opinc != null && r.rev) ? r.opinc / r.rev * 100 : null,
    nm:  (r.ni != null && r.rev)    ? r.ni / r.rev * 100     : null,
    de:  (r.debt != null && r.equity) ? r.debt / r.equity   : null,
    roe: (r.ni != null && r.equity) ? r.ni / r.equity * 100  : null,
    roa: (r.ni != null && r.assets) ? r.ni / r.assets * 100  : null,
  }}));
}}

function periodLabel(date, span){{
  const d = new Date(date);
  if (span === 'annual') return 'FY ' + d.getUTCFullYear();
  return 'Q' + (Math.floor(d.getUTCMonth() / 3) + 1) + ' ' + d.getUTCFullYear();
}}

// เอาเฉพาะไตรมาสของปีที่ดำเนินอยู่ ถ้าปีนี้ยังไม่มีงบออกเลยค่อยโชว์ 4 ไตรมาสล่าสุดแทน
function currentYearQuarters(rows){{
  const y = new Date().getUTCFullYear();
  const thisYear = rows.filter(r => new Date(r.date).getUTCFullYear() === y);
  // ปีนี้ยังไม่มีงบออกเลย (เช่นเพิ่งขึ้นปีใหม่) โชว์ 4 ไตรมาสล่าสุดที่มีแทน ดีกว่าโชว์ตารางว่าง
  return thisYear.length ? thisYear : rows.slice(-4);
}}

// สีชุดกราฟ — คงที่ตายตัว ไม่ไล่สีใหม่ตามจำนวนเส้นที่เหลือ
// หุ้นหลัก = brass, หุ้นที่เอามาเทียบ = econ (น้ำเงิน), มาร์จิ้น = teal/purple/amber
// (ตรวจด้วยสูตร OKLab ΔE + จำลองตาบอดสี protan/deutan แล้ว: คู่แย่สุดของเส้นมาร์จิ้น
//  ΔE 16.9 และ brass↔econ ΔE 26.6 ผ่านเกณฑ์ทั้งหมด — คู่ purple↔blue ΔE 1.7 ใช้ร่วมกันไม่ได้
//  จึงไม่เอาเส้นสีน้ำเงินไปไว้ในกราฟมาร์จิ้น)
const FIN_BAR_CMP = 'var(--econ)';
const FIN_LINE_COLORS = ['var(--biz)', 'var(--mixed)', 'var(--poli)'];

// กราฟแท่งแบบมีเส้นศูนย์ — แท่งชี้ลงได้ถ้าค่าติดลบ (เช่นปีขาดทุน) ไม่ปัดให้เป็น 0 ที่ทำให้เข้าใจผิด
// opt: {{fmt, cmp, stat}} — cmp คือค่าหุ้นเทียบที่จับคู่งวดมาแล้ว วางบนแกน/สเกลเดียวกัน
function divergingBarChart(title, periods, values, opt){{
  opt = opt || {{}};
  const fmt = opt.fmt || finFmt;
  const cmp = opt.cmp || null;
  if (!values.some(v => v != null && isFinite(v))) return '';
  const pool = values.concat(cmp || []).filter(v => v != null && isFinite(v));
  const max = Math.max(0, ...pool), min = Math.min(0, ...pool);
  const range = (max - min) || Math.abs(max) || 1;
  const zeroPct = (max / range) * 100;
  // ตัวเลขวางในแท่งเมื่อแท่งสูงพอ (ไม่งั้นตัวหนังสือจะล้นออกนอกแท่ง) — ~15% ของราง 150px
  // คือราว 22px พอดีกับตัวอักษร .86rem หนึ่งบรรทัด แท่งเตี้ยกว่านั้นเอาเลขไปไว้เหนือแท่ง
  const INSIDE_MIN = 15;
  const slot = (v, cls, who, period, label) => {{
    if (v == null || !isFinite(v)) return '<div class="fin-bar-slot"></div>';
    const pct = Math.abs(v) / range * 100, neg = v < 0;
    const top = neg ? zeroPct : zeroPct - pct;
    const klass = cls === 'cmp' ? 'cmp' : (neg ? 'neg' : 'pos');
    const num = label
      ? `<span class="fin-bar-num ${{pct >= INSIDE_MIN ? 'in' : 'out'}}${{neg ? ' dn' : ''}}">` +
        `${{esc(fmt(v))}}</span>`
      : '';
    return `<div class="fin-bar-slot"><div class="fin-bar ${{klass}}${{neg ? ' dn' : ''}}" ` +
      `style="top:${{top.toFixed(2)}}%;height:${{pct.toFixed(2)}}%" ` +
      `title="${{esc(who)}} · ${{esc(period)}}: ${{esc(fmt(v))}}">${{num}}</div></div>`;
  }};
  const cols = periods.map((p, i) => {{
    const v = values[i];
    // เทียบอยู่ = ช่องแคบลงครึ่งหนึ่ง (โชว์เลขไม่ได้) และ opt.noLabel = การ์ดที่ตั้งใจไม่ให้
    // มีเลขบนแท่งเลย (เช่นกราฟปันผลย้อน 10 ปี ที่ 10 ช่องไม่มีทางพอสำหรับเลขที่อ่านออก
    // ในการ์ดกว้างเท่าจอมือถือ) — ทั้งสองกรณีดูค่าจริงได้จาก tooltip กับตารางข้างล่างแทน
    const bars = slot(v, 'main', chCur, p, !cmp && !opt.noLabel) +
      (cmp ? slot(cmp[i], 'cmp', finCompareSym, p, false) : '');
    const na = (v == null || !isFinite(v)) ? '<div class="fin-bar-na">—</div>' : '';
    return `<div class="fin-bar-col">` +
      `<div class="fin-bar-track">${{bars}}${{na}}</div>` +
      `<div class="fin-bar-lbl">${{esc(p)}}</div></div>`;
  }}).join('');
  // ไม่ใส่ legend ซ้ำทุกการ์ด — สีคู่นี้ใช้เหมือนกันทั้งแดชบอร์ด มี legend รวมอยู่ใต้แถบเครื่องมือ
  // (แท่งทุกแท่งมี tooltip บอกชื่อหุ้นกำกับอยู่แล้ว ตัวตนจึงไม่ได้อยู่ที่สีอย่างเดียว)
  const stat = opt.stat ? `<span class="fin-chart-stat">${{esc(opt.stat)}}</span>` : '';
  const unit = opt.unit || 'reported currency';
  // opt.metric = เปิดฉบับเต็มของหมวดนี้ได้ (ไม่ส่งมา = การ์ดในหน้าฉบับเต็มเอง กดซ้อนไม่ได้)
  const tap = opt.metric ? ` tap" role="button" tabindex="0" data-metric="${{esc(opt.metric)}}"` +
    ` onclick="openMetric('${{esc(opt.metric)}}')"` +
    ` onkeydown="if(event.key==='Enter'||event.key===' '){{event.preventDefault();openMetric('${{esc(opt.metric)}}')}}"`
    : '"';
  const more = opt.metric
    ? `<span class="fin-chart-more">${{finReadLang === 'th' ? 'ดูฉบับเต็ม' : 'Full detail'}} ›</span>` : '';
  return `<div class="fin-chart${{tap}}>
      <div class="fin-chart-h"><span><span class="fin-chart-t">${{esc(title)}}</span>` +
        `<span class="fin-chart-u">${{esc(unit)}} · per ${{finSpan === 'annual' ? 'fiscal year' : 'quarter'}}</span></span>${{stat}}</div>
      <div class="fin-chart-plot"><div class="fin-zero-line" style="top:${{zeroPct}}%"></div>${{cols}}</div>
      ${{more}}
    </div>`;
}}

// เส้นแนวโน้มมาร์จิ้น % — เส้นตาราง 3 ระดับ + ป้ายงวดเป็น HTML ใต้กราฟ
function marginLineChart(title, periods, series){{
  const n = periods.length;
  if (n < 2) return '';
  const allVals = [];
  series.forEach(s => s.vals.forEach(v => {{ if (v != null && isFinite(v)) allVals.push(v); }}));
  if (!allVals.length) return '';
  let max = Math.max(...allVals), min = Math.min(...allVals);
  if (max === min) {{ max += 1; min -= 1; }}
  const pad = (max - min) * 0.14;
  max += pad; min -= pad;
  const W = 640, H = 170, padL = 6, padR = 6, padT = 10, padB = 10;
  const x = i => padL + (i / (n - 1)) * (W - padL - padR);
  const y = v => padT + (1 - (v - min) / (max - min)) * (H - padT - padB);

  let svg = `<svg class="fin-line-svg" viewBox="0 0 ${{W}} ${{H}}" preserveAspectRatio="none" ` +
    `role="img" aria-label="${{esc(title)}}">`;
  for (const t of [max, (max + min) / 2, min]) {{
    svg += `<line class="fin-grid-ln" x1="${{padL}}" x2="${{W - padR}}" ` +
      `y1="${{y(t).toFixed(1)}}" y2="${{y(t).toFixed(1)}}"/>`;
  }}
  if (min <= 0 && max >= 0) {{
    svg += `<line class="fin-zero-ln" x1="${{padL}}" x2="${{W - padR}}" ` +
      `y1="${{y(0).toFixed(1)}}" y2="${{y(0).toFixed(1)}}"/>`;
  }}
  // สีอิงตำแหน่งเดิมในชุดเสมอ (ไม่ไล่เลขใหม่) ให้ธุรกิจแบงก์ที่ไม่มี Gross/Operating
  // ยังโชว์ Net Margin เป็นสีเดิมทุกครั้ง ไม่ใช่สีของสล็อตแรกที่ว่างไป
  const legendParts = [];
  series.forEach((s, si) => {{
    const color = FIN_LINE_COLORS[si % FIN_LINE_COLORS.length];
    let d = '', started = false, any = false;
    const dots = [];
    s.vals.forEach((v, i) => {{
      if (v == null || !isFinite(v)) {{ started = false; return; }}
      any = true;
      const px = x(i), py = y(v);
      d += (started ? 'L' : 'M') + px.toFixed(1) + ' ' + py.toFixed(1) + ' ';
      started = true;
      dots.push(`<circle class="fin-line-dot" cx="${{px.toFixed(1)}}" cy="${{py.toFixed(1)}}" r="3.5" ` +
        `fill="${{color}}"><title>${{esc(periods[i])}} · ${{esc(s.name)}}: ${{v.toFixed(1)}}%</title></circle>`);
    }});
    if (!any) return;                        // ไม่มีข้อมูลเลย (เช่นแบงก์ไม่มี Gross Margin) ข้ามทั้งเส้นและ legend
    if (d) svg += `<path class="fin-line-path" d="${{d.trim()}}" stroke="${{color}}" fill="none"/>`;
    svg += dots.join('');
    legendParts.push(`<span class="fin-legend-item"><i style="background:${{color}}"></i>${{esc(s.name)}}</span>`);
  }});
  svg += '</svg>';
  const xaxis = `<div class="fin-xaxis">${{periods.map(p => `<span>${{esc(p)}}</span>`).join('')}}</div>`;
  const range = `<span class="fin-chart-stat">${{min.toFixed(0)}}% – ${{max.toFixed(0)}}%</span>`;
  return `<div class="fin-chart fin-chart-wide">
      <div class="fin-chart-h"><span><span class="fin-chart-t">${{esc(title)}}</span>` +
        `<span class="fin-chart-u">percent of revenue · per ${{finSpan === 'annual' ? 'fiscal year' : 'quarter'}}</span></span>${{range}}</div>
      <div class="fin-legend">${{legendParts.join('')}}</div>
      ${{svg}}${{xaxis}}
    </div>`;
}}

// ── ข้อมูลหุ้นที่เอามาเทียบ ─────────────────────────────────
function finCompareRows(){{
  if (!finCompareSym || !finCache[finCompareSym]) return null;
  let cRows = finCache[finCompareSym][finSpan] || [];
  if (finSpan === 'quarterly') cRows = currentYearQuarters(cRows);
  return cRows.length ? withRatios(cRows) : null;
}}
// จับคู่งวดตามป้ายงวด (FY 2024 ↔ FY 2024) ไม่ใช่ตามลำดับ — บริษัทที่ปิดรอบบัญชีคนละเดือน
// หรือมีจำนวนงวดไม่เท่ากัน จะได้ไม่ถูกเอาไปเทียบผิดงวดโดยไม่รู้ตัว
function finAlign(periods, cRows){{
  if (!cRows) return null;
  const byLabel = {{}};
  cRows.forEach(r => {{ byLabel[periodLabel(r.date, finSpan)] = r; }});
  return periods.map(p => byLabel[p] || null);
}}

// CAGR คิดได้เฉพาะข้อมูลรายปี และต้องเป็นบวกทั้งงวดต้นและงวดปลาย —
// ถ้าปีใดขาดทุน (ติดลบ) อัตราการเติบโตทบต้นไม่มีนิยามทางคณิตศาสตร์ จึงไม่โชว์ ดีกว่าโชว์เลขมั่ว
function cagrText(rows, key){{
  if (finSpan !== 'annual') return '';
  const vals = rows.map(r => r[key]);
  const i = vals.findIndex(v => v != null && isFinite(v));
  let j = -1;
  for (let k = vals.length - 1; k >= 0; k--) {{
    if (vals[k] != null && isFinite(vals[k])) {{ j = k; break; }}
  }}
  if (i < 0 || j <= i) return '';
  const a = vals[i], b = vals[j];
  if (!(a > 0 && b > 0)) return '';
  const yrs = j - i;
  const g = (Math.pow(b / a, 1 / yrs) - 1) * 100;
  return `${{yrs}}Y CAGR ${{g >= 0 ? '+' : ''}}${{g.toFixed(1)}}%`;
}}

// ── แถบเครื่องมือแดชบอร์ด (ปุ่มกระโดดหัวข้อ + เลือกหุ้นเทียบ) ──
// ── ปันผล — คำนวณจากประวัติการจ่ายจริงที่ผูกมากับไฟล์กราฟ (chData.div) ──────
// ทุกตัวเลขในหมวดนี้มาจากเงินปันผลที่บริษัทประกาศจริงเท่านั้น ไม่มีการประมาณ/พยากรณ์อนาคต
function divRows(){{ return (chData && chData.div) || []; }}

// ผลรวมเงินปันผลย้อนหลัง 12 เดือนนับจาก asOf (epoch วินาที) — ใช้ทำ "อัตราผลตอบแทนปัจจุบัน"
function divTTM(rows, asOf){{
  const cutoff = asOf - 365 * 86400;
  const recent = rows.filter(d => d.date > cutoff && d.date <= asOf);
  return {{sum: recent.reduce((a, d) => a + d.amount, 0), n: recent.length}};
}}

// ราคาปิดของแท่งล่าสุดที่ "ไม่เกิน" epoch ที่ให้มา — ใช้หาราคา ณ ต้นปีนั้นๆ
function priceAt(daily, epoch){{
  if (!daily || !daily.length) return null;
  let best = null;
  for (const b of daily) {{ if (b[0] <= epoch) best = b; else break; }}
  return (best || daily[0])[4];
}}

function divCurrency(){{ return (CHARTS[chCur] || {{}}).cur || 'USD'; }}
// ป้ายตัวเลขบนแท่งกราฟปันผลมีถึง 10 แท่ง (10 ปี) ช่องแคบกว่าตารางมาก — ใช้เวอร์ชันสั้น
// 2 ตำแหน่งทศนิยมแทน 4 เฉพาะตอนวาดเป็นป้ายบนแท่ง ตัวเลขเต็มยังอยู่ครบในตารางข้างล่าง
function divFmtShort(v){{
  if (v == null || !isFinite(v)) return '—';
  let s = d3.format(Math.abs(v) >= 1000 ? ',.0f' : ',.2f')(v);
  if (s.includes('.')) s = s.replace(/0+$/, '').replace(/\\.$/, '');
  return s;
}}
// เงินปันผล/ราคาต่อหุ้นเป็นเลขทศนิยมเล็กๆ (0.27, 12.0, 1.4 บาท ฯลฯ) — finFmt ปัดเป็น
// K/M/B ซึ่งไม่เหมาะ จึงใช้ทศนิยมตรงๆ แทน ตัดศูนย์ท้ายที่ไม่มีความหมายทิ้ง
function divFmt(v){{
  if (v == null || !isFinite(v)) return '—';
  let s = d3.format(Math.abs(v) >= 1000 ? ',.0f' : ',.4f')(v);
  if (s.includes('.')) s = s.replace(/0+$/, '').replace(/\\.$/, '');
  return s;
}}

// ── ช่องกรอกจำนวนเงิน ────────────────────────────────────
// input type=number ใส่ , ไม่ได้ (เบราว์เซอร์ถือว่าค่าไม่ถูกต้องแล้วคืนค่าว่าง) แต่งบก้อนโต
// อย่าง 500000 อ่านไม่ออกเลยถ้าไม่มีตัวคั่น จึงเปลี่ยนเป็น type=text แล้วจัดรูปแบบเอง
const groupDigits = s => {{
  const [i, ...rest] = s.split('.');
  const gi = i.replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ',');
  return rest.length ? gi + '.' + rest.join('') : gi;
}};
const moneyVal = el => parseFloat((el.value || '').replace(/,/g, ''));
// จัดรูปแบบระหว่างพิมพ์ แล้วเลื่อนเคอร์เซอร์ตามจำนวนหลัก (ไม่ใช่ตามตำแหน่งตัวอักษร)
// ไม่งั้นพอ , โผล่เพิ่มกลางตัวเลข เคอร์เซอร์จะเด้งไปท้ายช่องทุกครั้งที่พิมพ์
function fmtMoneyInput(el){{
  const pos = el.selectionStart;
  const digitsBefore = (el.value.slice(0, pos).match(/[\\d.]/g) || []).length;
  let clean = el.value.replace(/[^\\d.]/g, '');
  const dot = clean.indexOf('.');            // เก็บจุดทศนิยมได้จุดเดียว
  if (dot >= 0) clean = clean.slice(0, dot + 1) + clean.slice(dot + 1).replace(/\\./g, '');
  const out = groupDigits(clean);
  el.value = out;
  let seen = 0, np = digitsBefore ? out.length : 0;
  for (let k = 0; k < out.length && digitsBefore; k++) {{
    if (/[\\d.]/.test(out[k]) && ++seen === digitsBefore) {{ np = k + 1; break; }}
  }}
  try {{ el.setSelectionRange(np, np); }} catch(e) {{}}
}}

// เรตแลกเงินเอาจากคู่ USD/THB ที่แถบราคาของเว็บดึงสดอยู่แล้ว ไม่ได้ฮาร์ดโค้ดหรือเดาเอง
// ถ้าดึงเรตไม่ได้ให้คืน null แล้วไม่ต้องโชว์ยอดบาทเลย ดีกว่าโชว์ตัวเลขที่ไม่รู้ที่มา
function usdThbRate(){{
  const v = parseFloat((TNEWS['USD/THB'] || {{}}).price);
  return isFinite(v) && v > 0 ? v : null;
}}
// ยอดบาทเทียบเท่าแบบเล็กๆ ต่อท้ายตัวเลขสกุล USD — หุ้นไทยเป็นบาทอยู่แล้วไม่ต้องแปลงซ้ำ
function thbEq(v, cur){{
  if (cur !== 'USD' || v == null || !isFinite(v)) return '';
  const r = usdThbRate();
  return r ? `<span class="thb-eq">≈ ${{divFmtShort(v * r)}} THB</span>` : '';
}}
// เวอร์ชันข้อความล้วน สำหรับที่ที่ทั้งก้อนโดน escape อยู่แล้ว (ใส่แท็กเข้าไปไม่ได้)
function thbEqTxt(v, cur){{
  if (cur !== 'USD' || v == null || !isFinite(v)) return '';
  const r = usdThbRate();
  return r ? ` · ≈ ${{divFmtShort(v * r)}} THB` : '';
}}

function renderDivSection(){{
  const rows = divRows();
  const cur = divCurrency();
  // ราคา "วันนี้" เอาจากซีรีส์ความละเอียดสูงสุดที่มี (1Y รายวัน) — แต่การย้อนหาราคา
  // "ต้นปีที่ N ปีก่อน" ต้องใช้ซีรีส์ 10Y เท่านั้น เพราะ 1Y มีข้อมูลแค่ปีเดียวย้อนไม่ถึง
  // (บั๊กที่เจอตอนทดสอบ: ใช้ 1Y ทำให้ทุกปีย้อนหลังโชว์ราคาเดียวกันหมด เพราะ priceAt หา
  // แท่งที่เก่ากว่า epoch ที่ขอไม่เจอในซีรีส์ 1 ปี เลยตกไปใช้แท่งแรกสุดของ 1Y ซ้ำทุกครั้ง)
  const daily = (chData && chData.tf && (chData.tf['1Y'] || chData.tf['10Y'])) || [];
  const histSeries = (chData && chData.tf &&
    (chData.tf['10Y'] || chData.tf['5Y'] || chData.tf['3Y'] || chData.tf['1Y'])) || [];
  const price = daily.length ? daily[daily.length - 1][4] : null;
  const now = Date.now() / 1000;
  const ttm = divTTM(rows, now);
  const hasDiv = rows.length > 0;
  const yieldPct = (ttm.sum > 0 && price > 0) ? ttm.sum / price * 100 : null;
  const monthlyEq = yieldPct != null ? yieldPct / 12 : null;

  // ความถี่การจ่ายบอกตรงๆ จากจำนวนงวดจริงใน 12 เดือนที่ผ่านมา — ไม่เดาจากชื่อประเภทหุ้น
  // กันไม่ให้ "อัตราต่อเดือน" ถูกเข้าใจผิดว่าบริษัทจ่ายปันผลทุกเดือนทั้งที่จริงจ่ายปีละครั้ง
  const freqTxt = ttm.n === 0 ? 'no payment in the trailing 12 months'
    : ttm.n === 1 ? '1 payment in the trailing 12 months'
    : `${{ttm.n}} payments in the trailing 12 months` +
      (ttm.n >= 11 ? ' (monthly)' : (ttm.n >= 3 && ttm.n <= 5) ? ' (quarterly)' :
       ttm.n === 2 ? ' (semi-annual)' : '');

  const kpis = !hasDiv ? '' : `<div class="fin-kpis">` +
    `<div class="fin-kpi gold-frame"><div class="fin-kpi-lbl">Annual Yield (TTM)</div>` +
      `<div class="fin-kpi-val">${{yieldPct != null ? yieldPct.toFixed(2) + '%' : '—'}}</div></div>` +
    `<div class="fin-kpi"><div class="fin-kpi-lbl">Monthly Equivalent</div>` +
      `<div class="fin-kpi-val">${{monthlyEq != null ? monthlyEq.toFixed(2) + '%' : '—'}}</div>` +
      `<div class="fin-kpi-cmp"><span>annual ÷ 12 — not a real monthly payment</span></div></div>` +
    `<div class="fin-kpi gold-frame"><div class="fin-kpi-lbl">Dividend / Share (TTM)</div>` +
      `<div class="fin-kpi-val">${{divFmt(ttm.sum)}} ${{esc(cur)}}</div>${{thbEq(ttm.sum, cur)}}` +
      `<div class="fin-kpi-cmp"><span>${{esc(freqTxt)}}</span></div></div>` +
    `<div class="fin-kpi"><div class="fin-kpi-lbl">Price / Share</div>` +
      `<div class="fin-kpi-val">${{divFmt(price)}} ${{esc(cur)}}</div>${{thbEq(price, cur)}}</div>` +
    `</div>`;

  const emptyMsg = hasDiv ? '' : `<p class="met-note">No dividend payments found in the ` +
    `10 years of price history held for ${{esc(chCur)}}. The calculator below still works — ` +
    `plug in a hypothetical dividend to see what yield it would imply.</p>`;

  // ยอดบาทเป็นของแถมไว้พออ่านออก ไม่ใช่ตัวเลขที่ใครรายงาน — ต้องบอกเรตที่ใช้และที่มาไว้ตรงๆ
  // และเรตนี้เป็นเรตวันนี้ตัวเดียว ใช้คูณย้อนอดีตในตารางด้วย ซึ่งไม่ใช่เรตของปีนั้นจริงๆ
  const rate = cur === 'USD' ? usdThbRate() : null;
  // โชว์เรตด้วยสตริงเดิมจากแถบราคา ไม่เอาไปผ่าน divFmtShort ซึ่งตัดศูนย์ท้ายทิ้ง (33.30 → 33.3)
  // เรตค่าเงินตัดทศนิยมทิ้งแล้วดูเหมือนละเอียดน้อยกว่าที่มีจริง และไม่ตรงกับที่แถบราคาโชว์
  const rateTxt = esc((TNEWS['USD/THB'] || {{}}).price || '');
  const fxNote = rate ? `<p class="met-note">THB figures are a conversion at today's ` +
    `USD/THB rate (${{rateTxt}}), taken from the live pair on this page — shown for ` +
    `reference only. Dividends are paid in USD, and past years are converted at today's rate, ` +
    `not the rate that applied back then.</p>` : '';

  // เรียง: ตัวเลข → เหตุผลถ้าไม่มีปันผล → หมายเหตุเรตแลกเงิน ตัวที่ไม่มีปันผลจะได้อ่านเจอ
  // ว่า "ไม่มีปันผล" ก่อน ไม่ใช่เจอหมายเหตุเรื่องเรตเงินบาทลอยมาก่อนทั้งที่ยังไม่มีตัวเลขให้แปลง
  return kpis + emptyMsg + fxNote +
    renderDivCalc(ttm.sum, price, cur) + renderDivHistory(rows, histSeries, cur);
}}

function renderDivCalc(dps, price, cur){{
  const d = isFinite(dps) ? dps : 0, p = isFinite(price) ? price : 0;
  return `<div class="div-calc">
      <div class="div-calc-h">Yield calculator</div>
      <p class="div-calc-note">Enter how much you'd like to invest — shares, income and yield
        fill in on their own. Price and dividend per share default to this stock's own current
        figures; change either to test a different scenario. Nothing here is saved or sent
        anywhere.</p>
      <div class="div-calc-row">
        <label class="div-calc-primary">Investment budget (${{esc(cur)}})<input type="text"
          inputmode="decimal" class="gold-frame" id="dcalc-budget" placeholder="e.g. 10,000"
          oninput="fmtMoneyInput(this);updateDivCalc()"></label>
        <label>Price / share (${{esc(cur)}})<input type="text" inputmode="decimal" id="dcalc-price"
          value="${{groupDigits(p.toFixed(4))}}" oninput="fmtMoneyInput(this);updateDivCalc()"></label>
        <label>Dividend / share (${{esc(cur)}})<input type="text" inputmode="decimal" id="dcalc-dps"
          value="${{groupDigits(d.toFixed(4))}}" oninput="fmtMoneyInput(this);updateDivCalc()"></label>
        <button type="button" class="div-calc-reset" onclick="resetDivCalc(${{d}},${{p}})">Reset</button>
      </div>
      <div class="div-calc-out" id="dcalc-out"></div>
    </div>`;
}}
// เขียนแยกจาก renderFinTable โดยตั้งใจ — จะได้พิมพ์ในช่องคำนวณแล้วอัปเดตเฉพาะผลลัพธ์
// ไม่ต้องวาดทั้งหน้าใหม่ทุกตัวอักษรที่พิมพ์ (เสียตำแหน่งเลื่อน/โฟกัสถ้าทำแบบนั้น)
function updateDivCalc(){{
  const out = document.getElementById('dcalc-out');
  if (!out) return;
  const dps = moneyVal(document.getElementById('dcalc-dps'));
  const price = moneyVal(document.getElementById('dcalc-price'));
  const budget = moneyVal(document.getElementById('dcalc-budget'));
  const cur = divCurrency();

  // จำนวนหุ้นมาจากงบที่กรอกล้วนๆ — ปัดลงเป็นหุ้นเต็ม เพราะตลาดหุ้นทั่วไปซื้อเศษหุ้นไม่ได้
  // (ต่างจากราคา/ปันผลต่อหุ้นที่ยังพิมพ์ทศนิยมได้ตามที่รายงานจริง)
  const shares = (isFinite(budget) && isFinite(price) && price > 0 && budget > 0)
    ? Math.floor(budget / price) : null;
  const spent = shares != null ? shares * price : null;
  const left = (spent != null && isFinite(budget)) ? budget - spent : null;

  const yieldPct = (isFinite(dps) && isFinite(price) && price > 0) ? dps / price * 100 : null;
  const monthly = yieldPct != null ? yieldPct / 12 : null;
  const income = (isFinite(dps) && shares != null && shares > 0) ? dps * shares : null;

  const cell = (lbl, val, sub) => `<div class="dcalc-cell"><span>${{esc(lbl)}}</span><b>${{val}}</b>` +
    (sub ? `<i>${{esc(sub)}}</i>` : '') + `</div>`;
  out.innerHTML =
    cell('Shares you’d get', shares != null ? shares.toLocaleString() : '—',
      spent != null ? `${{divFmt(spent)}} ${{cur}} spent · ${{divFmt(left)}} ${{cur}} left over` : '') +
    cell('Annual income', income != null ? divFmt(income) + ' ' + esc(cur) + thbEq(income, cur) : '—',
      // เฉลี่ยรายเดือนจากยอดทั้งปี ไม่ใช่เงินที่เข้าจริงทุกเดือน — บริษัทส่วนใหญ่จ่ายปันผล
      // เป็นรอบไตรมาส ไม่ใช่รายเดือน จึงเขียน "avg" กำกับไว้กันเข้าใจผิดว่าเป็นเงินสดที่โอนเข้าจริง
      // เฉลี่ย/เดือนไม่ใช่ตัวเลขที่รายงานจริง แค่ประมาณ — 2 ตำแหน่งพอ ใส่ 4 ตำแหน่งจะดูแม่นยำเกินจริง
      income != null ? `≈ ${{divFmtShort(income / 12)}} ${{cur}} / month avg` +
        thbEqTxt(income / 12, cur) : '') +
    cell('Implied annual yield', yieldPct != null ? yieldPct.toFixed(2) + '%' : '—') +
    cell('Monthly equivalent', monthly != null ? monthly.toFixed(2) + '%' : '—');
  // แถวประวัติรายปีข้างล่างก็ใช้จำนวนหุ้นที่คำนวณจากงบเดียวกันนี้ อัปเดตรายได้ต่อปีให้แบบเรียลไทม์
  document.querySelectorAll('.div-income-cell').forEach(td => {{
    const rd = parseFloat(td.dataset.dps);
    const inc = (isFinite(rd) && shares != null && shares > 0) ? rd * shares : null;
    td.innerHTML = inc != null
      ? divFmt(inc) + ' ' + esc(cur) + thbEq(inc, cur) : '—';
  }});
}}
function resetDivCalc(dps, price){{
  document.getElementById('dcalc-dps').value = groupDigits(dps.toFixed(4));
  document.getElementById('dcalc-price').value = groupDigits(price.toFixed(4));
  document.getElementById('dcalc-budget').value = '';
  updateDivCalc();
}}

// ประวัติรายปี — เอาเฉพาะปีปฏิทินที่จบแล้วจริง (ปีปัจจุบันยังไม่ครบปีจึงไม่นับ) และย้อนได้
// สูงสุด 10 ปีตามที่แหล่งข้อมูลมีจริง (จะได้ไม่สัญญาความลึกที่ไม่มีข้อมูลรองรับ)
function renderDivHistory(rows, series, cur){{
  if (!rows.length) return '';
  const nowYear = new Date().getUTCFullYear();
  const byYear = {{}};
  rows.forEach(d => {{
    const y = new Date(d.date * 1000).getUTCFullYear();
    if (y >= nowYear) return;
    (byYear[y] = byYear[y] || []).push(d);
  }});
  const years = Object.keys(byYear).map(Number).sort((a, b) => b - a).slice(0, 10);
  if (!years.length) return '';

  const chartPeriods = years.slice().reverse().map(String);
  const chartVals = chartPeriods.map(y => byYear[y].reduce((a, d) => a + d.amount, 0));
  // สลับ finSpan ชั่วคราวเป็น annual ตอนวาดกราฟนี้ — กราฟปันผลเป็นรายปีเสมอไม่ว่าแท็บ
  // งบการเงินด้านล่างจะสลับไปที่ QUARTERLY อยู่ก็ตาม ไม่งั้นป้ายหน่วยจะขึ้นผิดเป็น "per quarter"
  const prevSpan = finSpan;
  finSpan = 'annual';
  const chart = divergingBarChart('Dividend per Share', chartPeriods, chartVals,
    {{fmt: v => divFmtShort(v), unit: cur + '/share', noLabel: chartPeriods.length > 6}});
  finSpan = prevSpan;

  const tblRows = years.map(y => {{
    const evs = byYear[y];
    const dps = evs.reduce((a, d) => a + d.amount, 0);
    const price0 = priceAt(series, Date.UTC(y, 0, 1) / 1000);
    const yld = (price0 != null && price0 > 0) ? dps / price0 * 100 : null;
    return {{y, dps, n: evs.length, price0, yld}};
  }});
  const rowsHtml = tblRows.map(r => `<tr>
      <td>${{r.y}}</td>
      <td>${{divFmt(r.dps)}} ${{esc(cur)}}<span class="div-pmt-n">${{r.n}} pmt${{r.n === 1 ? '' : 's'}}</span>${{thbEq(r.dps, cur)}}</td>
      <td>${{r.price0 != null ? divFmt(r.price0) + ' ' + esc(cur) + thbEq(r.price0, cur) : '<span class="met-na">—</span>'}}</td>
      <td>${{r.yld != null ? r.yld.toFixed(2) + '%' : '<span class="met-na">—</span>'}}</td>
      <td>${{r.yld != null ? (r.yld / 12).toFixed(2) + '%' : '<span class="met-na">—</span>'}}</td>
      <td class="div-income-cell" data-dps="${{r.dps}}">—</td>
    </tr>`).join('');

  return `<div class="div-hist">
      <div class="met-sub-h"><b>History</b><span>complete calendar years · ${{years.length}} of up to 10</span></div>
      ${{chart}}
      <div class="fin-table-wrap"><table class="met-tbl">
        <thead><tr><th>Year</th><th>Dividend / Share</th><th>Price at Year Start</th>
          <th>Yield</th><th>Monthly Equiv.</th><th>Income (shares held)</th></tr></thead>
        <tbody>${{rowsHtml}}</tbody>
      </table></div>
    </div>`;
}}

const FIN_SECTIONS = [
  ['div',   'DIVIDENDS'],
  ['kpi',   'KEY METRICS'],
  ['grow',  'GROWTH'],
  ['prof',  'PROFITABILITY'],
  ['bal',   'BALANCE SHEET'],
  ['lev',   'LEVERAGE & RETURNS'],
  ['stmt',  'STATEMENTS'],
];
function renderFinToolbar(){{
  const jump = FIN_SECTIONS.map(([id, label]) =>
    `<button type="button" onclick="finJump('${{id}}')">${{esc(label)}}</button>`).join('');
  const opts = finNavList().filter(l => l !== chCur).map(l =>
    `<option value="${{esc(l)}}"${{l === finCompareSym ? ' selected' : ''}}>${{esc(l)}}</option>`
  ).join('');
  // legend รวมของทั้งแดชบอร์ด — สีคู่นี้ใช้เหมือนกันทุกกราฟ เลยมีที่เดียวพอ
  const legend = finCompareSym ? `<div class="fin-cmpbar">` +
    `<span class="fin-legend-item"><i style="background:var(--brass)"></i>${{esc(chCur)}}</span>` +
    `<span class="fin-legend-item"><i style="background:${{FIN_BAR_CMP}}"></i>${{esc(finCompareSym)}}</span>` +
    `<em>same scale on every chart — bars are directly comparable</em></div>` : '';
  return `<div class="fin-toolbar"><div class="fin-jump">${{jump}}</div>` +
    `<div class="fin-cmp"><label for="fin-cmp-sel">⇄ COMPARE</label>` +
    `<select id="fin-cmp-sel" onchange="setFinCompare(this.value)">` +
    `<option value="">— none —</option>${{opts}}</select></div></div>` + legend;
}}
function finJump(id){{
  const el = document.getElementById('fin-sec-' + id);
  if (el) el.scrollIntoView({{behavior: 'smooth', block: 'start'}});
}}
const finSection = (id, title, note, inner) =>
  `<div class="fin-section" id="fin-sec-${{id}}">` +
  `<div class="fin-section-h"><b>${{esc(title)}}</b>` +
  (note ? `<span>${{esc(note)}}</span>` : '') + `</div>${{inner}}</div>`;

// ── บันทึกอ่านงบ ─────────────────────────────────────────
// อ่านจากตัวเลขที่อยู่ในหน้านี้ล้วนๆ แล้วสรุปเป็นภาษาคน — เป็นการ "ตีความ" ไม่ใช่ "ทำนาย"
// ไม่มีการเดาราคาในอนาคต ไม่มีคำว่าซื้อ/ขาย/ถือ และไม่แต่งตัวเลขที่ไม่มีในข้อมูลขึ้นมา
// ส่วน "จับตาดู" คือเงื่อนไขที่ถ้าเปลี่ยน ภาพที่อ่านได้ตรงนี้จะเปลี่ยนตาม ไม่ใช่การพยากรณ์
// ── บันทึกอ่านงบ (ไทย/อังกฤษ) ────────────────────────────
// แยกเป็นสองขั้น: หาข้อเท็จจริงจากตัวเลขก่อน แล้วค่อยเรียบเรียงเป็นประโยคตามภาษาที่เลือก
// ทำแบบนี้ตรรกะมีชุดเดียว สองภาษาจึงพูดตรงกันเสมอ ไม่มีทางหลุดไปคนละเรื่อง
// อ่านจากตัวเลขในหน้านี้ล้วนๆ เป็นการ "ตีความ" ไม่ใช่ "ทำนายราคา" และไม่มีคำแนะนำซื้อขาย
let finReadLang = 'th';
try {{ finReadLang = localStorage.getItem('finReadLang') || 'th'; }} catch(e) {{}}

function setFinReadLang(l){{
  finReadLang = l;
  try {{ localStorage.setItem('finReadLang', l); }} catch(e) {{}}
  renderFinTable();
}}

function finReadFacts(rows){{
  if (!rows || rows.length < 2) return null;
  const first = rows[0], last = rows[rows.length - 1], prev = rows[rows.length - 2];
  const gr = (a, b) => (a != null && b != null && a > 0) ? (b / a - 1) * 100 : null;
  const f = {{
    n: rows.length, span: finSpan,
    firstLbl: periodLabel(first.date, finSpan), lastLbl: periodLabel(last.date, finSpan),
    revSpan: gr(first.rev, last.rev), revYoY: gr(prev.rev, last.rev),
    nm: last.nm, dNm: (last.nm != null && first.nm != null) ? last.nm - first.nm : null,
    de: last.de, dDe: (last.de != null && first.de != null) ? last.de - first.de : null,
    roe: last.roe, noGross: (last.rev != null && last.gp == null),
  }};
  f.decel = (f.revSpan > 0 && f.revYoY > 0 && f.revYoY * (f.n - 1) < f.revSpan);
  f.accel = (f.revSpan > 0 && f.revYoY > 0 && !f.decel);
  const daily = (chData && chData.tf && chData.tf['1Y']) || null;
  if (daily && daily.length >= 200) {{
    const c = daily.map(b => b[4]);
    const ma = c.slice(-200).reduce((a, b) => a + b, 0) / 200;
    f.px = c[c.length - 1];
    f.maGap = (f.px / ma - 1) * 100;
    f.yr = (f.px / c[0] - 1) * 100;
  }}
  return f;
}}

const FIN_READ_T = {{
  th: {{
    head: 'บทวิเคราะห์', watch: 'สิ่งที่จะทำให้ภาพเปลี่ยน',
    yrs: n => `${{n}} ${{finSpan === 'annual' ? 'ปี' : 'ไตรมาส'}}ล่าสุด`,
    rev: f => {{
      const w = f.revSpan > 0 ? 'โต' : 'หด';
      const tail = f.accel ? 'และงวดล่าสุดยังไปได้เร็วกว่าค่าเฉลี่ยที่ผ่านมา'
        : f.decel ? 'แต่งวดล่าสุดโตช้าลงกว่าจังหวะเดิม'
        : (f.revSpan > 0) ? 'แต่งวดล่าสุดกลับพลิกมาติดลบ'
        : (f.revYoY > 0) ? 'โดยงวดล่าสุดเริ่มกลับมาเป็นบวกเป็นครั้งแรก'
        : 'และงวดล่าสุดก็ยังลดลงต่อ';
      return `รายได้<b>${{w}} ${{f.n1(f.revSpan)}}%</b> ตลอด ${{f.yrs}} ` +
        `<span class="${{f.sg(f.revYoY)}}">(งวดล่าสุด ${{f.n1(f.revYoY)}}%)</span> — ${{tail}}`;
    }},
    mg: f => {{
      const q = (f.revSpan > 0 && f.dNm > 0.5) ? 'เก็บกำไรได้มากขึ้นต่อยอดขายหนึ่งหน่วย แปลว่าโตแล้วกำไรตามจริง ไม่ได้ใช้ส่วนต่างกำไรแลกยอด'
        : (f.revSpan > 0 && f.dNm < -0.5) ? 'แต่เก็บกำไรได้น้อยลงต่อยอดขายหนึ่งหน่วย แปลว่ายอดที่โตมาต้องแลกด้วยส่วนต่างกำไร'
        : (f.revSpan <= 0 && f.dNm > 0.5) ? 'แต่คุมต้นทุนได้ดีขึ้น ยอดที่หดจึงยังไม่กินความสามารถทำกำไร'
        : (f.dNm < -0.5) ? 'และความสามารถทำกำไรก็บางลงไปพร้อมกับยอดขาย'
        : 'โดยส่วนต่างกำไรแทบไม่ขยับตลอดช่วงนี้';
      return `อัตรากำไรสุทธิอยู่ที่ <b>${{f.nm.toFixed(1)}}%</b> ` +
        `<span class="${{f.sg(f.dNm)}}">(${{f.n1(f.dNm)}}pp เทียบ ${{f.firstLbl}})</span> — ${{q}}`;
    }},
    bs: f => {{
      const lvl = f.de > 2 ? 'ใช้หนี้หนัก' : f.de > 1 ? 'มีหนี้ค่อนข้างมาก'
        : f.de > 0.4 ? 'มีหนี้ระดับกลางๆ' : 'แทบไม่ใช้หนี้';
      let s = `ฐานะการเงิน${{lvl}} หนี้สินต่อทุน <b>${{f.de.toFixed(2)}} เท่า</b>`;
      if (f.dDe != null && Math.abs(f.dDe) > 0.1)
        s += ` <span class="${{f.sg(-f.dDe)}}">(${{f.n1(f.dDe)}} เท่า จาก ${{f.firstLbl}})</span>`;
      if (f.roe != null) s += ` และให้ผลตอบแทนต่อส่วนของผู้ถือหุ้น <b>${{f.roe.toFixed(1)}}%</b> ในงวดล่าสุด`;
      return s;
    }},
    px: f => `ราคาในตลาดอยู่<b>${{f.maGap >= 0 ? 'เหนือ' : 'ใต้'}}เส้นค่าเฉลี่ย 200 วัน ` +
      `${{Math.abs(f.maGap).toFixed(1)}}%</b> และรอบปีที่ผ่านมา ` +
      `<span class="${{f.sg(f.yr)}}">${{f.n1(f.yr)}}%</span> — ` +
      `อันนี้บอกว่าราคาที่ผ่านมาเป็นยังไง ไม่ได้บอกว่าต่อไปจะไปทางไหน`,
    wDecel: 'งวดที่ช้าลงเป็นแค่สะดุดชั่วคราว หรือกลายเป็นจังหวะใหม่',
    wFall: 'รายได้จะกลับมาโตได้ไหม หรืองวดที่ตกกลายเป็นแนวโน้ม',
    wMargin: 'ส่วนต่างกำไรจะหยุดหดได้ไหม หรือบางลงไปอีก',
    wDebt: 'ต้นทุนการกู้ยืมรอบใหม่ เพราะหนี้เพิ่มขึ้นต่อเนื่อง',
    wRoe: 'ผลตอบแทนต่อทุนที่สูงนั้น มาจากกำไรที่ดีขึ้นจริง หรือมาจากฐานทุนที่เล็กลง',
    wNext: 'งบงวดถัดไป ซึ่งจะเข้ามาแทนคอลัมน์ใหม่สุดในตารางนี้',
    wBank: 'ธนาคารและประกันไม่ได้รายงานกำไรขั้นต้น ส่วนนั้นจึงว่างตามธรรมชาติของธุรกิจ',
    foot: 'นี่คือการอ่านตัวเลขในหน้านี้ของผมเอง เป็นการตีความ ไม่ใช่การพยากรณ์ และไม่ใช่คำแนะนำการลงทุน ' +
      'คำนวณจากตัวเลขที่รายงานล้วนๆ จึงไม่รู้เรื่องผู้บริหาร คู่แข่ง หรือภาวะเศรษฐกิจโดยรวม โปรดตัดสินใจด้วยตัวเอง',
  }},
  en: {{
    head: "The desk's read", watch: 'What would change it',
    yrs: n => `${{n}} reported ${{finSpan === 'annual' ? 'years' : 'quarters'}}`,
    rev: f => {{
      const w = f.revSpan > 0 ? 'grown' : 'shrunk';
      const tail = f.accel ? 'and the latest period ran ahead of that pace.'
        : f.decel ? 'though the latest period grew slower than the run behind it.'
        : (f.revSpan > 0) ? 'but the most recent period broke the trend and fell.'
        : (f.revYoY > 0) ? 'with the latest period the first to turn back up.'
        : 'and the latest period kept falling.';
      return `Revenue has <b>${{w}} ${{f.n1(f.revSpan)}}%</b> across ${{f.yrs}} ` +
        `<span class="${{f.sg(f.revYoY)}}">(${{f.n1(f.revYoY)}}% latest)</span> — ${{tail}}`;
    }},
    mg: f => {{
      const q = (f.revSpan > 0 && f.dNm > 0.5) ? 'it keeps more of every unit it sells than it used to, so the growth is being earned rather than bought'
        : (f.revSpan > 0 && f.dNm < -0.5) ? 'it keeps less of every unit sold than it used to, so the growth is costing margin to get'
        : (f.revSpan <= 0 && f.dNm > 0.5) ? 'it runs leaner than it did, so the shrinking topline has not eaten profitability'
        : (f.dNm < -0.5) ? 'profitability has thinned alongside the topline'
        : 'margins are essentially flat across the window';
      return `Net margin sits at <b>${{f.nm.toFixed(1)}}%</b> ` +
        `<span class="${{f.sg(f.dNm)}}">(${{f.n1(f.dNm)}}pp vs ${{f.firstLbl}})</span> — ${{q}}.`;
    }},
    bs: f => {{
      const lvl = f.de > 2 ? 'carries heavy leverage' : f.de > 1 ? 'is meaningfully levered'
        : f.de > 0.4 ? 'carries moderate debt' : 'is lightly levered';
      let s = `The balance sheet ${{lvl}} at <b>${{f.de.toFixed(2)}}x</b> debt to equity`;
      if (f.dDe != null && Math.abs(f.dDe) > 0.1)
        s += ` <span class="${{f.sg(-f.dDe)}}">(${{f.n1(f.dDe)}}x since ${{f.firstLbl}})</span>`;
      if (f.roe != null) s += `, and it returned <b>${{f.roe.toFixed(1)}}%</b> on equity last period`;
      return s + '.';
    }},
    px: f => `The market has it <b>${{Math.abs(f.maGap).toFixed(1)}}% ` +
      `${{f.maGap >= 0 ? 'above' : 'below'}}</b> its 200-day average and ` +
      `<span class="${{f.sg(f.yr)}}">${{f.n1(f.yr)}}%</span> over the past year — ` +
      `which says how the price has behaved, not where it goes next.`,
    wDecel: 'Whether the slower latest period is a blip or the new pace.',
    wFall: 'Whether the topline turns back up, or the latest drop becomes a trend.',
    wMargin: 'Whether margin compression stops, or thins further.',
    wDebt: 'Refinancing cost, since the debt load has been rising.',
    wRoe: 'How much of that return on equity comes from a shrinking equity base rather than rising profit.',
    wNext: 'The next results release, which replaces the newest column here.',
    wBank: 'Banks and insurers report no gross margin, so that part of the read is blank by nature.',
    foot: 'This is my own reading of the figures on this page — an interpretation, not a forecast ' +
      'and not investment advice. It is generated from the reported numbers alone, so it knows ' +
      'nothing of management, competition or the wider economy. Decide for yourself.',
  }},
}};

function finReadNote(rows){{
  const f = finReadFacts(rows);
  if (!f) return '';
  const L = FIN_READ_T[finReadLang] || FIN_READ_T.en;
  f.n1 = v => (v >= 0 ? '+' : '') + v.toFixed(1);
  f.sg = v => v >= 0 ? 'up' : 'down';
  f.yrs = L.yrs(f.n);
  const say = [], watch = [];
  if (f.revSpan != null && f.revYoY != null) {{
    say.push(L.rev(f));
    if (f.decel) watch.push(L.wDecel);
    if (f.revYoY <= 0 && f.revSpan > 0) watch.push(L.wFall);
  }}
  if (f.nm != null && f.dNm != null) {{
    say.push(L.mg(f));
    if (f.dNm < -0.5) watch.push(L.wMargin);
  }}
  if (f.de != null) {{
    say.push(L.bs(f));
    if (f.dDe != null && f.dDe > 0.1) watch.push(L.wDebt);
    if (f.roe != null && f.roe > 50 && f.de > 1) watch.push(L.wRoe);
  }}
  if (f.maGap != null) say.push(L.px(f));
  if (!say.length) return '';
  watch.push(L.wNext);
  if (f.noGross) watch.push(L.wBank);
  const btn = (l, t) => `<button type="button" class="fin-lang${{finReadLang === l ? ' on' : ''}}" ` +
    `onclick="setFinReadLang('${{l}}')">${{t}}</button>`;
  return `<aside class="fin-read">
    <div class="fin-read-h">${{esc(L.head)}} · ${{esc(chCur)}}
      <span class="fin-lang-sw">${{btn('th', 'ไทย')}}${{btn('en', 'EN')}}</span></div>
    ${{say.map(s => `<p>${{s}}</p>`).join('')}}
    <div class="fin-read-watch"><span>${{esc(L.watch)}}</span>
      <ul>${{watch.slice(0, 3).map(w => `<li>${{esc(w)}}</li>`).join('')}}</ul></div>
    <p class="fin-read-foot">${{esc(L.foot)}}</p>
  </aside>`;
}}

// แถบ KPI — ตัวเลขงวดล่าสุด + เทียบงวดก่อน + เทียบกับหุ้นที่เลือก (ถ้ามี)
const FIN_KPI_DEFS = [
  ['rev', 'Revenue'], ['ni', 'Net Income'], ['nm', 'Net Margin'],
  ['eps', 'Diluted EPS'], ['assets', 'Total Assets'], ['de', 'Debt / Equity'],
];
function renderFinKpiSection(rows, cmpRows){{
  if (!rows.length) return '';
  const last = rows[rows.length - 1], prev = rows.length > 1 ? rows[rows.length - 2] : null;
  const cmpLast = cmpRows && cmpRows.length ? cmpRows[cmpRows.length - 1] : null;
  const period = periodLabel(last.date, finSpan);
  // Net Income/Net Margin ติดกรอบทองเน้นพิเศษ — สองตัวที่มักเป็นจุดแรกที่คนดูงบมองหา
  const GOLD_KPI = new Set(['ni', 'nm']);
  const tiles = FIN_KPI_DEFS.map(([key, label]) => {{
    const txt = finCellText(key, last[key]) ?? '—';
    const delta = prev ? finDelta(key, last[key], prev[key]) : '';
    const cmp = cmpLast
      ? `<div class="fin-kpi-cmp"><span>${{esc(finCompareSym)}}</span>` +
        `<b>${{esc(finCellText(key, cmpLast[key]) ?? '—')}}</b></div>`
      : '';
    const gold = GOLD_KPI.has(key) ? ' gold-frame' : '';
    return `<div class="fin-kpi${{gold}}"><div class="fin-kpi-lbl">${{esc(label)}}</div>` +
      `<div class="fin-kpi-val">${{esc(txt)}}</div>${{delta}}${{cmp}}</div>`;
  }}).join('');
  const note = 'latest reported period · ' + period +
    (prev ? ' — change shown vs ' + periodLabel(prev.date, finSpan) : '');
  const body = `<div class="fin-kpi-wrap"><div class="fin-kpis">${{tiles}}</div>` +
    finReadNote(rows) + `</div>`;
  return finSection('kpi', 'KEY METRICS', note, body);
}}

// กลุ่มแผงกราฟแยกหัวข้อวิเคราะห์ — โตขึ้นแค่ไหน (Growth), กำไรต่อบาทรายได้ (Profitability),
// ฐานะการเงิน (Balance Sheet), หนี้กับผลตอบแทนผู้ถือหุ้น (Leverage & Returns)
function renderFinDashboard(rows, cmpRows){{
  const periods = rows.map(r => periodLabel(r.date, finSpan));
  const aligned = finAlign(periods, cmpRows);
  const bar = (key, title, extra) => divergingBarChart(title, periods, rows.map(r => r[key]),
    Object.assign({{metric: key, cmp: aligned ? aligned.map(r => r && r[key]) : null}},
                  extra || {{}}));
  const pct = {{fmt: v => v.toFixed(1) + '%', unit: 'percent of revenue'}};
  const ret = {{fmt: v => v.toFixed(1) + '%', unit: 'percent return'}};
  const out = [];

  const grow = [bar('rev', 'Revenue', {{stat: cagrText(rows, 'rev')}}),
                bar('ni', 'Net Income', {{stat: cagrText(rows, 'ni')}})].filter(Boolean).join('');
  if (grow) out.push(finSection('grow', 'GROWTH', 'revenue and bottom line over the periods shown',
    `<div class="fin-panel-grid">${{grow}}</div>`));

  const prof = [marginLineChart('Margin trend', periods, [
      {{name: 'Gross', vals: rows.map(r => r.gm)}},
      {{name: 'Operating', vals: rows.map(r => r.om)}},
      {{name: 'Net', vals: rows.map(r => r.nm)}},
    ]), bar('nm', 'Net Margin', pct), bar('roa', 'Return on Assets', ret)].filter(Boolean).join('');
  if (prof) out.push(finSection('prof', 'PROFITABILITY', 'how much of each unit of revenue is kept',
    `<div class="fin-panel-grid">${{prof}}</div>`));

  const bal = [bar('assets', 'Total Assets'), bar('liab', 'Total Liabilities'),
               bar('equity', 'Total Equity'), bar('cash', 'Cash & Equivalents')]
    .filter(Boolean).join('');
  if (bal) out.push(finSection('bal', 'BALANCE SHEET', 'what the company owns and owes',
    `<div class="fin-panel-grid">${{bal}}</div>`));

  const lev = [bar('de', 'Debt / Equity', {{fmt: v => v.toFixed(2) + 'x', unit: 'times equity'}}),
               bar('roe', 'Return on Equity', ret),
               bar('debt', 'Total Debt')].filter(Boolean).join('');
  if (lev) out.push(finSection('lev', 'LEVERAGE & RETURNS', 'borrowing level and shareholder return',
    `<div class="fin-panel-grid">${{lev}}</div>`));

  return out.join('');
}}

function toggleFinSort(i){{
  finSortDir = (finSortCol === i) ? -finSortDir : -1;   // คลิกแรกของคอลัมน์ = มากไปน้อย
  finSortCol = i;
  renderFinTable();
}}

function renderFinTable(){{
  const body = document.getElementById('fin-body');
  const data = finCache[chCur];
  if (!data) {{ body.innerHTML = '<div class="cempty">Loading…</div>'; return; }}

  // DIVIDENDS ไม่ขึ้นกับงบการเงินเลย — มาจาก chData.div ของไฟล์กราฟโดยตรง จึงวาดได้เสมอ
  // ไม่ว่าจะมีงบหรือไม่ก็ตาม (ก่อนหน้านี้ฟังก์ชันนี้ return ก่อนถึงตรงนี้เวลาไม่มีงบ ทำให้
  // ETF อย่าง JEPQ ที่มีปันผลจริงแต่ไม่มีงบ ไม่เคยเห็นส่วนปันผลของตัวเองเลย)
  let html = '<div class="fin-inner">' +
    finSection('div', 'DIVIDENDS',
      'trailing 12 months, priced today — every figure comes from dividends already paid',
      renderDivSection());
  const finish = () => {{
    // เก็บตำแหน่งเลื่อนไว้ก่อนวาดใหม่ — กดเรียงคอลัมน์หรือเปลี่ยนหุ้นเทียบแล้วไม่ให้เด้งกลับบนสุด
    const keep = body.scrollTop;
    body.innerHTML = html + '</div>';
    body.scrollTop = keep;
    updateDivCalc();
  }};

  // ไม่มีงบการเงินเลยทั้งรายปีและรายไตรมาส — ต่างจาก "span นี้ยังไม่มีข้อมูล" ข้างล่าง
  // ซึ่งเป็นบริษัทจริงที่มีงบอีก span หนึ่งอยู่ ตรงนี้ไม่มีงบให้เลยสักงวด (เช่น ETF) จบแค่
  // DIVIDENDS พอ ไม่ต้องขึ้น KEY METRICS/GROWTH/ฯลฯ ที่ไม่มีข้อมูลให้วาดอยู่ดี
  if (!(data.annual || []).length && !(data.quarterly || []).length) {{
    html += `<p class="met-note">No income statement or balance sheet on file for ` +
      `${{esc(chCur)}} — likely a fund or ETF, which doesn't file the same annual/quarterly ` +
      `reports a company does. The dividend history above is real and unaffected by this.</p>`;
    finish();
    return;
  }}

  let rows = data[finSpan] || [];
  if (finSpan === 'quarterly') rows = currentYearQuarters(rows);
  if (!rows.length) {{
    html += `<div class="fin-empty"><b>No ${{finSpan}} data reported yet</b>` +
      '<span>Try the other tab, or check back after the next earnings release.</span></div>';
    finish();
    return;
  }}
  rows = withRatios(rows);
  const cmpRows = finCompareRows();
  const cols = rows.map(r => periodLabel(r.date, finSpan));
  const sortKey = (finSortCol >= 0 && rows[finSortCol]) ? rows[finSortCol] : null;

  html += renderFinToolbar() +
    renderFinKpiSection(rows, cmpRows) + renderFinDashboard(rows, cmpRows);
  let table = '<div class="fin-table-wrap"><table class="fin-table"><thead><tr><th>Line item</th>' +
    cols.map((c, i) => {{
      const on = i === finSortCol;
      const ic = on ? (finSortDir > 0 ? '▲' : '▼') : '';
      return `<th class="${{on ? 'sorted' : ''}}" onclick="toggleFinSort(${{i}})">` +
        `${{esc(c)}}${{ic ? `<span class="fin-sort-ic">${{ic}}</span>` : ''}}</th>`;
    }}).join('') + '</tr></thead><tbody>';

  // จัดกลุ่มตามหมวดก่อน แล้วค่อย sort ภายในแต่ละหมวด — ไม่ปนงบกำไรขาดทุนกับงบดุลตอนเรียง
  const bySec = {{}};
  for (const f of FIN_FIELDS) (bySec[f[2]] = bySec[f[2]] || []).push(f);
  for (const secName of ['income', 'balance', 'ratios']) {{
    let items = bySec[secName] || [];
    if (!items.length) continue;
    if (sortKey) {{
      items = items.slice().sort((a, b) => {{
        const av = sortKey[a[0]], bv = sortKey[b[0]];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;              // ไม่มีข้อมูล ("—") ไปอยู่ท้ายเสมอ
        if (bv == null) return -1;
        return (av - bv) * finSortDir;
      }});
    }}
    table += `<tr class="fin-sec"><td colspan="${{cols.length + 1}}">${{FIN_SEC[secName]}}</td></tr>`;
    for (const [key, label] of items) {{
      table += `<tr><td>${{esc(label)}}</td>` + rows.map((r, i) => {{
        const v = r[key];
        const txt = finCellText(key, v);
        if (txt == null) return '<td class="fin-na">—</td>';
        const delta = i > 0 ? finDelta(key, v, rows[i - 1][key]) : '';
        return `<td><span class="fin-val">${{txt}}</span>${{delta}}</td>`;
      }}).join('') + '</tr>';
    }}
  }}
  table += '</tbody></table></div>';
  html += finSection('stmt', 'STATEMENTS',
    'click a period column to sort line items within each group', table);
  finish();
}}

function pickFinSpan(span){{
  finSpan = span;
  finSortCol = -1; finSortDir = -1;
  document.querySelectorAll('.fin-tab').forEach(b =>
    b.classList.toggle('on', b.dataset.span === span));
  renderFinTable();
}}

// ลูกศรเปลี่ยนหุ้นโดยไม่ต้องปิดหน้าต่าง — ไล่ในรายการโปรดก่อนถ้าหุ้นปัจจุบันติดดาวไว้
// (และมีงบให้ดู) ไม่งั้นไล่ทุกตัวที่มีงบการเงิน กันเผื่อเข้ามาจากการค้นหา
//
// includeDivOnly = true: ใช้กับลูกศร ‹ › เลื่อนหุ้นในหัวหน้าต่าง — รวม ETF ที่ไม่มีงบแต่มี
// ปันผลจริงด้วย (เช่น JEPQ) จะได้เลื่อนไปเจอ ไม่ข้ามไปเงียบๆ
// ปล่อยว่าง (ค่าเริ่มต้น false): ใช้กับดรอปดาวน์ "เทียบกับ" ที่ต้องมีแถวงบการเงินมาจับคู่
// เทียบกัน — ตัวที่ไม่มีงบจับคู่เทียบไม่ได้อยู่ดี จึงยังกรองด้วย .f เหมือนเดิม
function finNavList(includeDivOnly){{
  const withData = Object.keys(CHARTS).filter(l =>
    CHARTS[l].f || (includeDivOnly && CHARTS[l].d));
  const favs = withData.filter(l => chFavs.has(l));
  const list = (favs.length > 1 && favs.includes(chCur)) ? favs : withData;
  return list.slice().sort((a, b) => a.localeCompare(b));
}}
async function finNav(step){{
  const list = finNavList(true);
  if (list.length < 2) return;
  const i = list.indexOf(chCur);
  const next = list[((i < 0 ? 0 : i) + step + list.length) % list.length];
  await pickChart(next);
  await openFinancials();
}}

async function openFinancials(){{
  const hasFin = !!CHARTS[chCur]?.f, hasDiv = !!CHARTS[chCur]?.d;
  if (!chCur || !(hasFin || hasDiv)) return;
  document.getElementById('metmodal').hidden = true;
  finSortCol = -1; finSortDir = -1; finCompareSym = null;
  document.getElementById('fin-name').textContent = chCur;
  setSymFull('fin-full', chCur);
  document.getElementById('fin-currency').textContent =
    TNEWS[chCur]?.group === 'th' ? 'THB' : '';
  // "เช็คงบล่าสุดเมื่อไร" มีความหมายเฉพาะตอนมีงบการเงินจริง — ETF ที่มีแต่ปันผลไม่ต้องมีบรรทัดนี้
  document.getElementById('fin-checked').textContent =
    (hasFin && window.__FIN_AT__) ? 'Statements last checked ' + window.__FIN_AT__ +
      ' · rechecked daily, sooner if a new quarter is reported' : '';
  // ป้ายบอกที่มาของงบ + แท็บ ANNUAL/QUARTERLY มีความหมายเฉพาะตอนมีงบจริง — ตัวที่มีแต่
  // ปันผล (เช่น JEPQ) ไม่มีอะไรให้สลับดู ซ่อนทั้งคู่ไว้ไม่งั้นจะดูเหมือนมีงบทั้งที่ไม่มี
  document.getElementById('fin-note-stmt').hidden = !hasFin;
  document.querySelector('.fin-tabs').hidden = !hasFin;
  document.getElementById('finmodal').hidden = false;
  document.body.style.overflow = 'hidden';
  // จับชื่อตัวที่กำลังเปิดไว้ก่อน — ถ้าผู้ใช้กดลูกศรเปลี่ยนตัวระหว่างที่ยังโหลดไม่เสร็จ
  // ของที่โหลดมาต้องไม่ไปตกใส่ตัวใหม่ (ของเดิมเขียน finCache[chCur] หลัง await จึงสลับกันได้)
  const sym = chCur;
  if (!finCache[sym]) {{
    document.getElementById('fin-body').innerHTML = '<div class="cempty">Loading…</div>';
    await loadFin(sym);
    if (chCur !== sym) return;        // เปลี่ยนตัวไปแล้ว ปล่อยให้รอบของตัวใหม่วาดเอง
  }}
  if (chCur && (hasFin || hasDiv)) {{
    renderFinTable();
    document.getElementById('fin-body').scrollTop = 0;   // เปลี่ยนหุ้น = เริ่มอ่านจากบนสุดใหม่
  }}
}}
function closeFinancials(){{
  document.getElementById('metmodal').hidden = true;   // ปิดชั้นฉบับเต็มที่ซ้อนอยู่ไปด้วย
  document.getElementById('finmodal').hidden = true;
  if (document.getElementById('cmodal').hidden) document.body.style.overflow = '';
}}
// เลือกหุ้นเทียบจากดรอปดาวน์บนแถบเครื่องมือ — ดึงงบมาเก็บใน finCache ก้อนเดียวกัน
// แล้ว re-render ทั้งแดชบอร์ด (แท่งกราฟขึ้นเป็นคู่ + KPI มีบรรทัดเทียบเพิ่ม)
async function setFinCompare(label){{
  finCompareSym = label || null;
  // ตัวที่เอามาเทียบมักเป็นรายการโปรดที่โหลดล่วงหน้าไว้แล้ว กรณีนั้นขึ้นทันทีไม่ต้องรอ
  if (finCompareSym && !finCache[finCompareSym]) await loadFin(finCompareSym);
  renderFinTable();
}}
document.getElementById('finmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'finmodal') closeFinancials();
}});

// ── แถบเครื่องมือ: เปิด/ปิดอินดิเคเตอร์ ────────────────────
function renderIndList(){{
  document.getElementById('ind-list').innerHTML = IND.map(g =>
    `<div class="cpop-grp">${{g.g}}</div>` + g.items.map(i =>
      `<button class="cpop-row${{chInd.has(i.k) ? ' on' : ''}}" type="button"
         data-ind="${{i.k}}" onclick="toggleInd('${{i.k}}')">
        <span class="sw" style="background:${{i.c}}"></span>${{i.n}}
        <span class="ck">✓</span></button>`).join('')).join('');
  const n = chInd.size, b = document.getElementById('crn');
  b.textContent = n; b.hidden = !n;
  document.querySelector('.crbtn[data-pop="ind"]').classList.toggle('on', !!n);
  wireIndTips();
}}
// ── กล่องอธิบายอินดิเคเตอร์ ────────────────────────────────
let tipT = null;
function showIndTip(k, el){{
  const d = (IND_MAP[k] || {{}}).d;
  if (!d) return;
  const tip = document.getElementById('ind-tip');
  tip.innerHTML = `<div class="tip-h">${{IND_MAP[k].n}}</div>
    <div class="tip-f">${{d.f}}</div>
    <div class="tip-r"><b>วิเคราะห์อะไร</b>${{d.w}}</div>
    <div class="tip-r"><b>ใช้เมื่อไร</b>${{d.t}}</div>
    <div class="tip-r"><b>มักใช้ทำอะไร</b>${{d.u}}</div>`;
  tip.hidden = false;
  const r = el.getBoundingClientRect();
  const w = tip.offsetWidth, h = tip.offsetHeight;
  let left = r.left - w - 12;
  if (left < 8) left = Math.min(r.right + 12, innerWidth - w - 8);
  tip.style.left = Math.max(8, left) + 'px';
  tip.style.top = Math.max(8, Math.min(innerHeight - h - 8, r.top - 12)) + 'px';
}}
function hideIndTip(){{
  clearTimeout(tipT);
  document.getElementById('ind-tip').hidden = true;
}}
function wireIndTips(){{
  const list = document.getElementById('ind-list');
  if (!list || list.__wired) return;
  list.__wired = true;
  list.addEventListener('mouseover', ev => {{
    const row = ev.target.closest('.cpop-row');
    if (!row) return;
    clearTimeout(tipT);
    tipT = setTimeout(() => showIndTip(row.dataset.ind, row), 320);
  }});
  list.addEventListener('mouseout', ev => {{
    if (!ev.relatedTarget || !ev.relatedTarget.closest('.cpop-row')) hideIndTip();
  }});
  // มือถือ: กดค้างเพื่อดูคำอธิบาย แล้วไม่ให้นับเป็นการกดเปิดอินดิเคเตอร์
  list.addEventListener('touchstart', ev => {{
    const row = ev.target.closest('.cpop-row');
    if (!row) return;
    row.dataset.long = '';
    clearTimeout(tipT);
    tipT = setTimeout(() => {{ row.dataset.long = '1'; showIndTip(row.dataset.ind, row); }}, 420);
  }}, {{passive: true}});
  list.addEventListener('touchend', ev => {{
    const row = ev.target.closest('.cpop-row');
    clearTimeout(tipT);
    if (row && row.dataset.long === '1') {{ ev.preventDefault(); row.dataset.long = ''; }}
  }});
  list.addEventListener('touchmove', () => clearTimeout(tipT), {{passive: true}});
}}

function toggleInd(k){{
  hideIndTip();
  chInd.has(k) ? chInd.delete(k) : chInd.add(k);
  try {{ localStorage.setItem('chInd', JSON.stringify([...chInd])); }} catch(e) {{}}
  renderIndList(); renderChart();
}}
function clearInd(){{
  chInd.clear();
  try {{ localStorage.setItem('chInd', '[]'); }} catch(e) {{}}
  renderIndList(); renderChart();
}}
function toggleTool(t){{
  chTools.has(t) ? chTools.delete(t) : chTools.add(t);
  try {{ localStorage.setItem('chTools', JSON.stringify([...chTools])); }} catch(e) {{}}
  syncTools(); renderChart();
}}
function syncTools(){{
  document.querySelectorAll('.crbtn[data-tool]').forEach(b =>
    b.classList.toggle('on', chTools.has(b.dataset.tool)));
}}
function togglePop(id){{
  const p = document.getElementById('pop-' + id);
  const open = p.hidden;
  document.querySelectorAll('.cpop').forEach(x => {{ x.hidden = true; }});
  p.hidden = !open;
}}
function resetZoom(){{
  const svg = d3.select('#cchart svg');
  if (svg.node() && chZoom) svg.call(chZoom.transform, d3.zoomIdentity);
}}
document.addEventListener('click', ev => {{
  // คลิกนอกแถบเครื่องมือให้ปิดป๊อปอัป
  if (!ev.target.closest('.crail')) {{
    document.querySelectorAll('.cpop').forEach(x => {{ x.hidden = true; }});
    hideIndTip();
  }}
}});

function renderChart(){{
  const host = document.getElementById('cchart');
  const avail = CH_TF.filter(t => (chData?.tf || {{}})[t]?.length);
  if (!avail.length) {{ host.innerHTML = '<div class="cempty">No chart data</div>'; return; }}
  if (!avail.includes(chTf)) chTf = avail.includes('3M') ? '3M' : avail[0];
  // เจาะเฉพาะปุ่มช่วงเวลา ไม่งั้นจะไปปิดปุ่มเลือกชนิดกราฟที่ใช้คลาสเดียวกัน
  document.querySelectorAll('#cmodal-tf .tfbtn').forEach(b => {{
    b.classList.toggle('on', b.dataset.tf === chTf);
    b.disabled = !avail.includes(b.dataset.tf);
    b.style.opacity = avail.includes(b.dataset.tf) ? '' : '.35';
  }});

  const rows = chData.tf[chTf];
  const closes = rows.map(r => r[4]);
  const has = k => chInd.has(k);

  // คำนวณเฉพาะเส้นที่เปิดใช้อยู่ จะได้ไม่เสียเวลากับตัวที่ไม่ได้ดู
  const S = {{}};
  if (has('sma20'))  S.sma20  = smaA(closes, 20);
  if (has('sma50'))  S.sma50  = smaA(closes, 50);
  if (has('sma200')) S.sma200 = smaA(closes, 200);
  if (has('ema12'))  S.ema12  = emaA(closes, 12);
  if (has('ema26'))  S.ema26  = emaA(closes, 26);
  if (has('vwap'))   S.vwap   = vwapA(rows, 20);
  if (has('bb')) {{
    const mid = smaA(closes, 20), sd = stdA(closes, 20);
    S.bbm = mid;
    S.bbu = mid.map((v, i) => v == null ? null : v + 2 * sd[i]);
    S.bbl = mid.map((v, i) => v == null ? null : v - 2 * sd[i]);
  }}
  if (has('rsi'))  S.rsi  = rsiA(closes, 14);
  if (has('macd')) S.macd = macdA(closes);
  if (has('atr'))  S.atr  = atrA(rows, 14);
  if (has('dc')) {{
    S.dcu = rollA(rows.map(r => r[2]), 20, Math.max);
    S.dcl = rollA(rows.map(r => r[3]), 20, Math.min);
  }}

  const OVER = [['sma20','sma20'],['sma50','sma50'],['sma200','sma200'],
                ['ema12','ema12'],['ema26','ema26'],['vwap','vwap'],
                ['bb','bbm'],['bb','bbu'],['bb','bbl'],
                ['dc','dcu'],['dc','dcl']];
  const subs = ['vol', 'rsi', 'macd', 'atr'].filter(has);

  const W = host.clientWidth || 700, H = host.clientHeight || 360;
  const m = {{t: 9, r: 58, b: 20, l: 8}};
  const iw = Math.max(50, W - m.l - m.r);
  const innerH = Math.max(90, H - m.t - m.b);
  const subH = subs.length ? Math.max(46, Math.min(78, innerH * 0.9 / (subs.length + 2.2))) : 0;
  // พื้นขั้นต่ำของกราฟหลักต้องยืดหยุ่นตามความสูงจริง — ตายตัวที่ 80px ทำให้บนจอมือถือ
  // (กราฟสูงราว 113px) กราฟหลัก 80 + แผงย่อย 46 ล้นพื้นที่ไป 13px แผงย่อยเลยไปทับแถวป้ายวันที่
  // จอปกติไม่เปลี่ยนอะไร เพราะ innerH − แผงย่อย มากกว่าพื้นขั้นต่ำอยู่แล้ว
  const mainMin = Math.min(80, innerH * 0.5);
  const mainH = Math.max(mainMin, innerH - subs.length * subH);

  host.innerHTML = '';
  const svg = d3.select(host).append('svg').attr('viewBox', `0 0 ${{W}} ${{H}}`);
  const defs = svg.append('defs');
  // เปิดโหมดเทรนด์ต้องเผื่อที่ว่างขวามือไว้กางกรวยข้างหน้า
  const fcast = has('trend') ? Math.max(6, Math.round(rows.length * 0.14)) : 0;
  const x0 = d3.scaleLinear().domain([-0.6, rows.length - 0.4 + fcast]).range([0, iw]);

  // แต่ละแพเนลมีสเกล y ของตัวเอง แต่ใช้แกนเวลาร่วมกัน
  const panes = [];
  let top = 0;
  const addPane = (k, h, label) => {{
    const g = svg.append('g').attr('transform', `translate(${{m.l}},${{m.t + top}})`);
    defs.append('clipPath').attr('id', 'clip-' + k)
        .append('rect').attr('width', iw).attr('height', h);
    if (panes.length) g.append('line').attr('class', 'c-pane-sep')
        .attr('x1', 0).attr('x2', iw + m.r - 6).attr('y1', 0).attr('y2', 0);
    const p = {{k, h, top, g, label,
      y: d3.scaleLinear().range([h, 0]),
      grid: g.append('g').attr('class', 'c-grid'),
      body: g.append('g').attr('clip-path', 'url(#clip-' + k + ')'),
      axis: g.append('g').attr('class', 'c-axis').attr('transform', `translate(${{iw}},0)`),
      leg: g.append('g').attr('class', 'c-leg').attr('transform', 'translate(2,11)')}};
    panes.push(p);
    top += h;
    return p;
  }};
  const main = addPane('main', mainH, '');
  const pane = {{}};
  subs.forEach(k => {{ pane[k] = addPane(k, subH, IND_MAP[k].pane); }});

  const gX = svg.append('g').attr('class', 'c-axis')
      .attr('transform', `translate(${{m.l}},${{m.t + innerH}})`);
  const over = svg.append('g').attr('transform', `translate(${{m.l}},${{m.t}})`)
      .style('pointer-events', 'none');
  const crossX = over.append('line').attr('class', 'c-cross')
      .attr('y1', 0).attr('y2', innerH).style('display', 'none');
  const crossY = over.append('line').attr('class', 'c-cross')
      .attr('x1', 0).attr('x2', iw).style('display', 'none');
  const tagY = over.append('g').attr('class', 'c-tag').style('display', 'none');
  tagY.append('rect').attr('width', 52).attr('height', 15).attr('x', iw + 2).attr('y', -7.5);
  tagY.append('text').attr('x', iw + 28).attr('y', 3.4).attr('text-anchor', 'middle');
  const tagNow = over.append('g').attr('class', 'c-tag now');
  tagNow.append('rect').attr('width', 52).attr('height', 15).attr('x', iw + 2).attr('y', -7.5);
  tagNow.append('text').attr('x', iw + 28).attr('y', 3.4).attr('text-anchor', 'middle')
      .attr('fill', 'var(--bg)');

  const fmtT = ts => {{
    const dt = new Date(ts * 1000);
    return chTf === '1D'
      ? dt.toLocaleTimeString('th-TH', {{hour: '2-digit', minute: '2-digit'}})
      : dt.toLocaleDateString('th-TH', {{day: '2-digit', month: 'short',
          year: ['1Y', '3Y', '5Y', '10Y'].includes(chTf) ? '2-digit' : undefined}});
  }};
  // ป้ายบนแกนเวลาใช้แบบย่อกว่าแถบอ่านค่า — ช่วงยาวตั้งแต่ 1 ปีขึ้นไปป้ายห่างกันเป็นเดือน
  // วันที่จึงเป็นสัญญาณรบกวน ตัดออกแล้วป้ายสั้นลงเกือบครึ่ง ใส่ป้ายได้มากขึ้นในความกว้างเท่าเดิม
  // (แถบอ่านค่าตอนชี้เมาส์ยังใช้ fmtT ที่มีวันที่เต็ม เพราะตรงนั้นต้องรู้ว่าแท่งไหนจริงๆ)
  const LONG_TF = ['1Y', '3Y', '5Y', '10Y'];
  const fmtAxis = ts => LONG_TF.includes(chTf)
    ? new Date(ts * 1000).toLocaleDateString('th-TH', {{month: 'short', year: '2-digit'}})
    : fmtT(ts);
  const fmtP = n => d3.format(Math.abs(n) >= 1000 ? ',.0f' : ',.2f')(n);
  const fmtV = n => n >= 1e9 ? (n / 1e9).toFixed(1) + 'B'
    : n >= 1e6 ? (n / 1e6).toFixed(1) + 'M'
    : n >= 1e3 ? (n / 1e3).toFixed(0) + 'K' : String(n);
  const lineGen = (zx, yy, arr, i0) => d3.line()
      .defined(d => d.v != null && isFinite(d.v))
      .x(d => zx(d.i)).y(d => yy(d.v))(
        arr.map((v, k) => ({{i: i0 + k, v}})));

  function paneAxis(p, ticks, fmt) {{
    if (chTools.has('grid')) {{
      p.grid.selectAll('line').data(ticks).join('line')
        .attr('x1', 0).attr('x2', iw).attr('y1', p.y).attr('y2', p.y);
    }} else {{ p.grid.selectAll('line').remove(); }}
    p.axis.selectAll('text').data(ticks).join('text')
      .attr('x', 6).attr('y', p.y).attr('dy', '.32em').text(fmt);
  }}

  let lastTM = null;             // เก็บผลโมเดลเทรนด์ไว้ให้ป้ายกำกับใช้ต่อ
  function draw(t){{
    const zx = t.rescaleX(x0);
    const i0 = Math.max(0, Math.floor(zx.invert(0)));
    const i1 = Math.min(rows.length - 1, Math.ceil(zx.invert(iw)));
    const vis = rows.slice(i0, i1 + 1);
    if (!vis.length) return;

    // ── แพเนลราคา ──
    let lo = d3.min(vis, d => d[3]), hi = d3.max(vis, d => d[2]);
    OVER.forEach(([k, s]) => {{
      if (!has(k) || !S[s]) return;
      const seg = S[s].slice(i0, i1 + 1).filter(v => v != null && isFinite(v));
      if (seg.length) {{ lo = Math.min(lo, d3.min(seg)); hi = Math.max(hi, d3.max(seg)); }}
    }});
    // แนวรับ-ต้าน กับ ช่องแนวโน้ม คำนวณจากกรอบที่มองเห็น จึงขยับตามการซูม
    const sr = has('sr') || has('trend') ? srLevels(vis) : [];
    const tm = has('trend') ? trendModel(vis) : null;
    lastTM = tm;
    if (tm) {{
      // เผื่อกรอบให้เห็นทั้งช่องแนวโน้มและกรวย
      const k = fcast;
      const band = tm.last * (Math.exp(2 * tm.sd * Math.sqrt(k)) - 1);
      lo = Math.min(lo, tm.last - band, tm.inter + tm.slope * (tm.lastI + k) + Math.min(0, tm.off));
      hi = Math.max(hi, tm.last + band, tm.inter + tm.slope * (tm.lastI + k) + Math.max(0, tm.off));
    }}
    const rc = has('regr') ? regrChannel(vis) : null;
    if (rc) {{
      const ends = [rc.a - rc.sd, rc.a + rc.sd,
                    rc.a + rc.b * (rc.n - 1) - rc.sd, rc.a + rc.b * (rc.n - 1) + rc.sd];
      lo = Math.min(lo, d3.min(ends)); hi = Math.max(hi, d3.max(ends));
    }}
    const pad = (hi - lo) * 0.08 || Math.abs(hi) * 0.02 || 1;
    const logOn = chTools.has('log') && lo - pad > 0;
    main.y = (logOn ? d3.scaleLog() : d3.scaleLinear())
      .domain([lo - pad, hi + pad]).range([mainH, 0]);
    paneAxis(main, logOn ? main.y.ticks(5, ',.0f').concat([lo, hi]) : main.y.ticks(6), fmtP);

    // จำนวนป้ายวันที่ต้องคิดจากความกว้างที่มีจริง ไม่ใช่ตายตัว 7 ป้ายทุกจอ — ป้ายแบบมีปี
    // อย่าง "03 ก.ย. 69" กว้างราว 70px เจ็ดป้ายกินพื้นที่เกินความกว้างกราฟบนมือถือ
    // เคยทำให้ช่วง 10Y บนจอแคบป้ายซ้อนกัน 6 จาก 7 ป้ายจนอ่านไม่ออกเลย
    // ความกว้างป้ายวัดจากของจริง: "ก.ย. 69" ~46px · "04 มิ.ย." ~48px · "20:30" ~38px
    // บวกช่องไฟกันชนกันอีกราว 12px
    const lblW = chTf === '1D' ? 50 : LONG_TF.includes(chTf) ? 58 : 60;
    const seg = Math.max(1, Math.min(6, Math.floor(iw / lblW) - 1));
    const step = Math.max(1, Math.ceil((i1 - i0) / seg));
    const xt = [];
    for (let i = i0; i <= i1; i += step) xt.push(i);
    gX.selectAll('text').data(xt).join('text')
      .attr('x', d => zx(d)).attr('y', 14).attr('text-anchor', 'middle')
      .text(d => fmtAxis(rows[d][0]));

    const gC = main.body;
    if (chType === 'line') {{
      gC.selectAll('g.cd').remove();
      const up = vis[vis.length - 1][4] >= vis[0][4];
      gC.selectAll('path.cline').data([0]).join('path').attr('class', 'cline')
        .attr('d', d3.line().x((d, k) => zx(i0 + k)).y(d => main.y(d[4]))(vis))
        .attr('fill', 'none').attr('stroke-width', 1.7)
        .attr('stroke', up ? 'var(--up)' : 'var(--down)');
    }} else {{
      gC.selectAll('path.cline').remove();
      const bw = Math.max(1, Math.min(18, (zx(1) - zx(0)) * 0.68));
      const g = gC.selectAll('g.cd').data(vis, d => d[0]).join(
        en => {{ const s = en.append('g').attr('class', 'cd');
                 s.append('line'); s.append('rect'); return s; }});
      g.attr('class', d => 'cd ' + (d[4] >= d[1] ? 'c-up' : 'c-down'))
       .attr('transform', (d, k) => `translate(${{zx(i0 + k)}},0)`);
      g.select('line').attr('x1', 0).attr('x2', 0)
        .attr('y1', d => main.y(d[2])).attr('y2', d => main.y(d[3])).attr('stroke-width', 1);
      g.select('rect').attr('x', -bw / 2).attr('width', bw)
        .attr('y', d => main.y(Math.max(d[1], d[4])))
        .attr('height', d => Math.max(1, Math.abs(main.y(d[1]) - main.y(d[4]))));
    }}

    // เส้นอินดิเคเตอร์บนแพเนลราคา
    const lines = OVER.filter(([k]) => has(k) && S[OVER.find(o => o[0] === k)[1]])
      .map(([k, s]) => ({{k: k + '-' + s, c: IND_MAP[k].c,
                         dash: s === 'bbm' ? '4 3' : null,
                         d: lineGen(zx, main.y, S[s].slice(i0, i1 + 1), i0)}}));
    gC.selectAll('path.ind').data(lines, d => d.k).join('path')
      .attr('class', 'ind').attr('fill', 'none').attr('stroke-width', 1.3)
      .attr('stroke', d => d.c).attr('stroke-dasharray', d => d.dash)
      .attr('d', d => d.d);

    // ── Fibonacci จากช่วงที่มองเห็น ──
    const fibs = chTools.has('fib')
      ? FIB.map(f => ({{f, v: hi - (hi - lo) * f}})) : [];
    const fg = gC.selectAll('g.c-fib').data(fibs, d => d.f).join(
      en => {{ const s = en.append('g').attr('class', 'c-fib');
               s.append('line'); s.append('text'); return s; }});
    fg.select('line').attr('x1', 0).attr('x2', iw)
      .attr('y1', d => main.y(d.v)).attr('y2', d => main.y(d.v))
      .attr('stroke', d => d.f === 0 || d.f === 1 ? 'var(--dim)' : 'var(--brass)')
      .attr('opacity', .75);
    fg.select('text').attr('x', 4).attr('y', d => main.y(d.v) - 3)
      .attr('fill', 'var(--brass)')
      .text(d => (d.f * 100).toFixed(1) + '%  ' + fmtP(d.v));

    // ── แนวรับ-แนวต้านอัตโนมัติ ──
    const lastC = rows[i1][4];
    const sg = gC.selectAll('g.c-sr').data(sr, d => d.v).join(
      en => {{ const s = en.append('g').attr('class', 'c-sr');
               s.append('line'); s.append('text'); return s; }});
    sg.select('line').attr('x1', 0).attr('x2', iw)
      .attr('y1', d => main.y(d.v)).attr('y2', d => main.y(d.v))
      .attr('stroke', d => d.v >= lastC ? 'var(--down)' : 'var(--up)')
      .attr('stroke-width', d => Math.min(2.4, 0.9 + d.n * 0.35))
      .attr('stroke-dasharray', '2 3').attr('opacity', .85);
    sg.select('text').attr('x', 4).attr('y', d => main.y(d.v) - 3)
      .attr('font-size', 9.5).attr('font-family', "'IBM Plex Mono',monospace")
      .attr('fill', d => d.v >= lastC ? 'var(--down)' : 'var(--up)')
      .text(d => (d.v >= lastC ? 'R ' : 'S ') + fmtP(d.v) + '  ×' + d.n);

    // ── แผนที่เทรนด์: ขาขึ้น-ขาลง + ช่องแนวโน้ม + กรวยข้างหน้า ──
    const legG = gC.selectAll('g.c-trend').data(tm ? [0] : []).join(
      en => en.append('g').attr('class', 'c-trend'));
    if (tm) {{
      const X = i => zx(i0 + i);
      const Y = v => main.y(v);

      // กรวยความผันผวนข้างหน้า — วาดก่อนเพื่อให้อยู่ใต้เส้นอื่น
      const steps = [];
      for (let k = 0; k <= fcast; k++) steps.push(k);
      const band = (k, mult) => tm.last * (Math.exp(mult * tm.sd * Math.sqrt(k)) - 1);
      const cone = (mult, cls) => {{
        const up = steps.map(k => [X(tm.lastI + k), Y(tm.last + band(k, mult))]);
        const dn = steps.map(k => [X(tm.lastI + k), Y(tm.last - band(k, mult))]).reverse();
        return 'M' + up.concat(dn).map(p => p.join(',')).join('L') + 'Z';
      }};
      legG.selectAll('path.c-cone').data([[2, .10], [1, .16]]).join('path')
        .attr('class', 'c-cone')
        .attr('d', d => cone(d[0]))
        .attr('fill', IND_MAP.trend.c).attr('opacity', d => d[1]).attr('stroke', 'none');

      // ช่องแนวโน้มของขาปัจจุบัน ต่อเส้นประออกไปข้างหน้า
      const s = tm.cur.a.i, e = tm.lastI + fcast;
      const chan = [{{k: 'base', off: 0}}, {{k: 'par', off: tm.off}}];
      legG.selectAll('line.c-chan').data(chan, d => d.k).join('line')
        .attr('class', 'c-chan')
        .attr('x1', X(s)).attr('x2', X(e))
        .attr('y1', d => Y(tm.inter + tm.slope * s + d.off))
        .attr('y2', d => Y(tm.inter + tm.slope * e + d.off))
        .attr('stroke', tm.cur.up ? 'var(--up)' : 'var(--down)')
        .attr('stroke-width', 1.2).attr('opacity', .55)
        .attr('stroke-dasharray', '6 5');

      // ขาขึ้น-ขาลงจริง ลากจากจุดกลับตัวถึงจุดกลับตัว
      legG.selectAll('line.c-leg').data(tm.legs, d => d.a.i + '-' + d.b.i).join('line')
        .attr('class', 'c-leg')
        .attr('x1', d => X(d.a.i)).attr('y1', d => Y(d.a.v))
        .attr('x2', d => X(d.b.i)).attr('y2', d => Y(d.b.v))
        .attr('stroke', d => d.up ? 'var(--up)' : 'var(--down)')
        .attr('stroke-width', d => d === tm.cur ? 2.6 : 1.6)
        .attr('opacity', d => d === tm.cur ? .95 : .5)
        .attr('stroke-linecap', 'round');

      // จุดกลับตัว
      legG.selectAll('circle.c-piv').data(tm.piv, d => d.i + d.t).join('circle')
        .attr('class', 'c-piv')
        .attr('cx', d => X(d.i)).attr('cy', d => Y(d.v)).attr('r', 2.6)
        .attr('fill', d => d.t === 'H' ? 'var(--down)' : 'var(--up)')
        .attr('stroke', 'var(--bg)').attr('stroke-width', 1);

      // ป้าย % ของแต่ละขา เขียนเฉพาะขาที่กว้างพอ ไม่งั้นตัวหนังสือทับกัน
      const wide = tm.legs.filter(d => Math.abs(X(d.b.i) - X(d.a.i)) > 46);
      legG.selectAll('text.c-legt').data(wide, d => d.a.i + '-' + d.b.i).join('text')
        .attr('class', 'c-legt')
        .attr('x', d => (X(d.a.i) + X(d.b.i)) / 2)
        .attr('y', d => (Y(d.a.v) + Y(d.b.v)) / 2 - 5)
        .attr('text-anchor', 'middle').attr('font-size', 9.5)
        .attr('font-family', "'IBM Plex Mono',monospace")
        .attr('fill', d => d.up ? 'var(--up)' : 'var(--down)')
        .text(d => (d.pct >= 0 ? '+' : '') + d.pct.toFixed(1) + '%');
    }}

    // ── ช่องแนวโน้มจากเส้นถดถอย ──
    const rcLines = rc ? [
      {{k: 'mid', off: 0, dash: null}},
      {{k: 'up',  off: rc.sd, dash: '5 4'}},
      {{k: 'dn',  off: -rc.sd, dash: '5 4'}}] : [];
    const rg = gC.selectAll('line.c-regr').data(rcLines, d => d.k).join('line')
      .attr('class', 'c-regr')
      .attr('x1', zx(i0)).attr('x2', zx(i1))
      .attr('y1', d => main.y(rc.a + d.off))
      .attr('y2', d => main.y(rc.a + rc.b * (rc.n - 1) + d.off))
      .attr('stroke', IND_MAP.regr.c).attr('stroke-width', d => d.k === 'mid' ? 1.6 : 1.1)
      .attr('stroke-dasharray', d => d.dash).attr('opacity', d => d.k === 'mid' ? .95 : .6);

    // ── เส้นค่าคำนวณที่กดเปิดจากแผง METRICS ──
    const ovs = [];
    for (const k of chOverlays) {{
      const st = OV_STYLE[k];
      if (k === 'trend') {{
        const tr = chLevels.trend;
        if (!tr) continue;
        ovs.push({{k, st, y1: tr.first + tr.slope * i0, y2: tr.first + tr.slope * i1, line: true}});
      }} else if (chLevels[k] != null) {{
        ovs.push({{k, st, y1: chLevels[k], y2: chLevels[k]}});
      }}
    }}
    const og = gC.selectAll('g.ov').data(ovs, d => d.k).join(
      en => {{ const s = en.append('g').attr('class', 'ov');
               s.append('line'); s.append('text'); return s; }});
    og.select('line')
      .attr('x1', 0).attr('x2', iw)
      .attr('y1', d => main.y(d.y1)).attr('y2', d => main.y(d.y2))
      .attr('stroke', d => d.st.color).attr('stroke-width', 1.5)
      .attr('stroke-dasharray', d => d.line ? '6 4' : '4 4');
    og.select('text')
      .attr('x', 5).attr('y', d => main.y(d.y1) - 4)
      .attr('fill', d => d.st.color).attr('font-size', 10)
      .attr('font-family', "'IBM Plex Mono',monospace")
      .text(d => d.st.label);

    // ── แพเนลย่อย ──
    if (pane.vol) {{
      const p = pane.vol, mx = d3.max(vis, d => d[5] || 0) || 1;
      p.y.domain([0, mx * 1.05]);
      paneAxis(p, p.y.ticks(2).slice(1), fmtV);
      const bw = Math.max(1, Math.min(18, (zx(1) - zx(0)) * 0.68));
      p.body.selectAll('g.c-vol').data([0]).join('g').attr('class', 'c-vol')
        .selectAll('rect').data(vis, d => d[0]).join('rect')
        .attr('x', (d, k) => zx(i0 + k) - bw / 2).attr('width', bw)
        .attr('y', d => p.y(d[5] || 0)).attr('height', d => p.h - p.y(d[5] || 0))
        .attr('fill', d => d[4] >= d[1] ? 'var(--up)' : 'var(--down)');
    }}
    if (pane.rsi) {{
      const p = pane.rsi;
      p.y.domain([0, 100]);
      paneAxis(p, [30, 70], d => d);
      p.body.selectAll('path.ind').data([0]).join('path').attr('class', 'ind')
        .attr('fill', 'none').attr('stroke', IND_MAP.rsi.c).attr('stroke-width', 1.4)
        .attr('d', lineGen(zx, p.y, S.rsi.slice(i0, i1 + 1), i0));
    }}
    if (pane.macd) {{
      const p = pane.macd;
      const seg = k => S.macd[k].slice(i0, i1 + 1).filter(v => v != null);
      const ex = d3.max([...seg('m'), ...seg('sig'), ...seg('h')].map(Math.abs)) || 1;
      p.y.domain([-ex * 1.15, ex * 1.15]);
      paneAxis(p, [0], d => d);
      const bw = Math.max(1, Math.min(12, (zx(1) - zx(0)) * 0.6));
      p.body.selectAll('g.c-hist').data([0]).join('g').attr('class', 'c-hist')
        .selectAll('rect').data(vis.map((_, k) => S.macd.h[i0 + k]), (d, k) => k)
        .join('rect')
        .attr('x', (d, k) => zx(i0 + k) - bw / 2).attr('width', bw)
        .attr('y', d => d == null ? 0 : p.y(Math.max(0, d)))
        .attr('height', d => d == null ? 0 : Math.abs(p.y(d) - p.y(0)))
        .attr('fill', d => d >= 0 ? 'var(--up)' : 'var(--down)').attr('opacity', .55);
      const two = [['m', IND_MAP.macd.c], ['sig', '#F5A524']];
      p.body.selectAll('path.ind').data(two, d => d[0]).join('path').attr('class', 'ind')
        .attr('fill', 'none').attr('stroke', d => d[1]).attr('stroke-width', 1.3)
        .attr('d', d => lineGen(zx, p.y, S.macd[d[0]].slice(i0, i1 + 1), i0));
    }}
    if (pane.atr) {{
      const p = pane.atr;
      const seg = S.atr.slice(i0, i1 + 1).filter(v => v != null);
      p.y.domain([d3.min(seg) * 0.9 || 0, d3.max(seg) * 1.1 || 1]);
      paneAxis(p, p.y.ticks(2), fmtP);
      p.body.selectAll('path.ind').data([0]).join('path').attr('class', 'ind')
        .attr('fill', 'none').attr('stroke', IND_MAP.atr.c).attr('stroke-width', 1.3)
        .attr('d', lineGen(zx, p.y, seg.length ? S.atr.slice(i0, i1 + 1) : [], i0));
    }}

    // ── ราคาปิดล่าสุดติดป้ายที่แกนขวา ──
    const last = rows[rows.length - 1][4];
    const inView = last >= main.y.domain()[0] && last <= main.y.domain()[1];
    tagNow.style('display', inView ? null : 'none')
      .attr('transform', `translate(0,${{main.y(last)}})`);
    tagNow.select('text').text(fmtP(last));

    legend(i1);
    svg.node().__view = {{zx, i0, i1}};
  }}

  // ป้ายชื่อ + ค่าอินดิเคเตอร์ที่ตำแหน่งเมาส์ (ถ้าไม่ชี้ ใช้แท่งล่าสุด)
  function legend(i){{
    const items = [{{t: chCur + ' · ' + chTf, c: 'var(--ink)'}}];
    OVER.forEach(([k, s]) => {{
      if (!has(k) || !S[s] || s === 'bbl' || s === 'bbm' || s === 'dcl') return;
      const v = S[s][i];
      if (v == null) return;
      const nm = s === 'bbu' ? 'BB 20' : s === 'dcu' ? 'DC 20' : IND_MAP[k].n;
      items.push({{t: nm + ' ' + fmtP(v), c: IND_MAP[k].c}});
    }});
    if (has('sr')) items.push({{t: 'S/R', c: IND_MAP.sr.c}});
    if (has('regr')) items.push({{t: 'REGR ±2σ', c: IND_MAP.regr.c}});
    if (lastTM) {{
      const c = lastTM.cur;
      items.push({{
        t: (c.up ? '▲ ขาขึ้น ' : '▼ ขาลง ') +
           (c.pct >= 0 ? '+' : '') + c.pct.toFixed(1) + '% · ' + c.bars + ' แท่ง · ' +
           lastTM.structure,
        c: c.up ? 'var(--up)' : 'var(--down)'}});
    }}
    main.leg.selectAll('text').data(items).join('text')
      .attr('x', (d, k) => k === 0 ? 0 : null)
      .attr('fill', d => d.c).attr('y', 0)
      .text(d => d.t)
      .attr('x', function(d, k){{
        let x = 0;
        for (let j = 0; j < k; j++) x += this.parentNode.children[j].getComputedTextLength() + 11;
        return x;
      }});
    if (pane.rsi) pane.rsi.leg.selectAll('text').data([S.rsi[i]]).join('text')
      .attr('fill', IND_MAP.rsi.c).text(d => 'RSI 14  ' + (d == null ? '—' : d.toFixed(1)));
    if (pane.macd) pane.macd.leg.selectAll('text').data([S.macd.m[i]]).join('text')
      .attr('fill', IND_MAP.macd.c)
      .text(d => 'MACD  ' + (d == null ? '—' : fmtP(d)) +
        '   SIGNAL ' + (S.macd.sig[i] == null ? '—' : fmtP(S.macd.sig[i])));
    if (pane.atr) pane.atr.leg.selectAll('text').data([S.atr[i]]).join('text')
      .attr('fill', IND_MAP.atr.c).text(d => 'ATR 14  ' + (d == null ? '—' : fmtP(d)));
    if (pane.vol) pane.vol.leg.selectAll('text').data([rows[i][5] || 0]).join('text')
      .attr('fill', 'var(--mute)').text(d => 'VOL  ' + (d ? fmtV(d) : '—'));
  }}

  chZoom = d3.zoom().scaleExtent([1, 40])
    .translateExtent([[0, 0], [iw, innerH]]).extent([[0, 0], [iw, innerH]])
    .on('zoom', ev => draw(ev.transform));
  svg.call(chZoom).on('dblclick.zoom', null);
  svg.on('dblclick', () => svg.call(chZoom.transform, d3.zoomIdentity));
  draw(d3.zoomTransform(svg.node()));
  renderCalc();

  const out = document.getElementById('creadout');
  svg.on('mousemove', ev => {{
    const v = svg.node().__view; if (!v) return;
    const [px, py] = d3.pointer(ev, svg.node());
    const i = Math.max(v.i0, Math.min(v.i1, Math.round(v.zx.invert(px - m.l))));
    const r = rows[i]; if (!r) return;
    crossX.style('display', null).attr('x1', v.zx(i)).attr('x2', v.zx(i));
    const ly = py - m.t;
    if (ly >= 0 && ly <= mainH) {{
      crossY.style('display', null).attr('y1', ly).attr('y2', ly);
      tagY.style('display', null).attr('transform', `translate(0,${{ly}})`);
      tagY.select('text').text(fmtP(main.y.invert(ly)));
    }} else {{ crossY.style('display', 'none'); tagY.style('display', 'none'); }}
    legend(i);
    const chg = ((r[4] / r[1] - 1) * 100);
    out.innerHTML = `<span>${{fmtT(r[0])}}</span><span>O <b>${{fmtP(r[1])}}</b></span>` +
      `<span>H <b>${{fmtP(r[2])}}</b></span><span>L <b>${{fmtP(r[3])}}</b></span>` +
      `<span>C <b>${{fmtP(r[4])}}</b></span>` +
      `<span class="${{chg >= 0 ? 'up' : 'down'}}">${{chg >= 0 ? '+' : ''}}${{chg.toFixed(2)}}%</span>` +
      (r[5] ? `<span>VOL <b>${{fmtV(r[5])}}</b></span>` : '');
  }}).on('mouseleave', () => {{
    crossX.style('display', 'none'); crossY.style('display', 'none');
    tagY.style('display', 'none'); out.innerHTML = '';
    const v = svg.node().__view; if (v) legend(v.i1);
  }});
}}

document.getElementById('cmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'cmodal') closeCharts();
}});
addEventListener('keydown', ev => {{
  const mm = document.getElementById('metmodal');
  const metOpen = mm && !mm.hidden;
  const fm = document.getElementById('finmodal');
  const finOpen = fm && !fm.hidden;
  // ชั้นฉบับเต็มอยู่บนสุด ลูกศรจึงต้องเลื่อนหมวด ไม่ใช่เปลี่ยนหุ้นข้างใต้
  if (metOpen && ev.key === 'ArrowLeft')  {{ metNav(-1); return; }}
  if (metOpen && ev.key === 'ArrowRight') {{ metNav(1); return; }}
  if (finOpen && ev.key === 'ArrowLeft')  {{ finNav(-1); return; }}
  if (finOpen && ev.key === 'ArrowRight') {{ finNav(1); return; }}
  if (ev.key !== 'Escape') return;
  if (metOpen) {{ closeMetric(); return; }}
  if (finOpen) {{ closeFinancials(); return; }}   // ปิดชั้นงบการเงินก่อน ไม่ปิดกราฟข้างใต้ไปด้วย
  // ย่อกราฟกลับก่อน ยังไม่ปิดหน้าต่าง — กด Esc ครั้งเดียวไม่ควรหลุดออกไปเลยสองชั้น
  if (!document.getElementById('cmodal').hidden && chartFullOpen()) {{
    toggleChartFull(false); return;
  }}
  const lm = document.getElementById('lmodal');
  if (!document.getElementById('cmodal').hidden) closeCharts();
  if (lm && !lm.hidden) closeLive();
  if (document.getElementById('navpanel').classList.contains('open')) toggleNav(false);
}});

function setScope(s){{
  document.querySelectorAll('.tab').forEach(t => {{
    const on = t.dataset.scope === s;
    t.classList.toggle('active', on);
    if (on) document.getElementById('navbar-now').textContent =
      t.textContent.replace(/\\d+$/, '').trim();
  }});
  document.querySelectorAll('.scope-group').forEach(g =>
    g.hidden = !(s === 'all' || g.dataset.scope === s));
  try {{ sessionStorage.setItem('scope', s); }} catch(e) {{}}
}}

// ── เมนูแถบซ้าย / แผนที่ / พับหัวข้อกลุ่มข่าว ─────────────
function toggleNav(force){{
  const p = document.getElementById('navpanel');
  const dim = document.getElementById('navdim');
  const b = document.querySelector('.burger');
  const open = force !== undefined ? force : !p.classList.contains('open');
  p.classList.toggle('open', open);
  dim.classList.toggle('on', open);
  b.setAttribute('aria-expanded', open ? 'true' : 'false');
  // อย่าคืน scroll ให้หน้าหลัก ถ้ายังมีหน้าต่างเต็มจอเปิดอยู่
  if (open) document.body.style.overflow = 'hidden';
  else if (!document.querySelector('.tmodal:not([hidden])')) document.body.style.overflow = '';
}}
// เลือกเมนูแล้วปิดแถบให้เอง (ทำงานหลัง onclick ของปุ่มเสมอ)
document.getElementById('tabs').addEventListener('click', ev => {{
  if (ev.target.closest('.tab')) toggleNav(false);
}});

function openLive(){{
  const m = document.getElementById('lmodal');
  if (!m) return;                     // รอบไหนไม่มีข่าวสด ก็ไม่มีหน้านี้
  m.hidden = false;
  document.body.style.overflow = 'hidden';
}}
function closeLive(){{
  const m = document.getElementById('lmodal');
  if (!m) return;
  m.hidden = true;
  document.body.style.overflow = '';
}}
document.getElementById('lmodal')?.addEventListener('click', ev => {{
  if (ev.target.id === 'lmodal') closeLive();
}});

// ── สัดส่วนข่าวความขัดแย้ง (ลูกโลกมุมขวาบนของแผนที่) ─────────
// ตัวเลขนี้คือ "ข่าวความขัดแย้งคิดเป็นกี่ % ของข่าวทั้งหมดใน 24 ชม." ซึ่งนับได้จริง
// ไม่ใช่ดัชนีโอกาสเกิดสงครามโลก — เขียนกำกับไว้ในแผงให้ชัด ไม่ให้เข้าใจผิด
const PULSE = window.__PULSE__ || {{pct: 0, n: 0, total: 0, stories: [], places: []}};
function paintTension(){{
  const el = document.getElementById('tension-n');
  if (!el) return;
  el.textContent = PULSE.pct + '%';
  el.classList.toggle('hot', PULSE.pct >= 25);
  el.classList.toggle('warm', PULSE.pct >= 12 && PULSE.pct < 25);
  document.getElementById('tension-btn').title =
    `${{PULSE.n}} of ${{PULSE.total}} stories in the last 24h mention armed conflict ` +
    `or military tension — a share of coverage, not a forecast`;
}}
function toggleTension(){{
  const p = document.getElementById('tension-panel');
  const b = document.getElementById('tension-btn');
  const open = p.hidden;
  if (open && !p.dataset.built) {{
    const rows = (PULSE.stories || []).map(s => {{
      const img = s.image
        ? `<img src="${{esc(s.image)}}" loading="lazy" alt="" onerror="this.remove()">` : '';
      const where = s.place ? esc(s.place) + ' · ' : '';
      return `<a class="tension-row" href="${{esc(s.link)}}" target="_blank" rel="noopener">` +
        `${{img}}<span><span class="tension-row-t">${{esc(s.title)}}</span>` +
        `<span class="tension-row-m">${{where}}${{esc(s.source)}} · ${{esc(s.age)}}</span></span></a>`;
    }}).join('');
    const places = (PULSE.places || []).length
      ? ` Places named most often: <b>${{PULSE.places.map(esc).join(', ')}}</b>.` : '';
    p.innerHTML =
      `<p class="tension-note"><b>${{PULSE.n}} of ${{PULSE.total}}</b> stories in the last 24 hours ` +
      `mention armed conflict or military tension — <b>${{PULSE.pct}}%</b> of current coverage.` +
      places +
      ` This counts how much of the news is about conflict right now. It is not a probability ` +
      `of war and not a forecast: 100% would mean every story in the feed was about conflict, ` +
      `not that a world war had begun. Matching is by keyword, so it will miss some stories ` +
      `and over-count others.</p>` +
      (rows ? `<div class="tension-list">${{rows}}</div>`
            : `<p class="tension-note">No conflict stories matched in this window.</p>`);
    p.dataset.built = '1';
  }}
  p.hidden = !open;
  b.setAttribute('aria-expanded', open ? 'true' : 'false');
}}

function openMap(){{
  document.getElementById('mmodal').hidden = false;
  document.body.style.overflow = 'hidden';
  paintTension();
  draw();                       // แผนที่เพิ่งมีขนาดตอนนี้ ต้องวาดใหม่
}}
function closeMap(){{
  document.getElementById('mmodal').hidden = true;
  document.body.style.overflow = '';
}}
document.getElementById('mmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'mmodal') closeMap();
}});

// ── ลากสลับลำดับแท็บได้เอง แล้วจำลำดับไว้ ────────────────
(() => {{
  const bar = document.getElementById('tabs');
  if (!bar) return;
  const KEY = 'tabOrder';
  const save = () => {{
    try {{ localStorage.setItem(KEY,
      JSON.stringify([...bar.children].map(t => t.dataset.id))); }} catch(e) {{}}
  }};
  try {{
    const saved = JSON.parse(localStorage.getItem(KEY) || '[]');
    saved.forEach(id => {{
      const el = bar.querySelector(`.tab[data-id="${{id}}"]`);
      if (el) bar.appendChild(el);          // เรียงตามที่เคยจัดไว้
    }});
  }} catch(e) {{}}

  let src = null;
  bar.addEventListener('dragstart', ev => {{
    src = ev.target.closest('.tab');
    if (!src) return;
    src.classList.add('dragging');
    ev.dataTransfer.effectAllowed = 'move';
    try {{ ev.dataTransfer.setData('text/plain', src.dataset.id); }} catch(e) {{}}
  }});
  bar.addEventListener('dragend', () => {{
    if (src) src.classList.remove('dragging');
    src = null; save();
  }});
  bar.addEventListener('dragover', ev => {{
    ev.preventDefault();
    const over = ev.target.closest('.tab');
    if (!over || !src || over === src) return;
    const r = over.getBoundingClientRect();
    const after = ev.clientY > r.top + r.height / 2;   // เมนูเรียงแนวตั้งแล้ว
    bar.insertBefore(src, after ? over.nextSibling : over);
  }});
  bar.addEventListener('drop', ev => ev.preventDefault());
}})();

// ── ลากจัดลำดับรายการโปรดในหน้ากราฟ (เฉพาะแท็บ FAVORITES) ──
// ผูก listener ไว้ที่กล่องแม่ครั้งเดียวตอนโหลดหน้า ไม่ใช่ทุกครั้งที่ renderAssetList
// วาดใหม่ — innerHTML ถูกแทนที่บ่อย แต่ตัวกล่องแม่เองไม่ได้ถูกสร้างใหม่ listener จึงอยู่รอด
(() => {{
  const list = document.getElementById('cmodal-list');
  if (!list) return;
  let src = null;
  list.addEventListener('dragstart', ev => {{
    src = ev.target.closest('.citem[draggable="true"]');
    if (!src) return;
    src.classList.add('dragging');
    ev.dataTransfer.effectAllowed = 'move';
    try {{ ev.dataTransfer.setData('text/plain', src.dataset.label); }} catch(e) {{}}
  }});
  list.addEventListener('dragend', () => {{
    if (src) {{ src.classList.remove('dragging'); saveFavOrder(); }}
    src = null;
  }});
  list.addEventListener('dragover', ev => {{
    const over = ev.target.closest('.citem');
    if (!over || !src || over === src) return;
    // ห้ามลากข้ามกลุ่ม THAILAND/GLOBAL — กลุ่มของแต่ละตัวมาจากข้อมูลจริง ลากข้ามแล้ว
    // วาดใหม่รอบหน้าจะกระโดดกลับกลุ่มเดิมทันที ดูเหมือนลากไม่ติด
    if (over.closest('.cfav-group') !== src.closest('.cfav-group')) return;
    ev.preventDefault();
    const r = over.getBoundingClientRect();
    const after = ev.clientY > r.top + r.height / 2;
    over.parentNode.insertBefore(src, after ? over.nextSibling : over);
  }});
  list.addEventListener('drop', ev => ev.preventDefault());
}})();

// จำแท็บที่เลือกไว้ ไม่ให้เด้งกลับตอนหน้ารีเฟรชอัตโนมัติ
(() => {{
  let s = 'all';
  try {{ s = sessionStorage.getItem('scope') || 'all'; }} catch(e) {{}}
  if (!document.querySelector(`.tab[data-scope="${{s}}"]`)) s = 'all';
  setScope(s);
}})();

// พอหน้าเว็บนิ่งแล้วค่อยแอบดึงงบการเงินของรายการโปรดมาพักไว้ — ต้องรอให้ข่าว รูป และกราฟ
// โหลดเสร็จก่อน ของพวกนั้นคือสิ่งที่ผู้ใช้เห็นทันที ส่วนงบเป็นของ "เผื่อกด" ยอมมาทีหลังได้
(() => {{
  const go = () => (window.requestIdleCallback || (f => setTimeout(f, 1200)))(prefetchFavFin);
  if (document.readyState === 'complete') go();
  else window.addEventListener('load', go, {{once: true}});
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
    btn.textContent = btn.classList.contains('poster-speak') ? '⏸' : '⏸ PLAYING…';
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

    print("เช็คช่องที่ถ่ายทอดสด...")
    streams = fetch_live_streams()
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

    charts, logos, fin_at, infl = {}, {}, None, {}
    if markets:
        print("ดึงข้อมูลพื้นฐาน...")
        fetch_fundamentals(markets)
        logos = fetch_logos()
        print("ดึงข้อมูลแท่งเทียน...")
        charts = build_charts(markets)
        print("ดึงงบการเงิน...")
        fin_labels, fin_at = fetch_financials()
        for label in fin_labels:
            if label in charts:
                charts[label]["f"] = True
        print("ดึงอัตราเงินเฟ้อ...")
        infl = fetch_inflation()
    print()

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(render(news, markets, charts, logos, streams, fin_at, infl))
    print(f"เสร็จ · index.html · {NOW.strftime('%H:%M')} น.")
