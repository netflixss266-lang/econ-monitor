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
""".split()


def universe_symbols():
    """(สัญลักษณ์ Yahoo, ชื่อที่แสดง, ตลาด) ของทุกตัวในเมนูค้นหา"""
    out = []
    seen = set()
    for label, sym, group in TICKERS:          # ตัวที่อยู่ในแถบราคาอยู่แล้ว
        if sym != THAI_GOLD and label not in seen:
            seen.add(label)
            out.append((sym, label, group))
    for t in UNIVERSE_TH:
        if t not in seen:
            seen.add(t)
            out.append((t + ".BK", t, "th"))
    for t in UNIVERSE_US:
        if t not in seen:
            seen.add(t)
            out.append((t, t.replace("-", "."), "intl"))
    return out


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


def build_charts(markets=None):
    """เขียนไฟล์แท่งเทียนแยกรายสินทรัพย์ไว้ให้หน้าเว็บโหลดตอนเปิดกราฟ

    แยกเป็นไฟล์ย่อยแทนที่จะฝังใน index.html เพราะข้อมูลรวมกันหลายสิบเมกะไบต์
    ชุดเต็มดึงทุก CHART_FULL_HOURS ชม. รอบระหว่างนั้นอัปเดตแค่กราฟรายวัน
    """
    os.makedirs(CHART_DIR, exist_ok=True)
    idx_path = f"{CHART_DIR}/index.json"
    cached = load_json(idx_path)
    if cached.get("index") and cached.get("at"):
        try:
            age = (NOW - datetime.fromisoformat(cached["at"])).total_seconds() / 3600
            if 0 <= age < CHART_FULL_HOURS:
                print(f"  ↻ ใช้ชุดกราฟเดิมที่ดึงมา {age:.1f} ชม.ที่แล้ว")
                return refresh_intraday(cached["index"])
        except Exception:
            pass

    uni = universe_symbols()
    jobs = [(sym, label, tf, rng, iv) for sym, label, _ in uni
            for tf, rng, iv in CHART_RANGES]

    def run(job):
        sym, label, tf, rng, iv = job
        try:
            return label, tf, fetch_candles(sym, rng, iv)
        except Exception:
            return label, tf, None

    frames = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        for label, tf, candles in pool.map(run, jobs):
            if candles:
                frames.setdefault(label, {})[tf] = candles

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
        save_json(f"{CHART_DIR}/{slug}.json",
                  {"label": label, "tf": tfs, "note": notes.get(label, "")})
        index[label] = {"s": slug, "g": group}
    total = sum(len(c) for tfs in frames.values() for c in tfs.values())
    print(f"  ✓ กราฟ {len(index)}/{len(uni)} สินทรัพย์ · {total:,} แท่งเทียน")
    if index:
        save_json(idx_path, {"at": NOW.isoformat(), "index": index})
    return index


# ─────────────────────────────────────────────────────────────
# งบการเงิน — รายได้/ค่าใช้จ่าย/กำไร/สินทรัพย์/หนี้สิน ย้อนหลังสูงสุด 4 ปี
# + รายไตรมาสของปีที่ดำเนินอยู่ ไว้ให้กดดูจากรายการโปรดในหน้ากราฟ
# ─────────────────────────────────────────────────────────────
FIN_DIR = "fin"
FIN_FULL_DAYS = 7      # งบเปลี่ยนแค่รายไตรมาส ไม่ต้องดึงถี่เท่ากราฟ

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
    """
    idx_path = f"{FIN_DIR}/index.json"
    cached = load_json(idx_path)
    if cached.get("labels") and cached.get("at"):
        try:
            age_days = (NOW - datetime.fromisoformat(cached["at"])).total_seconds() / 86400
            if 0 <= age_days < FIN_FULL_DAYS:
                print(f"  ↻ ใช้งบการเงินเดิมที่ดึงมา {age_days:.1f} วันที่แล้ว")
                return set(cached["labels"])
        except Exception:
            pass

    sess, crumb = yahoo_session()
    if not sess:
        return set()
    uni = universe_symbols()

    def one(job):
        sym, label, _ = job
        try:
            r = sess.get(
                "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/"
                f"v1/finance/timeseries/{sym}",
                params={"symbol": sym, "type": ",".join(_FIN_TYPES),
                        "period1": "1000000000", "period2": str(int(NOW.timestamp()) + 86400),
                        "crumb": crumb}, timeout=15)
            if r.status_code != 200:
                return label, None
            result = r.json().get("timeseries", {}).get("result")
            periods = _parse_fin_periods(result)
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
    save_json(idx_path, {"at": NOW.isoformat(), "labels": sorted(got)})
    return got


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
         .html(`<strong>${d.place}</strong><span>${d.total} stories · Econ ${d.econ} · Politics ${d.poli} · Business ${d.biz} · Env ${d.env}</span>`);
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


def render(news, markets, charts=None, logos=None, streams=None):
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
      <button class="speak poster-speak" type="button" title="Listen" {speak_attrs(it)}>🔊</button>
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
      <a class="btn btn-main" href="{html.escape(it['link'])}" target="_blank" rel="noopener">▶ READ FULL</a>
      <button class="btn btn-ghost speak" type="button" {speak_attrs(it)}>🔊 LISTEN</button>
    </div>
  </div>
</section>"""

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

    def row_section(cat, items, rid, label=None, badge=""):
        if not items:
            return ""          # ไม่ต้องโชว์แถวเปล่า
        label = label or CAT_LABELS[cat]
        body = "".join(poster(i) for i in items)
        return f"""<section class="row">
  <div class="row-head">
    <h2>{cat_icon(cat)}{label}{badge}<span class="row-n">{len(items)}</span></h2>
    <div class="row-tools">
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',-1)" aria-label="Scroll left">‹</button>
      <button class="row-nav" type="button" onclick="scrollRow('{rid}',1)" aria-label="Scroll right">›</button>
    </div>
  </div>
  <div class="row-track" id="{rid}">{body}</div>
</section>"""

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

    heroes = "".join(
        hero(g["top"], sc, sc == primary_scope)
        for sc, _ in SCOPES if (g := groups[sc])["top"]
    )

    def scope_block(sc, label, rows, kind="cat"):
        """kind: live = ป้าย LIVE เต้น · news = ป้าย NEWS · cat = ชื่อกลุ่มเต็ม"""
        if not rows:
            return ""          # ไม่มีข่าว ก็ไม่ต้องมีหัวข้อ
        flag = "TH" if sc == "th" else "INTL"
        if kind == "live":
            title = f'<span class="live live-dot">LIVE</span><span class="live-scope">{label}</span>'
        elif kind == "news":
            title = f'<span class="tag-news">NEWS</span><span class="live-scope">{label}</span>'
        else:
            title = f'{label}<span class="scope-flag">{flag}</span>'
        cls = " live-group" if kind in ("live", "news") else ""
        return f"""<div class="scope-group{cls}" data-scope="{sc}">
  <h2 class="scope-title">{title}</h2>
  {rows}
</div>"""

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

    live_tv_block = (f"""<div class="scope-group live-group" data-scope="all">
  <h2 class="scope-title"><span class="live live-dot">LIVE</span><span class="live-scope">BROADCASTS</span></h2>
  {live_tv_row('row-live-tv')}
</div>""" if streams else "")

    # LIVE = เฉพาะข่าวที่เพิ่งออกจริงๆ ในกรอบ LIVE_WINDOW_MIN นาที
    live_blocks = live_tv_block + "".join(
        scope_block(sc, lb, row_section("mixed", groups[sc]["live"], f"row-{sc}-live",
                                        "JUST IN", '<span class="live">NOW</span>'),
                    kind="live")
        for sc, lb in SCOPES)

    # ข่าวล่าสุดแยกออกมาเป็นก้อน NEWS ของตัวเอง (แผนที่/แถบราคาคั่นก่อนข่าวรายหมวด)
    latest_blocks = "".join(
        scope_block(sc, lb, row_section("mixed", groups[sc]["latest"], f"row-{sc}-latest",
                                        "LATEST"),
                    kind="news")
        for sc, lb in SCOPES)

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

    def cat_tabs_block(sc, label):
        """หมวดข่าวเป็นแท็บเดียวสลับได้ แทนการเรียงเป็น 4 แถวเต็มจอ — ลดความยาวหน้าแรกลงครึ่งหนึ่ง"""
        present = [c for c in CAT_NAMES if groups[sc]["cats"][c]]
        if not present:
            return ""
        flag = "TH" if sc == "th" else "INTL"
        tabs = "".join(
            f'<button class="cat-tab{" on" if i == 0 else ""}" type="button" '
            f'data-cat="{c}" onclick="switchCat(this,\'{sc}\',\'{c}\')">'
            f'{cat_icon(c, "ci-sm")}{CAT_LABELS[c]}'
            f'<span class="cat-n">{len(groups[sc]["cats"][c])}</span></button>'
            for i, c in enumerate(present))
        panels = "".join(
            f'<div class="cat-panel" data-cat="{c}"{"" if i == 0 else " hidden"}>'
            f'{row_section(c, groups[sc]["cats"][c], f"row-{sc}-{c}")}</div>'
            for i, c in enumerate(present))
        return f"""<div class="scope-group" data-scope="{sc}">
  <h2 class="scope-title">{label}<span class="scope-flag">{flag}</span></h2>
  <div class="cat-tabs" role="tablist">{tabs}</div>
  <div class="cat-panels">{panels}</div>
</div>"""

    category_blocks = "".join(cat_tabs_block(sc, lb) for sc, lb in SCOPES)

    next_run = (NOW + timedelta(minutes=REBUILD_MIN)).strftime("%H:%M")
    markers_json = json.dumps(markers, ensure_ascii=False)
    icons_json = json.dumps({c: cat_icon(c, "ci-sm") for c in CAT_NAMES}, ensure_ascii=False)
    tnews_json = json.dumps(
        {m["label"]: {"price": m["price"], "pct": m["pct_str"], "pctv": round(m["pct"], 4),
                      "group": m.get("group", "intl"),
                      "dir": "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat"),
                      "fund": m.get("fund") or {}, "news": m.get("news") or []}
         for m in markets}, ensure_ascii=False)
    charts_json = json.dumps(charts or {}, ensure_ascii=False)
    logos_json = json.dumps(logos or {}, ensure_ascii=False)
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
body{{background:var(--bg);color:var(--ink);
  font-family:'Noto Serif Thai',Georgia,'Times New Roman',serif;
  font-size:15px;line-height:1.55;padding:20px;max-width:1560px;margin:0 auto;
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
.cmodal-title h3{{font-size:1.05rem;font-weight:700}}
.cmodal-price{{display:flex;gap:9px;font-family:'IBM Plex Mono',monospace;font-size:.78rem}}
.tfbar{{display:flex;gap:4px;flex:1;flex-wrap:wrap}}
.tfbtn{{padding:5px 11px;border-radius:2px;cursor:pointer;font-family:'IBM Plex Mono',monospace;
  font-size:.72rem;color:var(--mute);background:transparent;border:1px solid var(--line)}}
.tfbtn:hover{{color:var(--ink)}}
.tfbtn.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:600}}
.cmodal-body{{flex:1;display:flex;min-height:0}}
.cmodal-pick{{width:210px;flex:none;display:flex;flex-direction:column;min-height:0}}
.csearch{{margin:8px 10px;width:auto;flex:none}}
.cmodal-list{{flex:1;overflow-y:auto;padding:2px 0}}
.cmodal-list .cnone{{padding:16px 14px;color:var(--dim);font-size:.76rem}}
.cgroup{{padding:9px 14px 5px;font-family:'IBM Plex Mono',monospace;font-size:.62rem;
  letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}}
.citem{{display:flex;align-items:center;gap:8px;width:100%;padding:7px 10px 7px 14px;
  cursor:pointer;background:none;border:0;color:var(--mute);font-family:inherit;
  font-size:.79rem;text-align:left}}
.citem .cname{{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
/* แถบเลือกว่าจะโชว์เฉพาะตัวโปรด หรือทั้งตลาด */
.cfav-bar{{display:flex;gap:4px;padding:9px 10px 0}}
.cfav-tab{{flex:1;padding:6px 8px;border-radius:2px;cursor:pointer;
  font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.06em;
  color:var(--mute);background:transparent;border:1px solid var(--line)}}
.cfav-tab:hover{{color:var(--ink)}}
.cfav-tab.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:700}}
.cpct{{font-family:'IBM Plex Mono',monospace;font-size:.7rem}}
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
.cctrl{{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end;margin-left:auto}}
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

/* ── แถบเครื่องมือเทคนิคฝั่งซ้าย (อย่างโปรแกรมเทรด) ────────── */
.crail{{position:relative;flex:none;width:46px;display:flex;flex-direction:column;
  align-items:center;gap:5px;padding:9px 0;background:var(--panel2);
  border-left:1px solid var(--line);border-right:1px solid var(--line)}}
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

/* ── ปุ่ม + หน้าต่างงบการเงิน ────────────────────────────── */
.fin-btn{{display:inline-flex;align-items:center;gap:7px;padding:8px 13px;
  margin-left:auto;flex:none;cursor:pointer;border-radius:2px;
  font-family:'IBM Plex Mono',monospace;font-size:.7rem;letter-spacing:.08em;
  font-weight:600;color:var(--mute);background:var(--panel2);border:1px solid var(--line)}}
.fin-btn:hover{{color:var(--ink);border-color:var(--brass)}}
.fin-btn svg{{width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:2;
  stroke-linecap:round;stroke-linejoin:round}}
.fin-tabs{{display:flex;gap:4px;flex:none}}
.fin-tab{{padding:8px 14px;border-radius:2px;cursor:pointer;
  font-family:'IBM Plex Mono',monospace;font-size:.68rem;letter-spacing:.08em;
  color:var(--mute);background:transparent;border:1px solid var(--line)}}
.fin-tab:hover{{color:var(--ink)}}
.fin-tab.on{{color:#0A0E1A;background:var(--brass);border-color:var(--brass);font-weight:700}}
.fin-body{{flex:1;overflow:auto;padding:16px 18px}}
.fin-note{{padding:9px 18px;font-size:.65rem;line-height:1.5;color:var(--dim);
  border-top:1px solid var(--line);background:var(--panel2)}}
.fin-empty{{display:grid;place-items:center;height:100%;color:var(--mute);
  font-size:.85rem;text-align:center;gap:8px;padding:30px}}
.fin-table{{border-collapse:collapse;font-family:'IBM Plex Mono',monospace;
  font-size:.78rem;font-variant-numeric:tabular-nums}}
.fin-table th,.fin-table td{{padding:9px 16px;text-align:right;white-space:nowrap;
  border-bottom:1px solid var(--line)}}
.fin-table thead th{{position:sticky;top:0;background:var(--panel);z-index:2;
  color:var(--dim);font-size:.62rem;letter-spacing:.06em;font-weight:600;
  border-bottom:1px solid var(--line2)}}
.fin-table th:first-child,.fin-table td:first-child{{position:sticky;left:0;
  background:var(--panel);z-index:1;text-align:left;font-family:'Noto Serif Thai',Georgia,serif;
  font-size:.82rem;color:var(--mute);white-space:normal;min-width:150px}}
.fin-table thead th:first-child{{z-index:3}}
.fin-table tbody tr:hover td{{background:var(--hover)}}
.fin-table tbody tr:hover td:first-child{{background:var(--hover)}}
.fin-table .fin-val{{color:var(--ink);font-weight:500}}
.fin-table .fin-na{{color:var(--dim)}}
.fin-table .fin-delta{{display:block;font-size:.64rem;font-weight:400;margin-top:2px}}
.fin-table .fin-delta.up{{color:var(--up)}}
.fin-table .fin-delta.down{{color:var(--down)}}
.fin-table .fin-delta.flat{{color:var(--dim)}}
.fin-sec{{background:var(--panel2)}}
.fin-sec td{{padding:7px 16px;font-family:'IBM Plex Mono',monospace;font-size:.6rem;
  letter-spacing:.14em;text-transform:uppercase;color:var(--brass);font-weight:600;
  border-bottom:1px solid var(--line2)}}
@media(max-width:700px){{
  .cmodal-head{{position:relative}}
  .fin-btn{{margin-left:0}}
  .fin-tabs{{width:100%;order:3}}
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
.calc-k{{font-size:.74rem;color:var(--mute)}}
.calc-k small{{display:block;font-size:.6rem;color:var(--dim);font-family:'IBM Plex Mono',monospace}}
.calc-v{{font-family:'IBM Plex Mono',monospace;font-size:.86rem;font-weight:500;
  text-align:right;white-space:nowrap}}
.calc-v small{{display:block;font-size:.6rem;font-weight:400;color:var(--dim)}}
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

/* ── เรื่องเด่น (billboard) — ภาพพิมพ์เต็มหน้า ตัวหนังสือสีกระดาษทับบนภาพ ── */
.hero{{position:relative;display:flex;align-items:flex-end;overflow:hidden;
  color:#F6F1E3;border:1px solid var(--ink);
  border-radius:2px;margin-bottom:30px;min-height:clamp(320px,44vw,510px)}}
.hero-img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover}}
.hero-scrim{{position:absolute;inset:0;
  background:linear-gradient(90deg,rgba(5,7,13,.95) 0%,rgba(5,7,13,.78) 40%,rgba(5,7,13,.28) 74%),
             linear-gradient(0deg,rgba(5,7,13,.96) 0%,rgba(5,7,13,0) 58%)}}
.hero-body{{position:relative;padding:clamp(18px,3vw,40px);max-width:780px}}
.hero-badge{{display:flex;align-items:center;flex-wrap:wrap;gap:7px;
  font-family:'IBM Plex Mono',monospace;font-size:.71rem;color:#DCD2BB;
  text-transform:uppercase;letter-spacing:.06em}}
.hero-sep{{color:#A99C82}}
.hero-title{{font-size:clamp(1.35rem,3.1vw,2.55rem);font-weight:700;line-height:1.18;
  margin:11px 0 12px;text-shadow:0 2px 20px rgba(0,0,0,.65)}}
.hero-sum{{color:#C2CCDD;font-size:.92rem;line-height:1.6;max-width:640px;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}}
.hero-actions{{display:flex;flex-wrap:wrap;gap:10px;margin-top:17px}}
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

/* หมวดข่าวเป็นแท็บเดียวสลับได้ แทนการเรียง 4 แถวเต็มจอ */
.cat-tabs{{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}}
.cat-tab{{display:inline-flex;align-items:center;gap:7px;padding:8px 14px;
  border-radius:2px;cursor:pointer;font-family:inherit;font-size:.83rem;font-weight:600;
  color:var(--mute);background:var(--panel);border:1px solid var(--line)}}
.cat-tab:hover{{color:var(--ink);border-color:var(--line2)}}
.cat-tab.on{{color:var(--ink);background:var(--sel);border-color:var(--line2)}}
.cat-tab .cicon{{width:15px;height:15px}}
.cat-n{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;font-weight:400;color:var(--dim)}}
.cat-panel .row-head{{border-bottom:0;padding-bottom:0}}
.cat-panel .row-head h2{{display:none}}
.cat-panel .row-head{{justify-content:flex-end}}
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

.poster{{position:relative;flex:0 0 288px;scroll-snap-align:start;background:var(--panel);
  border:1px solid var(--line);border-radius:2px;overflow:hidden;
  transition:transform .28s cubic-bezier(.2,.7,.3,1),box-shadow .28s,border-color .28s}}
.poster:hover{{transform:scale(1.07);z-index:3;border-color:var(--line2);
  box-shadow:0 18px 42px rgba(0,0,0,.62)}}
.poster.hidden{{display:none}}
.poster-img{{position:relative;display:grid;place-items:center;aspect-ratio:16/9}}
.poster-img img{{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}}
.poster-img .ci-lg{{width:42px;height:42px;opacity:.45}}
.poster-cat{{position:absolute;left:9px;top:9px;z-index:2;display:grid;place-items:center;
  width:24px;height:24px;border-radius:2px;background:rgba(5,7,13,.72)}}
.poster-speak{{position:absolute;right:9px;top:9px;z-index:2;width:26px;height:26px;padding:0;
  border-radius:2px;background:rgba(5,7,13,.72);opacity:0;transition:opacity .2s}}
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
body.searching .cat-panel[hidden]{{display:block!important}}
body.searching .scope-group[hidden]{{display:block!important}}
body.searching .scope-group.folded .row{{display:block!important}}
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

/* หัวข้อกลุ่มข่าว พับเก็บได้ */
.scope-title{{cursor:pointer;user-select:none}}
.scope-caret{{width:14px;height:14px;flex:none;fill:none;stroke:currentColor;
  stroke-width:2.4;stroke-linecap:round;stroke-linejoin:round;color:var(--dim);
  transition:transform .2s}}
.scope-group.folded .scope-caret{{transform:rotate(-90deg)}}
.scope-group.folded .row,.scope-group.folded .cat-tabs{{display:none}}
.live-scope{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;letter-spacing:.1em;
  color:var(--mute);font-weight:500}}
.tag-news{{font-family:'IBM Plex Mono',monospace;font-size:.6rem;letter-spacing:.1em;
  font-weight:700;color:var(--cream);background:#1D2739;border:1px solid #2C3548;
  border-radius:2px;padding:3px 9px}}
.live-group .scope-title{{font-size:.9rem}}
.tab-live{{color:var(--down)}}
.live-dot-sm{{width:7px;height:7px;border-radius:50%;background:var(--down);
  box-shadow:0 0 0 0 rgba(229,72,77,.55);animation:livePulse 2.2s infinite}}
.live-list{{flex:1;overflow-y:auto}}
.live-list .cnews-row{{padding:11px 16px}}
.tab-n{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;font-weight:400;
  color:var(--dim);margin-left:6px}}
.scope-title{{display:flex;align-items:center;gap:10px;font-size:1.18rem;font-weight:700;
  margin:6px 0 14px}}
.scope-title::after{{content:"";flex:1;height:1px;background:var(--line)}}
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
.row-head h2,.scope-title,.panel-head h2,.cmodal-title h3,.tmodal-head h3,
.cnews-head,.hd-top h4,.hero-title{{
  font-family:'Playfair Display','Noto Serif Thai',Georgia,serif}}
.row-head h2{{letter-spacing:.06em;text-transform:uppercase;font-size:1rem}}
.scope-title{{letter-spacing:.1em;text-transform:uppercase}}
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
    </div>
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
        <div class="cmodal-price"><span id="cmodal-p"></span><span id="cmodal-c"></span></div>
        <div class="cmodal-note" id="cnote" hidden></div>
      </div>
      <button class="fin-btn" type="button" id="fin-btn" onclick="openFinancials()" hidden>
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 19h16M7 19V9M12 19V5M17 19v-7"/></svg>
        FINANCIALS</button>
    </div>

    <div class="tickers cmodal-tape">
      {ticker_row("th", "THAI")}
      {ticker_row("intl", "GLOBAL")}
    </div>
    <div class="cmodal-body">
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
      <!-- แถบเครื่องมือเทคนิค ยื่นออกมาจากขอบซ้ายของกราฟ -->
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
      <div class="cmodal-chart">
        <div id="cchart"></div>
        <div class="cbottom">
          <div id="creadout" class="creadout"></div>
          <div class="cctrl">
            <div class="tfbar" id="cmodal-tf"></div>
            <div class="tfbar ctype">
              <button class="tfbtn on" type="button" data-ct="candle" onclick="pickType('candle')">CANDLES</button>
              <button class="tfbtn" type="button" data-ct="line" onclick="pickType('line')">LINE</button>
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
      <div class="cmodal-title">
        <h3 id="fin-name">—</h3>
        <div class="cmodal-price"><span id="fin-currency"></span></div>
      </div>
      <div class="fin-tabs" role="tablist">
        <button class="fin-tab on" type="button" data-span="annual"
                onclick="pickFinSpan('annual')">ANNUAL</button>
        <button class="fin-tab" type="button" data-span="quarterly"
                onclick="pickFinSpan('quarterly')">QUARTERLY · THIS YEAR</button>
      </div>
    </div>
    <div class="fin-body" id="fin-body"><div class="cempty">Loading…</div></div>
    <p class="fin-note">Figures are company-reported financial statements sourced from Yahoo
      Finance, up to the last 4 fiscal years / most recent reported quarters — banks and
      insurers often show "—" for cost of revenue / gross profit, which doesn't apply to
      their business model. Indicators only, not investment advice.</p>
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

{heroes}

{live_blocks}

{latest_blocks}

{category_blocks}

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
window.__LOGOS__ = {logos_json};</script>
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
  document.querySelectorAll('.row').forEach(row => {{
    if (row.closest('#cmodal')) return;      // ไม่ยุ่งกับหน้ากราฟ
    let any = false;
    row.querySelectorAll('.poster').forEach(p => {{
      const hit = !q || p.textContent.toLowerCase().includes(q);
      p.classList.toggle('hidden', !hit);
      if (hit) any = true;
    }});
    row.classList.toggle('no-match', !!q && !any);
    if (any) anySite = true;
  }});
  document.getElementById('gsearch-empty').hidden = !q || anySite;
}}

// ── สลับแท็บหมวดข่าว (Economy/Politics/Business/Environment) ──
function switchCat(btn, sc, cat){{
  const group = btn.closest('.scope-group');
  group.querySelectorAll('.cat-tab').forEach(b => b.classList.toggle('on', b === btn));
  group.querySelectorAll('.cat-panel').forEach(p => p.hidden = p.dataset.cat !== cat);
  try {{ sessionStorage.setItem('cat-' + sc, cat); }} catch(e) {{}}
}}
// จำหมวดที่เลือกไว้ล่าสุดของแต่ละฝั่ง ไม่ให้เด้งกลับตอนหน้ารีเฟรชอัตโนมัติ
document.querySelectorAll('.scope-group').forEach(group => {{
  const tabs = group.querySelectorAll('.cat-tab');
  if (!tabs.length) return;
  const sc = group.dataset.scope;
  let want = null;
  try {{ want = sessionStorage.getItem('cat-' + sc); }} catch(e) {{}}
  const btn = [...tabs].find(b => b.dataset.cat === want);
  if (btn) switchCat(btn, sc, want);
}});

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
const CH_TF = ['1D','1M','3M','6M','1Y','3Y','5Y'];
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

// ความชันเฉลี่ยของชุดตัวเลข (least squares) ใช้บอกทิศทางเทรนด์
function linSlope(ys){{
  const n = ys.length;
  if (n < 2) return 0;
  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (let i = 0; i < n; i++) {{ sx += i; sy += ys[i]; sxx += i * i; sxy += i * ys[i]; }}
  const den = n * sxx - sx * sx;
  return den ? (n * sxy - sx * sy) / den : 0;
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

function toggleFav(ev, label){{
  ev.stopPropagation();
  chFavs.has(label) ? chFavs.delete(label) : chFavs.add(label);
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
  const groups = {{th: 'THAILAND', intl: 'GLOBAL'}};
  let html = '', shown = 0;
  for (const [g, title] of Object.entries(groups)) {{
    const rows = Object.entries(CHARTS)
      .filter(([l, c]) => (c.g || 'intl') === g
        && (chMode === 'all' || chFavs.has(l))
        && (!term || l.toLowerCase().includes(term)))
      .sort((a, b) => {{
        const A = TNEWS[a[0]], B = TNEWS[b[0]];
        if (!!A !== !!B) return A ? -1 : 1;             // ตัวที่มีราคาสดขึ้นก่อน
        if (A && B) return (B.pctv ?? 0) - (A.pctv ?? 0);
        return a[0].localeCompare(b[0]);
      }});
    if (!rows.length) continue;
    shown += rows.length;
    html += `<div class="cgroup">${{title}} · ${{rows.length}}</div>` + rows.map(([l]) => {{
      const d = TNEWS[l], f = chFavs.has(l);
      return `<div class="citem" role="button" tabindex="0" data-label="${{esc(l)}}"
        onclick="pickChart('${{esc(l)}}')" onkeydown="if(event.key==='Enter')pickChart('${{esc(l)}}')">
        ${{assetLogo(l)}}<span class="cname">${{esc(l)}}</span>
        <span class="cpct ${{d ? d.dir : ''}}">${{d ? d.pct : ''}}</span>
        <span class="cfav${{f ? ' on' : ''}}" role="button" tabindex="-1"
          title="${{f ? 'Remove from favorites' : 'Add to favorites'}}"
          onclick="toggleFav(event,'${{esc(l)}}')">${{f ? '★' : '☆'}}</span></div>`;
    }}).join('');
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
  document.getElementById('fin-btn').hidden = !CHARTS[label].f;
  renderChart();
}}

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
];
const FIN_SEC = {{income: 'INCOME STATEMENT', balance: 'BALANCE SHEET'}};
let finCache = {{}}, finSpan = 'annual';

const finFmt = (v, isEps) => {{
  if (v == null || !isFinite(v)) return null;
  if (isEps) return (v < 0 ? '-' : '') + '$' + Math.abs(v).toFixed(2);
  const a = Math.abs(v), sign = v < 0 ? '-' : '';
  if (a >= 1e12) return sign + (a / 1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return sign + (a / 1e9).toFixed(2) + 'B';
  if (a >= 1e6)  return sign + (a / 1e6).toFixed(1) + 'M';
  if (a >= 1e3)  return sign + (a / 1e3).toFixed(0) + 'K';
  return sign + a.toFixed(0);
}};

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

function renderFinTable(){{
  const body = document.getElementById('fin-body');
  const data = finCache[chCur];
  if (!data) {{ body.innerHTML = '<div class="cempty">Loading…</div>'; return; }}
  let rows = data[finSpan] || [];
  if (finSpan === 'quarterly') rows = currentYearQuarters(rows);
  if (!rows.length) {{
    body.innerHTML = `<div class="fin-empty"><b>No ${{finSpan}} data reported yet</b>` +
      '<span>Try the other tab, or check back after the next earnings release.</span></div>';
    return;
  }}
  const cols = rows.map(r => periodLabel(r.date, finSpan));
  let html = '<table class="fin-table"><thead><tr><th>Line item</th>' +
    cols.map(c => `<th>${{esc(c)}}</th>`).join('') + '</tr></thead><tbody>';
  let sec = null;
  for (const [key, label, group] of FIN_FIELDS) {{
    if (group !== sec) {{
      sec = group;
      html += `<tr class="fin-sec"><td colspan="${{cols.length + 1}}">${{FIN_SEC[sec]}}</td></tr>`;
    }}
    html += `<tr><td>${{esc(label)}}</td>` + rows.map((r, i) => {{
      const v = r[key];
      const txt = finFmt(v, key === 'eps');
      if (txt == null) return '<td class="fin-na">—</td>';
      const prev = i > 0 ? rows[i - 1][key] : null;
      let delta = '';
      if (prev != null && prev !== 0 && v != null) {{
        const pct = (v / prev - 1) * 100;
        const dir = pct > 0.5 ? 'up' : pct < -0.5 ? 'down' : 'flat';
        delta = `<span class="fin-delta ${{dir}}">${{pct >= 0 ? '+' : ''}}${{pct.toFixed(1)}}%</span>`;
      }}
      return `<td><span class="fin-val">${{txt}}</span>${{delta}}</td>`;
    }}).join('') + '</tr>';
  }}
  html += '</tbody></table>';
  body.innerHTML = html;
}}

function pickFinSpan(span){{
  finSpan = span;
  document.querySelectorAll('.fin-tab').forEach(b =>
    b.classList.toggle('on', b.dataset.span === span));
  renderFinTable();
}}

async function openFinancials(){{
  if (!chCur || !CHARTS[chCur]?.f) return;
  document.getElementById('fin-name').textContent = chCur;
  document.getElementById('fin-currency').textContent =
    TNEWS[chCur]?.group === 'th' ? 'THB' : '';
  document.getElementById('finmodal').hidden = false;
  document.body.style.overflow = 'hidden';
  if (!finCache[chCur]) {{
    document.getElementById('fin-body').innerHTML = '<div class="cempty">Loading…</div>';
    try {{
      finCache[chCur] = await fetch('{FIN_DIR}/' + CHARTS[chCur].s + '.json').then(r => r.json());
    }} catch (e) {{ finCache[chCur] = {{annual: [], quarterly: []}}; }}
  }}
  renderFinTable();
}}
function closeFinancials(){{
  document.getElementById('finmodal').hidden = true;
  if (document.getElementById('cmodal').hidden) document.body.style.overflow = '';
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
  const mainH = Math.max(80, innerH - subs.length * subH);

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
          year: ['3Y', '5Y', '1Y'].includes(chTf) ? '2-digit' : undefined}});
  }};
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

    const step = Math.max(1, Math.round((i1 - i0) / 6));
    const xt = [];
    for (let i = i0; i <= i1; i += step) xt.push(i);
    gX.selectAll('text').data(xt).join('text')
      .attr('x', d => zx(d)).attr('y', 14).attr('text-anchor', 'middle')
      .text(d => fmtT(rows[d][0]));

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
  if (ev.key !== 'Escape') return;
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
  // แท็บ "ทั้งหมด" โชว์เรื่องเด่นอันเดียว (ข่าวใหม่สุด) ไม่ใช่ทั้งสองฝั่ง
  document.querySelectorAll('.hero').forEach(h =>
    h.hidden = !(s === 'all' ? h.dataset.primary === '1' : h.dataset.scope === s));
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

function openMap(){{
  document.getElementById('mmodal').hidden = false;
  document.body.style.overflow = 'hidden';
  draw();                       // แผนที่เพิ่งมีขนาดตอนนี้ ต้องวาดใหม่
}}
function closeMap(){{
  document.getElementById('mmodal').hidden = true;
  document.body.style.overflow = '';
}}
document.getElementById('mmodal').addEventListener('click', ev => {{
  if (ev.target.id === 'mmodal') closeMap();
}});

// พับ/กางกลุ่มข่าวไทย-ต่างประเทศ แล้วจำไว้
document.querySelectorAll('.scope-group').forEach(g => {{
  const t = g.querySelector('.scope-title');
  if (!t) return;
  const key = 'fold-' + g.dataset.scope + '-' + (g.querySelector('.row-track') || {{}}).id;
  t.insertAdjacentHTML('afterbegin',
    '<svg class="scope-caret" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg>');
  t.setAttribute('role', 'button');
  t.setAttribute('tabindex', '0');
  try {{ if (localStorage.getItem(key) === '1') g.classList.add('folded'); }} catch(e) {{}}
  const flip = () => {{
    g.classList.toggle('folded');
    try {{ localStorage.setItem(key, g.classList.contains('folded') ? '1' : '0'); }} catch(e) {{}}
  }};
  t.addEventListener('click', flip);
  t.addEventListener('keydown', ev => {{
    if (ev.key === 'Enter' || ev.key === ' ') {{ ev.preventDefault(); flip(); }}
  }});
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

    charts, logos = {}, {}
    if markets:
        print("ดึงข้อมูลพื้นฐาน...")
        fetch_fundamentals(markets)
        logos = fetch_logos()
        print("ดึงข้อมูลแท่งเทียน...")
        charts = build_charts(markets)
        print("ดึงงบการเงิน...")
        fin_labels = fetch_financials()
        for label in fin_labels:
            if label in charts:
                charts[label]["f"] = True
    print()

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(render(news, markets, charts, logos, streams))
    print(f"เสร็จ · index.html · {NOW.strftime('%H:%M')} น.")
