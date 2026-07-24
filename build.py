#!/usr/bin/env python3
"""
Econ/Politics Monitor — dashboard ข่าวเศรษฐกิจ+การเมือง พร้อมแผนที่โลก
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
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

import feedparser
import requests

socket.setdefaulttimeout(15)   # กัน feedparser ค้างถ้าเว็บข่าวไม่ตอบสนอง

TZ = timezone(timedelta(hours=7))          # Asia/Bangkok
NOW = datetime.now(TZ)
MAX_AGE_HOURS = 24
PER_CATEGORY = 18
CACHE_FILE = "cache.json"
HISTORY_FILE = "market_history.json"
HISTORY_POINTS = 60     # 60 รอบ x 3 ชม. ≈ 7.5 วัน

# ─────────────────────────────────────────────────────────────
# แหล่งข่าว — เพิ่ม/ลบได้ตามใจ ไม่ต้องใช้ API key
# ─────────────────────────────────────────────────────────────
FEEDS = [
    ("Thai PBS",        "https://www.thaipbs.or.th/rss/news.xml",                    "th"),
    ("The Standard",    "https://thestandard.co/feed/",                              "th"),
    ("ประชาชาติธุรกิจ",  "https://www.prachachat.net/feed",                            "th"),
    ("กรุงเทพธุรกิจ",    "https://www.bangkokbiznews.com/rss/feed/business.xml",       "th"),
    ("มติชน",           "https://www.matichon.co.th/feed",                            "th"),
    ("BBC Business",    "https://feeds.bbci.co.uk/news/business/rss.xml",             "en"),
    ("BBC World",       "https://feeds.bbci.co.uk/news/world/rss.xml",                "en"),
    ("Al Jazeera",      "https://www.aljazeera.com/xml/rss/all.xml",                  "en"),
    ("CNBC",            "https://www.cnbc.com/id/100727362/device/rss/rss.html",      "en"),
    ("Google News",     "https://news.google.com/rss/search?q=เศรษฐกิจไทย&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Google News",     "https://news.google.com/rss/search?q=การเมืองไทย&hl=th&gl=TH&ceid=TH:th", "th"),
    ("Reuters",         "https://news.google.com/rss/search?q=site:reuters.com+when:1d&hl=en&gl=US&ceid=US:en", "en"),
    ("Investing.com",   "https://www.investing.com/rss/news.rss",                     "en"),
]

TICKERS = [
    ("SET",       "^SET.BK"),
    ("S&P 500",   "^GSPC"),
    ("NASDAQ",    "^IXIC"),
    ("USD/THB",   "THB=X"),
    ("ทองคำ",     "GC=F"),
    ("น้ำมัน WTI", "CL=F"),
    ("Bitcoin",   "BTC-USD"),
]

KW_ECON = [
    "เศรษฐกิจ", "จีดีพี", "GDP", "เงินเฟ้อ", "ดอกเบี้ย", "ธปท", "แบงก์ชาติ", "ตลาดหุ้น",
    "หุ้น", "ค่าเงิน", "ส่งออก", "นำเข้า", "ลงทุน", "ภาษี", "งบประมาณ", "หนี้",
    "ราคาน้ำมัน", "ทองคำ", "คริปโต", "ธนาคาร", "ท่องเที่ยว", "อสังหา", "ค้าปลีก",
    "economy", "economic", "inflation", "gdp", "fed", "interest rate", "market",
    "stock", "trade", "tariff", "export", "import", "invest", "bank", "currency",
    "oil price", "gold", "crypto", "recession", "budget", "debt", "earnings",
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


def classify(text):
    t = text.lower()
    e = sum(1 for k in KW_ECON if k.lower() in t)
    p = sum(1 for k in KW_POLI if k.lower() in t)
    if e == 0 and p == 0:
        return None
    return "econ" if e >= p else "poli"


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


def extract_image(e):
    """ดึงรูปประกอบข่าวจาก media:thumbnail / media:content / enclosure / <img> แรกใน summary"""
    thumbs = e.get("media_thumbnail") or []
    if thumbs and thumbs[0].get("url"):
        return thumbs[0]["url"]

    for m in e.get("media_content") or []:
        if m.get("url") and ("image" in (m.get("type") or "") or m.get("medium") == "image"):
            return m["url"]

    for lk in e.get("links") or []:
        if lk.get("rel") == "enclosure" and "image" in (lk.get("type") or ""):
            return lk.get("href")

    m = re.search(r'<img[^>]+src="([^"]+)"', e.get("summary", "") or "")
    if m:
        return m.group(1)
    return None


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


def build_markers(news):
    """รวมข่าวตามสถานที่ → จุดบนแผนที่"""
    by_place = {}
    for it in news:
        if not it["place"]:
            continue
        m = by_place.setdefault(it["place"], {
            "place": it["place"], "lat": it["lat"], "lon": it["lon"],
            "econ": 0, "poli": 0, "stories": [],
        })
        m[it["cat"]] += 1
        if len(m["stories"]) < 6:
            m["stories"].append({
                "title": it["title"], "link": it["link"], "image": it["image"],
                "source": it["source"], "age": it["age"], "cat": it["cat"],
            })
    out = list(by_place.values())
    for m in out:
        m["total"] = m["econ"] + m["poli"]
        m["cat"] = ("econ" if m["econ"] > m["poli"]
                    else "poli" if m["poli"] > m["econ"] else "both")
    return sorted(out, key=lambda x: -x["total"])


def fetch_markets():
    out = []
    for label, sym in TICKERS:
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                params={"range": "2d", "interval": "1d"},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=12,
            )
            meta = r.json()["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev = meta.get("chartPreviousClose") or meta.get("previousClose") or price
            pct = (price - prev) / prev * 100 if prev else 0
            out.append({"label": label, "price": f"{price:,.2f}", "raw_price": price,
                        "pct": pct, "pct_str": f"{pct:+.2f}%"})
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
    ts = NOW.isoformat()
    for m in markets:
        h = history.setdefault(m["label"], [])
        h.append({"t": ts, "p": m["raw_price"]})
        del h[:-HISTORY_POINTS]
    save_json(HISTORY_FILE, history)
    return history


def sparkline_svg(points, color):
    if len(points) < 2:
        return ""
    lo, hi = min(points), max(points)
    span = (hi - lo) or (abs(hi) or 1)
    w, h, pad = 60, 20, 2
    step = (w - pad * 2) / (len(points) - 1)
    coords = [(pad + i * step, pad + (1 - (v - lo) / span) * (h - pad * 2))
              for i, v in enumerate(points)]
    d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    return (f'<svg class="spark" viewBox="0 0 {w} {h}" preserveAspectRatio="none">'
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.6"/></svg>')


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
const svg = d3.select("#map");
const gMap = svg.append("g");
const tip = d3.select("#tip");
const detail = d3.select("#hotspot-detail");
const COLOR = { econ:"#4C8DFF", poli:"#F5A524", both:"#9B8AFB" };

function show(d){
  detail.html(
    `<div class="hd-top"><h4>${d.place}</h4><span>${d.total} ข่าว</span></div>` +
    d.stories.map(s =>
      `<a class="hd-row" href="${s.link}" target="_blank" rel="noopener">
         ${s.image ? `<img class="hd-thumb" src="${s.image}" loading="lazy" alt="" onerror="this.remove()">` : ""}
         <span class="dot ${s.cat}"></span><span>${s.title}</span>
         <span class="hd-age">${s.age}</span></a>`).join("")
  );
}

function plot(proj){
  const maxN = d3.max(MARKERS, d => d.total) || 1;
  const r = d3.scaleSqrt().domain([1, maxN]).range([5, 17]);
  const g = gMap.append("g");

  const nodes = g.selectAll("g.mk").data(MARKERS).join("g")
    .attr("class", "mk")
    .attr("transform", d => {
      const p = proj([d.lon, d.lat]);
      return `translate(${p[0]},${p[1]})`;
    });

  nodes.append("circle").attr("class", "halo")
    .attr("r", d => r(d.total)).attr("fill", d => COLOR[d.cat]);
  nodes.append("circle").attr("class", "core")
    .attr("r", d => Math.max(2.6, r(d.total) * 0.36)).attr("fill", d => COLOR[d.cat]);

  nodes.filter(d => d.total >= Math.max(2, maxN * 0.5))
    .append("text").attr("class", "mk-label")
    .attr("y", d => -r(d.total) - 6).attr("text-anchor", "middle")
    .text(d => d.place);

  nodes
    .on("mousemove", (ev, d) => {
      tip.style("opacity", 1)
         .style("left", (ev.offsetX + 14) + "px")
         .style("top", (ev.offsetY - 8) + "px")
         .html(`<strong>${d.place}</strong><span>${d.total} ข่าว · เศรษฐกิจ ${d.econ} · การเมือง ${d.poli}</span>`);
    })
    .on("mouseleave", () => tip.style("opacity", 0))
    .on("click", (ev, d) => { show(d); ev.stopPropagation(); });
}

function draw(){
  const box = svg.node().getBoundingClientRect();
  const W = box.width, H = box.height;
  svg.attr("viewBox", `0 0 ${W} ${H}`);
  gMap.selectAll("*").remove();

  const proj = d3.geoNaturalEarth1()
    .fitSize([W, H * 1.28], { type: "Sphere" })
    .translate([W / 2, H / 2 + H * 0.03]);
  const path = d3.geoPath(proj);

  gMap.append("path").attr("class", "sphere").attr("d", path({ type: "Sphere" }));
  gMap.append("path").attr("class", "grat").attr("d", path(d3.geoGraticule10()));

  d3.json("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json")
    .then(topo => {
      const land = topojson.feature(topo, topo.objects.countries);
      gMap.append("g").selectAll("path").data(land.features).join("path")
        .attr("class", "country").attr("d", path);
      plot(proj);
    })
    .catch(() => plot(proj));
}

if (MARKERS.length) show(MARKERS[0]);
draw();
let _t; addEventListener("resize", () => { clearTimeout(_t); _t = setTimeout(draw, 200); });
"""


def render(news, markets, history):
    econ = [i for i in news if i["cat"] == "econ"][:PER_CATEGORY]
    poli = [i for i in news if i["cat"] == "poli"][:PER_CATEGORY]
    latest = news[:7]
    markers = build_markers(news)
    kws = top_keywords(news)
    maxf = max([f for _, f in kws], default=1)
    located = sum(1 for i in news if i["place"])

    def card(it):
        loc = f'<span class="loc">{html.escape(it["place"])}</span>' if it["place"] else ""
        img = (f'<img class="thumb" src="{html.escape(it["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        speak_text = html.escape(f"{it['title']}. {it['summary']}", quote=True)
        speak_lang = "th-TH" if it["lang"] == "th" else "en-US"
        return f"""<div class="item">
      {img}
      <div class="item-body">
        <div class="item-head"><span class="src">{html.escape(it['source'])}{loc}</span><span class="age">{it['age']}</span></div>
        <a class="item-link" href="{html.escape(it['link'])}" target="_blank" rel="noopener">
          <h3>{html.escape(it['title'])}</h3>
          {f'<p>{html.escape(it["summary"])}</p>' if it['summary'] else ''}
        </a>
        <div class="item-foot">
          <button class="speak" type="button" data-text="{speak_text}" data-lang="{speak_lang}">🔊 ฟังข่าว</button>
          <a class="full" href="{html.escape(it['link'])}" target="_blank" rel="noopener">อ่านฉบับเต็ม →</a>
        </div>
      </div>
    </div>"""

    def feed_row(it):
        img = (f'<img class="feed-thumb" src="{html.escape(it["image"])}" loading="lazy" alt=""'
               f' onerror="this.remove()">') if it.get("image") else ""
        return f"""<a class="feed-row" href="{html.escape(it['link'])}" target="_blank" rel="noopener">
      {img}
      <span class="dot {it['cat']}"></span>
      <span class="feed-title">{html.escape(it['title'])}</span>
      <span class="feed-age">{it['age']}</span></a>"""

    def tick(m):
        cls = "up" if m["pct"] > 0 else ("down" if m["pct"] < 0 else "flat")
        color = {"up": "var(--up)", "down": "var(--down)", "flat": "var(--mute)"}[cls]
        pts = [p["p"] for p in history.get(m["label"], [])]
        spark = sparkline_svg(pts, color)
        return f"""<div class="tick"><span class="t-label">{html.escape(m['label'])}</span>
      <span class="t-price">{m['price']}</span>{spark}<span class="t-pct {cls}">{m['pct_str']}</span></div>"""

    def hot_row(m, i):
        return f"""<div class="hot"><span class="rank">{i+1}</span>
      <span class="hot-name">{html.escape(m['place'])}</span>
      <span class="hot-bars"><i class="be" style="flex:{m['econ']}"></i><i class="bp" style="flex:{m['poli']}"></i></span>
      <span class="hot-n">{m['total']}</span></div>"""

    def kw_chip(w, f):
        return (f'<span class="kw" style="font-size:{0.78 + (f/maxf)*0.85:.2f}rem;'
                f'opacity:{0.45 + (f/maxf)*0.55:.2f}">{html.escape(w)}</span>')

    next_run = (NOW + timedelta(hours=3)).strftime("%H:%M")
    markers_json = json.dumps(markers, ensure_ascii=False)
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
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Thai:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#0A0E1A; --panel:#111726; --panel2:#0E1420; --line:#1E2637;
  --ink:#E7ECF5; --mute:#7A879C; --dim:#4E5A70;
  --econ:#4C8DFF; --poli:#F5A524; --both:#9B8AFB; --up:#3FB68B; --down:#E5484D;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--ink);
  font-family:'IBM Plex Sans Thai',system-ui,sans-serif;
  font-size:15px;line-height:1.55;padding:20px;max-width:1560px;margin:0 auto}}
a{{color:inherit;text-decoration:none}}

header{{display:flex;align-items:center;justify-content:space-between;gap:20px;
  flex-wrap:wrap;padding-bottom:16px;margin-bottom:16px;border-bottom:1px solid var(--line)}}
h1{{font-size:1.35rem;font-weight:700;letter-spacing:-.01em}}
h1 span{{color:var(--dim);font-weight:400}}
.stamp{{display:flex;align-items:center;gap:10px;
  font-family:'IBM Plex Mono',monospace;font-size:.76rem;color:var(--mute)}}
.pulse{{width:7px;height:7px;border-radius:50%;background:var(--up);
  box-shadow:0 0 0 0 rgba(63,182,139,.6);animation:p 2.4s infinite}}
@keyframes p{{70%{{box-shadow:0 0 0 9px rgba(63,182,139,0)}}100%{{box-shadow:0 0 0 0 rgba(63,182,139,0)}}}}

.ticker{{display:flex;overflow-x:auto;border:1px solid var(--line);
  border-radius:10px;background:var(--panel);margin-bottom:16px}}
.tick{{flex:1 0 auto;min-width:132px;padding:11px 16px;
  border-right:1px solid var(--line);display:flex;flex-direction:column;gap:2px}}
.tick:last-child{{border-right:0}}
.t-label{{font-size:.68rem;color:var(--mute);text-transform:uppercase;letter-spacing:.06em}}
.t-price{{font-family:'IBM Plex Mono',monospace;font-size:.95rem;font-weight:500}}
.t-pct{{font-family:'IBM Plex Mono',monospace;font-size:.74rem}}
.up{{color:var(--up)}} .down{{color:var(--down)}} .flat{{color:var(--mute)}}
.spark{{width:60px;height:20px;display:block}}

.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden}}
.panel-head{{display:flex;align-items:center;justify-content:space-between;
  padding:12px 15px;border-bottom:1px solid var(--line);background:var(--panel2)}}
.panel-head h2{{font-size:.8rem;font-weight:600;letter-spacing:.05em;text-transform:uppercase}}
.count{{font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--dim)}}
.bar{{width:3px;height:14px;border-radius:2px;display:inline-block;margin-right:8px;vertical-align:-2px}}
.bar.econ{{background:var(--econ)}} .bar.poli{{background:var(--poli)}}

.top{{display:grid;grid-template-columns:1fr 330px;gap:16px;margin-bottom:16px}}
@media(max-width:1000px){{.top{{grid-template-columns:1fr}}}}

.map-wrap{{position:relative;height:440px;
  background:radial-gradient(ellipse at 50% 45%,#101827 0%,#0B111C 70%)}}
#map{{width:100%;height:100%;display:block}}
.sphere{{fill:#0C1220;stroke:#1A2333;stroke-width:.8}}
.grat{{fill:none;stroke:#141C2B;stroke-width:.45}}
.country{{fill:#172030;stroke:#232E42;stroke-width:.45}}
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
.legend{{position:absolute;left:14px;bottom:12px;display:flex;gap:14px;
  font-size:.68rem;color:var(--mute);background:rgba(10,14,26,.8);
  border:1px solid var(--line);border-radius:7px;padding:6px 11px}}
.legend i{{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}}

#hotspot-detail{{border-top:1px solid var(--line);max-height:172px;overflow-y:auto}}
.hd-top{{display:flex;justify-content:space-between;padding:10px 15px 6px;align-items:baseline}}
.hd-top h4{{font-size:.85rem;font-weight:600}}
.hd-top span{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hd-row{{display:flex;align-items:center;gap:9px;
  padding:8px 15px;border-top:1px solid var(--line);font-size:.8rem}}
.hd-row:hover{{background:#151C2C}}
.hd-row .dot{{flex:none}}
.hd-row > span:nth-last-child(2){{flex:1;min-width:0}}
.hd-thumb{{width:36px;height:36px;border-radius:6px;object-fit:cover;flex:none;background:var(--panel2)}}
.hd-age{{font-family:'IBM Plex Mono',monospace;font-size:.65rem;color:var(--dim);white-space:nowrap}}

.feed{{max-height:340px;overflow-y:auto}}
.feed-row{{display:flex;align-items:center;gap:10px;
  padding:9px 15px;border-bottom:1px solid var(--line);transition:background .12s}}
.feed-row:hover{{background:#151C2C}}
.feed-row:last-child{{border-bottom:0}}
.feed-thumb{{width:44px;height:44px;border-radius:7px;object-fit:cover;flex:none;background:var(--panel2)}}
.dot{{width:6px;height:6px;border-radius:50%;flex:none}}
.dot.econ{{background:var(--econ)}} .dot.poli{{background:var(--poli)}}
.feed-title{{flex:1;min-width:0;font-size:.82rem;line-height:1.4}}
.feed-age{{flex:none;font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim);white-space:nowrap}}

.hot{{display:grid;grid-template-columns:18px 1fr 62px 26px;gap:9px;align-items:center;
  padding:7px 15px;border-bottom:1px solid var(--line);font-size:.8rem}}
.hot:last-child{{border-bottom:0}}
.rank{{font-family:'IBM Plex Mono',monospace;font-size:.68rem;color:var(--dim)}}
.hot-bars{{display:flex;height:5px;border-radius:3px;overflow:hidden;background:#1A2333}}
.hot-bars .be{{background:var(--econ)}} .hot-bars .bp{{background:var(--poli)}}
.hot-n{{font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--mute);text-align:right}}

.search-wrap{{padding:11px 15px;border-bottom:1px solid var(--line)}}
.search{{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:8px;
  color:var(--ink);font-family:inherit;font-size:.82rem;padding:8px 12px}}
.search::placeholder{{color:var(--dim)}}
.search:focus{{outline:none;border-color:var(--econ)}}

.grid{{display:grid;grid-template-columns:1fr 1fr 330px;gap:16px;align-items:start}}
@media(max-width:1000px){{.grid{{grid-template-columns:1fr}}}}
.items{{max-height:620px;overflow-y:auto}}
.item{{display:block;border-bottom:1px solid var(--line);transition:background .12s}}
.item:hover{{background:#151C2C}}
.item:last-child{{border-bottom:0}}
.item.hidden{{display:none}}
.item .thumb{{width:100%;aspect-ratio:16/9;object-fit:cover;display:block;background:var(--panel2)}}
.item-body{{padding:13px 15px}}
.item-head{{display:flex;justify-content:space-between;gap:10px;margin-bottom:5px}}
.src{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--mute);
  text-transform:uppercase;letter-spacing:.05em}}
.loc{{margin-left:8px;padding:1px 6px;border:1px solid var(--line);border-radius:4px;
  color:var(--dim);text-transform:none;letter-spacing:0}}
.age{{font-family:'IBM Plex Mono',monospace;font-size:.66rem;color:var(--dim)}}
.item h3{{font-size:.92rem;font-weight:600;line-height:1.42;margin-bottom:4px}}
.item p{{font-size:.78rem;color:var(--mute);line-height:1.5}}
.item-foot{{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-top:8px}}
.speak{{font-family:inherit;font-size:.7rem;color:var(--mute);background:var(--panel2);
  border:1px solid var(--line);border-radius:6px;padding:4px 9px;cursor:pointer}}
.speak:hover{{color:var(--ink);border-color:var(--dim)}}
.speak.playing{{color:var(--econ);border-color:var(--econ)}}
.full{{font-size:.7rem;color:var(--dim)}}
.full:hover{{color:var(--mute)}}

.kws{{display:flex;flex-wrap:wrap;gap:7px 12px;padding:15px;align-items:baseline}}
.kw{{font-weight:500;line-height:1.2}}

footer{{margin-top:18px;padding-top:14px;border-top:1px solid var(--line);
  display:flex;justify-content:space-between;flex-wrap:wrap;gap:12px;
  font-family:'IBM Plex Mono',monospace;font-size:.7rem;color:var(--dim)}}
::-webkit-scrollbar{{width:8px;height:8px}}
::-webkit-scrollbar-track{{background:transparent}}
::-webkit-scrollbar-thumb{{background:#222B3D;border-radius:4px}}
@media(prefers-reduced-motion:reduce){{*{{animation:none!important;transition:none!important}}}}
</style>
</head>
<body>

<header>
  <h1>Econ · Politics Monitor <span>/ เศรษฐกิจ · การเมือง</span></h1>
  <div class="stamp">
    <span class="pulse"></span>
    <span>อัปเดต {NOW.strftime('%d %b %Y · %H:%M')} น.</span>
    <span style="color:var(--dim)">· รอบถัดไป {next_run} น.</span>
  </div>
</header>

<div class="ticker">{''.join(tick(m) for m in markets)}</div>

<div class="top">
  <section class="panel">
    <div class="panel-head">
      <h2>แผนที่ข่าว</h2>
      <span class="count">{len(markers)} พื้นที่ · คลิกจุดเพื่อดูข่าว</span>
    </div>
    <div class="map-wrap">
      <svg id="map"></svg>
      <div id="tip"></div>
      <div class="legend">
        <span><i style="background:var(--econ)"></i>เศรษฐกิจ</span>
        <span><i style="background:var(--poli)"></i>การเมือง</span>
        <span><i style="background:var(--both)"></i>ทั้งสอง</span>
      </div>
    </div>
    <div id="hotspot-detail"></div>
  </section>

  <section class="panel">
    <div class="panel-head"><h2>ล่าสุด</h2><span class="count">LIVE</span></div>
    <div class="feed">{''.join(feed_row(i) for i in latest)}</div>
  </section>
</div>

<div class="grid">
  <section class="panel">
    <div class="panel-head"><h2><span class="bar econ"></span>เศรษฐกิจ</h2><span class="count">{len(econ)}</span></div>
    <div class="search-wrap"><input class="search" type="search" placeholder="ค้นหาข่าวเศรษฐกิจ…" oninput="filterItems(this)"></div>
    <div class="items">{''.join(card(i) for i in econ) or '<div class="item"><p>ยังไม่มีข่าวในรอบนี้</p></div>'}</div>
  </section>

  <section class="panel">
    <div class="panel-head"><h2><span class="bar poli"></span>การเมือง</h2><span class="count">{len(poli)}</span></div>
    <div class="search-wrap"><input class="search" type="search" placeholder="ค้นหาข่าวการเมือง…" oninput="filterItems(this)"></div>
    <div class="items">{''.join(card(i) for i in poli) or '<div class="item"><p>ยังไม่มีข่าวในรอบนี้</p></div>'}</div>
  </section>

  <div style="display:flex;flex-direction:column;gap:16px">
    <section class="panel">
      <div class="panel-head"><h2>พื้นที่ที่มีข่าวมากสุด</h2></div>
      <div>{''.join(hot_row(m, i) for i, m in enumerate(markers[:10])) or '<div class="hot"><span></span><span>—</span></div>'}</div>
    </section>
    <section class="panel">
      <div class="panel-head"><h2>คำที่พูดถึงมากสุด</h2></div>
      <div class="kws">{''.join(kw_chip(w, f) for w, f in kws)}</div>
    </section>
  </div>
</div>

<footer>
  <span>{len(FEEDS)} แหล่งข่าว · {len(news)} ข่าวใน 24 ชม. · ระบุพิกัดได้ {located} ข่าว</span>
  <span>รีเฟรชอัตโนมัติทุก 15 นาที · ดึงข้อมูลใหม่ทุก 3 ชม.</span>
</footer>

<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
<script>window.__MARKERS__ = {markers_json};</script>
<script>{MAP_JS}</script>
<script>
function filterItems(input){{
  const q = input.value.trim().toLowerCase();
  const items = input.closest('section').querySelectorAll('.item');
  items.forEach(it => {{
    const hit = !q || it.textContent.toLowerCase().includes(q);
    it.classList.toggle('hidden', !hit);
  }});
}}

if ('speechSynthesis' in window) {{
  let currentBtn = null;
  document.addEventListener('click', ev => {{
    const btn = ev.target.closest('.speak');
    if (!btn) return;
    const wasPlaying = btn.classList.contains('playing');
    speechSynthesis.cancel();
    if (currentBtn) {{ currentBtn.classList.remove('playing'); currentBtn.textContent = '🔊 ฟังข่าว'; }}
    currentBtn = null;
    if (wasPlaying) return;
    const u = new SpeechSynthesisUtterance(btn.dataset.text);
    u.lang = btn.dataset.lang;
    u.onend = u.onerror = () => {{ btn.classList.remove('playing'); btn.textContent = '🔊 ฟังข่าว'; currentBtn = null; }};
    btn.classList.add('playing');
    btn.textContent = '⏸ กำลังอ่าน…';
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
    print(f"→ ได้ {len(news)} ข่าว (ระบุพิกัดได้ {sum(1 for i in news if i['place'])})\n")

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

    history = update_history(markets) if markets else load_json(HISTORY_FILE)
    save_cache(news, markets)

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(render(news, markets, history))
    print(f"เสร็จ · index.html · {NOW.strftime('%H:%M')} น.")
