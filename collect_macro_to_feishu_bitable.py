import os
import re
import json
import math
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
import akshare as ak


# =========================
# 配置区
# =========================
TIMEOUT = 30

FRED_DGS10_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
FRED_DGS2_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2"
FRED_FEDFUNDS_TARGET_UPPER_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFEDTARU"
FRED_FEDFUNDS_TARGET_LOWER_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFEDTARL"
FRED_VIX_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"
FRED_WTI_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"  # fallback

EIA_WTI_PAGE = "https://www.eia.gov/dnav/pet/hist/rwtcm.htm"
CBOE_VIX_PAGE = "https://www.cboe.com/tradable-products/vix/"

INVESTING_DXY_PAGE = "https://www.investing.com/indices/usdollar"
INVESTING_COPPER_PAGE = "https://www.investing.com/commodities/copper"
INVESTING_USDCNH_PAGE = "https://www.investing.com/currencies/usd-cnh"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

HS300_PE_CANDIDATE_COLUMNS = [
    "滚动市盈率",
    "滚动市盈率中位数",
    "静态市盈率",
    "静态市盈率中位数",
    "等权滚动市盈率",
    "等权静态市盈率",
]

YF_FALLBACK = {
    "DXY": "DX-Y.NYB",
    "COPPER": "HG=F",
    "USDCNH": "CNH=X",
    "VIX": "^VIX",
}


# =========================
# 工具函数
# =========================
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def is_valid_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and not math.isnan(x)


def safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def normalize_date(x):
    if pd.isna(x):
        return None
    try:
        return pd.to_datetime(x).strftime("%Y-%m-%d")
    except Exception:
        return str(x)


def get_url_text(url: str) -> str:
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text


def parse_first_number(text: str):
    """
    从一段文本中提取第一个看起来像价格的数值，支持千分位逗号。
    """
    if not text:
        return None
    candidates = re.findall(r'(?<!\d)(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+\.\d+|\d+)(?!\d)', text)
    for c in candidates:
        try:
            return float(c.replace(",", ""))
        except Exception:
            continue
    return None


def read_fred_last_value(csv_url: str, value_name: str):
    resp = requests.get(csv_url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()

    df = pd.read_csv(pd.io.common.StringIO(resp.text))
    df.columns = [str(c).strip() for c in df.columns]

    date_col = None
    value_col = None

    for c in df.columns:
        col = str(c).strip().lower()
        if col in ["date", "observation_date"]:
            date_col = c
        if str(c).strip().upper() == value_name.upper():
            value_col = c

    if date_col is None or value_col is None:
        raise ValueError(f"FRED 字段异常：{list(df.columns)}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    if df.empty:
        raise ValueError(f"{value_name} 数据为空")

    last_row = df.iloc[-1]
    return safe_float(last_row[value_col]), normalize_date(last_row[date_col])


def fetch_yfinance_last_close(symbol: str):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="10d", interval="1d", auto_adjust=False)
    if hist is None or hist.empty:
        raise ValueError(f"{symbol} history 为空")

    hist = hist.dropna(subset=["Close"])
    if hist.empty:
        raise ValueError(f"{symbol} 无有效收盘价")

    last_row = hist.iloc[-1]
    last_idx = hist.index[-1]
    return safe_float(last_row["Close"]), pd.to_datetime(last_idx).strftime("%Y-%m-%d")


def extract_investing_price(html: str):
    """
    Investing 页面结构常改，这里做多模式兜底。
    """
    patterns = [
        r'data-test="instrument-price-last">([^<]+)<',
        r'"last_last","([^"]+)"',
        r'"last":"([^"]+)"',
        r'"price":"([^"]+)"',
        r'What Is the Current .*? exchange rate is ([0-9.,]+)',
        r'current .*? is ([0-9.,]+), with a previous close',
    ]

    for p in patterns:
        m = re.search(p, html, flags=re.I | re.S)
        if m:
            val = parse_first_number(m.group(1))
            if val is not None:
                return val

    # 最后兜底：找一些附近关键词
    keyword_patterns = [
        r'Price[^0-9]{0,30}([0-9][0-9,]*\.?[0-9]*)',
        r'Last[^0-9]{0,30}([0-9][0-9,]*\.?[0-9]*)',
    ]
    for p in keyword_patterns:
        m = re.search(p, html, flags=re.I | re.S)
        if m:
            val = parse_first_number(m.group(1))
            if val is not None:
                return val

    raise ValueError("Investing 页面价格解析失败")


# =========================
# 飞书
# =========================
def get_tenant_access_token():
    app_id = os.environ["FEISHU_APP_ID"]
    app_secret = os.environ["FEISHU_APP_SECRET"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")

    return data["tenant_access_token"]


# =========================
# 数据抓取：官方/FRED
# =========================
def fetch_fed_rate_target():
    # akshare 数据质量不稳定，改用 yfinance ^IRX（13周国债，短端利率近似）
    value, _ = fetch_yfinance_last_close("^IRX")
    if value is None:
        raise ValueError("^IRX 为空")
    return {"美联储基准利率": f"{value:.2f}"}


def fetch_us2y_fred():
    # akshare 美国2年期国债收益率，比 Treasury XML 更稳定
    df = ak.bond_zh_us_rate(start_date="20250101")
    if df is None or df.empty:
        raise ValueError("美国国债收益率数据为空")
    df.columns = [str(c).strip() for c in df.columns]
    # 找2年期列
    col_2y = None
    for c in df.columns:
        if "2" in c and ("年" in c or "year" in c.lower() or "Y" in c):
            col_2y = c
            break
    if col_2y is None:
        raise ValueError(f"未找到2年期列，列名：{list(df.columns)}")
    df[col_2y] = pd.to_numeric(df[col_2y], errors="coerce")
    df = df.dropna(subset=[col_2y])
    if df.empty:
        raise ValueError("2年期收益率有效数据为空")
    return {"美国2年期收益率": safe_float(df.iloc[-1][col_2y])}


def fetch_us10y_fred():
    value, _ = fetch_yfinance_last_close("^TNX")
    return {"美国10年期收益率": value}


def fetch_wti_eia():
    # 主源：EIA 官方 API（无需 key，返回 WTI 现货价格）
    try:
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=DEMO_KEY&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&length=5&facets[product][]=EPCWTI"
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("response", {}).get("data", [])
        for row in rows:
            val = safe_float(row.get("value"))
            if val is not None and 20 < val < 300:
                return {"WTI原油": val}
    except Exception:
        pass
    # 备源：FRED DCOILWTICO（1-2天延迟）
    value, _ = read_fred_last_value(FRED_WTI_CSV, "DCOILWTICO")
    if value is None:
        raise ValueError("WTI原油数据为空")
    return {"WTI原油": value}


def fetch_vix():
    value, _ = fetch_yfinance_last_close("^VIX")
    return {"VIX": value}


# =========================
# 数据抓取：Investing 主源 + 备源
# =========================
def fetch_dxy():
    try:
        html = get_url_text(INVESTING_DXY_PAGE)
        value = extract_investing_price(html)
        return {"美元指数DXY": value}
    except Exception:
        value, _ = fetch_yfinance_last_close(YF_FALLBACK["DXY"])
        return {"美元指数DXY": value}


def fetch_copper():
    try:
        html = get_url_text(INVESTING_COPPER_PAGE)
        value = extract_investing_price(html)
        return {"铜价": value}
    except Exception:
        value, _ = fetch_yfinance_last_close(YF_FALLBACK["COPPER"])
        return {"铜价": value}


def fetch_usdcnh():
    try:
        html = get_url_text(INVESTING_USDCNH_PAGE)
        value = extract_investing_price(html)
        return {"USD/CNH": value}
    except Exception:
        value, _ = fetch_yfinance_last_close(YF_FALLBACK["USDCNH"])
        return {"USD/CNH": value}


# =========================
# 中国数据
# =========================
def fetch_china_social_financing():
    df = ak.macro_china_shrzgm()
    if df is None or df.empty:
        raise ValueError("中国社融数据为空")

    month_col = None
    value_col = None

    for c in df.columns:
        if str(c).strip() == "月份":
            month_col = c
        if str(c).strip() == "社会融资规模增量":
            value_col = c

    if month_col is None or value_col is None:
        raise ValueError(f"社融字段异常：{list(df.columns)}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    if df.empty:
        raise ValueError("中国社融有效数据为空")

    last_row = df.iloc[-1]
    return {"中国社融增量_亿元": safe_float(last_row[value_col])}


def fetch_hs300_pe():
    """
    自动化上先用稳定备源，避免中证官网 JS 结构变化导致任务天天挂。
    """
    df = ak.stock_index_pe_lg(symbol="沪深300")
    if df is None or df.empty:
        raise ValueError("沪深300市盈率数据为空")

    pe_col = None
    for col in HS300_PE_CANDIDATE_COLUMNS:
        if col in df.columns:
            pe_col = col
            break

    if pe_col is None:
        raise ValueError(f"未找到沪深300市盈率字段：{list(df.columns)}")

    df[pe_col] = pd.to_numeric(df[pe_col], errors="coerce")
    df = df.dropna(subset=[pe_col])

    if df.empty:
        raise ValueError("沪深300市盈率有效数据为空")

    last_row = df.iloc[-1]
    return {"沪深300市盈率": safe_float(last_row[pe_col])}


# =========================
# 汇总
# =========================
def build_snapshot():
    snapshot = {
        "日期": datetime.now().strftime("%Y-%m-%d"),
        "美联储基准利率": "",
        "美国2年期收益率": None,
        "美国10年期收益率": None,
        "美元指数DXY": None,
        "WTI原油": None,
        "铜价": None,
        "USD/CNH": None,
        "VIX": None,
        "中国社融增量_亿元": None,
        "沪深300市盈率": None,
    }

    tasks = [
        ("美联储基准利率", fetch_fed_rate_target, "美联储基准利率"),
        ("美国2年期收益率", fetch_us2y_fred, "美国2年期收益率"),
        ("美国10年期收益率", fetch_us10y_fred, "美国10年期收益率"),
        ("美元指数DXY", fetch_dxy, "美元指数DXY"),
        ("WTI原油", fetch_wti_eia, "WTI原油"),
        ("铜价", fetch_copper, "铜价"),
        ("USD/CNH", fetch_usdcnh, "USD/CNH"),
        ("VIX", fetch_vix, "VIX"),
        ("中国社融", fetch_china_social_financing, "中国社融增量_亿元"),
        ("沪深300市盈率", fetch_hs300_pe, "沪深300市盈率"),
    ]

    for task_name, func, field_name in tasks:
        try:
            log(f"开始抓取：{task_name}")
            data = func()
            snapshot[field_name] = data.get(field_name, snapshot[field_name])
            log(f"抓取成功：{task_name}")
        except Exception as e:
            log(f"抓取失败：{task_name} | {e}")
            log(traceback.format_exc())

    return snapshot


# =========================
# 写入飞书
# =========================
def append_record_to_bitable(snapshot: dict):
    tenant_access_token = get_tenant_access_token()
    app_token = os.environ["FEISHU_BITABLE_APP_TOKEN"]
    table_id = os.environ["FEISHU_BITABLE_TABLE_ID"]

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"

    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json",
    }

    date_str = snapshot.get("日期", "")
    date_ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000) if date_str else 0

    fields = {
        "日期": date_ts,
        "美联储基准利率": snapshot.get("美联储基准利率", ""),
    }

    optional_number_fields = [
        "美国2年期收益率",
        "美国10年期收益率",
        "美元指数DXY",
        "WTI原油",
        "铜价",
        "USD/CNH",
        "VIX",
        "中国社融增量_亿元",
        "沪深300市盈率",
    ]

    for field_name in optional_number_fields:
        value = snapshot.get(field_name, None)
        if is_valid_number(value):
            fields[field_name] = value

    payload = {"fields": fields}

    resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    data = resp.json()

    if resp.status_code != 200 or data.get("code") != 0:
        raise RuntimeError(f"写入飞书多维表格失败: {data}")

    log("已写入飞书多维表格")
    log(json.dumps(fields, ensure_ascii=False))


def main():
    log("===== 开始执行 =====")
    snapshot = build_snapshot()
    append_record_to_bitable(snapshot)
    log("===== 执行完成 =====")


if __name__ == "__main__":
    main()
