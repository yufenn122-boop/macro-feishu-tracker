import os
import traceback
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
import akshare as ak

FRED_DGS10_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
FRED_DGS2_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2"
FRED_FEDFUNDS_TARGET_UPPER_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFEDTARU"
FRED_FEDFUNDS_TARGET_LOWER_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFEDTARL"

HS300_PE_CANDIDATE_COLUMNS = [
    "滚动市盈率",
    "滚动市盈率中位数",
    "静态市盈率",
    "静态市盈率中位数",
    "等权滚动市盈率",
    "等权静态市盈率",
]


def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


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


def get_tenant_access_token():
    app_id = os.environ["FEISHU_APP_ID"]
    app_secret = os.environ["FEISHU_APP_SECRET"]

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={
            "app_id": app_id,
            "app_secret": app_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")

    return data["tenant_access_token"]


def fetch_us10y_fred():
    resp = requests.get(FRED_DGS10_CSV, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(pd.io.common.StringIO(resp.text))
    df.columns = [str(c).strip() for c in df.columns]

    date_col = None
    value_col = None

    for c in df.columns:
        col = str(c).strip().lower()
        if col in ["date", "observation_date"]:
            date_col = c
        if str(c).strip().upper() == "DGS10":
            value_col = c

    if date_col is None or value_col is None:
        raise ValueError(f"美债字段异常：{list(df.columns)}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    if df.empty:
        raise ValueError("10年期美债收益率数据为空")

    last_row = df.iloc[-1]
    return {
        "10年期美债收益率": safe_float(last_row[value_col]),
        "10年期美债日期": normalize_date(last_row[date_col]),
    }

def fetch_us2y_fred():
    resp = requests.get(FRED_DGS2_CSV, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(pd.io.common.StringIO(resp.text))
    df.columns = [str(c).strip() for c in df.columns]

    date_col = None
    value_col = None

    for c in df.columns:
        col = str(c).strip().lower()
        if col in ["date", "observation_date"]:
            date_col = c
        if str(c).strip().upper() == "DGS2":
            value_col = c

    if date_col is None or value_col is None:
        raise ValueError(f"美国2年期收益率字段异常：{list(df.columns)}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    if df.empty:
        raise ValueError("美国2年期收益率数据为空")

    last_row = df.iloc[-1]
    return {
        "美国2年期收益率": safe_float(last_row[value_col]),
    }

def fetch_fed_rate_target():
    def _read_target_csv(url, value_name):
        resp = requests.get(url, timeout=30)
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
            raise ValueError(f"美联储基准利率字段异常：{list(df.columns)}")

        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df = df.dropna(subset=[value_col])

        if df.empty:
            raise ValueError("美联储基准利率数据为空")

        return df.iloc[-1][value_col]

    lower = _read_target_csv(FRED_FEDFUNDS_TARGET_LOWER_CSV, "DFEDTARL")
    upper = _read_target_csv(FRED_FEDFUNDS_TARGET_UPPER_CSV, "DFEDTARU")

    return {
        "美联储基准利率": f"{safe_float(lower):.2f}-{safe_float(upper):.2f}"
    }

def fetch_yahoo_last_close(symbol: str, value_col_name: str, date_col_name: str):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="10d", interval="1d", auto_adjust=False)

    if hist is None or hist.empty:
        raise ValueError(f"{symbol} 数据为空")

    hist = hist.dropna(subset=["Close"])
    if hist.empty:
        raise ValueError(f"{symbol} 无有效收盘价")

    last_row = hist.iloc[-1]
    last_idx = hist.index[-1]

    return {
        value_col_name: safe_float(last_row["Close"]),
        date_col_name: pd.to_datetime(last_idx).strftime("%Y-%m-%d"),
    }


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
    return {
        "中国社融增量_亿元": safe_float(last_row[value_col]),
        "中国社融月份": str(last_row[month_col]),
    }


def fetch_hs300_pe():
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
    return {
        "沪深300市盈率": safe_float(last_row[pe_col]),
    }


def build_snapshot():
    snapshot = {
    "日期": datetime.now().strftime("%Y-%m-%d"),
    "美联储基准利率": "null",
    "美国2年期收益率": "None",
    "美国10年期收益率": "None",
    "美元指数DXY": "None",
    "WTI原油": "None",
    "铜价": "None",
    "USD/CNH": "None",
    "VIX": "None",
    "中国社融增量_亿元": "None",
    "沪深300市盈率": "None",
    }

    try:
        log("开始抓取：美联储基准利率")
        data = fetch_fed_rate_target()
        snapshot["美联储基准利率"] = data.get("美联储基准利率", "null")
        log("抓取成功：美联储基准利率")
    except Exception as e:
        log(f"抓取失败：美联储基准利率 | {e}")
        log(traceback.format_exc())

    try:
        log("开始抓取：美国2年期收益率")
        data = fetch_us2y_fred()
        snapshot["美国2年期收益率"] = data.get("美国2年期收益率", "None")
        log("抓取成功：美国2年期收益率")
    except Exception as e:
        log(f"抓取失败：美国2年期收益率 | {e}")
        log(traceback.format_exc())
    
    try:
        log("开始抓取：美债")
        data = fetch_us10y_fred()
        snapshot["美国10年期收益率"] = data.get("10年期美债收益率", "None")
        log("抓取成功：美债")
    except Exception as e:
        log(f"抓取失败：美债 | {e}")
        log(traceback.format_exc())

    try:
        log("开始抓取：美元指数")
        data = fetch_yahoo_last_close("DX-Y.NYB", "美元指数DXY", "美元指数日期")
        snapshot["美元指数DXY"] = data.get("美元指数DXY", "None")
        log("抓取成功：美元指数")
    except Exception as e:
        log(f"抓取失败：美元指数 | {e}")
        log(traceback.format_exc())

    try:
        log("开始抓取：布伦特原油")
        data = fetch_yahoo_last_close("BZ=F", "布伦特原油", "布伦特原油日期")
        snapshot["WTI原油"] = data.get("布伦特原油", "None")
        log("抓取成功：布伦特原油")
    except Exception as e:
        log(f"抓取失败：布伦特原油 | {e}")
        log(traceback.format_exc())

    try:
        log("开始抓取：中国社融")
        data = fetch_china_social_financing()
        snapshot["中国社融增量_亿元"] = data.get("中国社融增量_亿元", "None")
        log("抓取成功：中国社融")
    except Exception as e:
        log(f"抓取失败：中国社融 | {e}")
        log(traceback.format_exc())

    try:
        log("开始抓取：沪深300市盈率")
        data = fetch_hs300_pe()
        snapshot["沪深300市盈率"] = data.get("沪深300市盈率", "None")
        log("抓取成功：沪深300市盈率")
    except Exception as e:
        log(f"抓取失败：沪深300市盈率 | {e}")
        log(traceback.format_exc())

    return snapshot


def append_record_to_bitable(snapshot: dict):
    tenant_access_token = get_tenant_access_token()
    app_token = os.environ["FEISHU_BITABLE_APP_TOKEN"]
    table_id = os.environ["FEISHU_BITABLE_TABLE_ID"]

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"

    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "fields": {
            "日期": snapshot.get("日期", "null"),
            "美联储基准利率": snapshot.get("美联储基准利率", "null"),
            "美国2年期收益率": snapshot.get("美国2年期收益率", None),
            "美国10年期收益率": snapshot.get("美国10年期收益率", None),
            "美元指数DXY": snapshot.get("美元指数DXY", None),
            "WTI原油": snapshot.get("WTI原油", None),
            "铜价": snapshot.get("铜价", None),
            "USD/CNH": snapshot.get("USD/CNH", None),
            "VIX": snapshot.get("VIX", None),
            "中国社融增量_亿元": snapshot.get("中国社融增量_亿元", None),
            "沪深300市盈率": snapshot.get("沪深300市盈率", None),
        }
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    data = resp.json()

    if resp.status_code != 200 or data.get("code") != 0:
        raise RuntimeError(f"写入飞书多维表格失败: {data}")

    log("已写入飞书多维表格")


def main():
    log("===== 开始执行 =====")
    snapshot = build_snapshot()
    append_record_to_bitable(snapshot)
    log("===== 执行完成 =====")


if __name__ == "__main__":
    main()
