"""main.py
Cloud Run KIS → BigQuery ETL 파이프라인 (리팩토링)"""

from dotenv import load_dotenv
load_dotenv()

from flask import Flask
import os, json, logging, requests
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

# ──────────────────── GCP & 3rd-party ────────────────────
from google.cloud import secretmanager, firestore
import yfinance as yf
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.auth import default
from googleapiclient.discovery import build
from google.cloud import bigquery
import uuid
import decimal
import re
from httplib2 import Http
from google.oauth2 import service_account


# ──────────────────── 기본 설정 ────────────────────
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

app = Flask(__name__)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────── 공통 리소스 ────────────────────
BQ_PROJECT  = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET  = os.getenv("BQ_DATASET", "portfolio")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")
bq = bigquery.Client(project=BQ_PROJECT)
db = firestore.Client()          # Firestore(토큰 캐시)

def T(name: str) -> str:
    return f"{BQ_PROJECT}.{BQ_DATASET}.{name}"

# ──────────────────── BigQuery 스키마 상수 ────────────────────
ASSETS_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("account_nickname", "STRING"),
    bigquery.SchemaField("account_number", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("exchange_code", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("quantity", "FLOAT64"),
    bigquery.SchemaField("avg_purchase_price", "FLOAT64"),
    bigquery.SchemaField("current_price", "FLOAT64"),
    bigquery.SchemaField("purchase_amount", "FLOAT64"),
    bigquery.SchemaField("eval_amount", "FLOAT64"),
    bigquery.SchemaField("eval_profit_loss_amount", "FLOAT64"),
    bigquery.SchemaField("profit_loss_rate", "FLOAT64"),
    bigquery.SchemaField("currency", "STRING"),
]

ACCOUNT_BALANCES_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("account_nickname", "STRING"),
    bigquery.SchemaField("account_number", "STRING"),
    bigquery.SchemaField("total_assets_amount", "FLOAT64"),
    bigquery.SchemaField("net_assets_amount", "FLOAT64"),
    bigquery.SchemaField("total_deposit_amount", "FLOAT64"),
    bigquery.SchemaField("deposit_amount", "FLOAT64"),
    bigquery.SchemaField("cma_eval_amount", "FLOAT64"),
    bigquery.SchemaField("total_eval_amount", "FLOAT64"),
]

ORDERS_SCHEMA = [
    bigquery.SchemaField("account_nickname", "STRING"),
    bigquery.SchemaField("account_number", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("order_number", "STRING"),
    bigquery.SchemaField("market", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("order_type", "STRING"),
    bigquery.SchemaField("total_quantity", "FLOAT64"),
    bigquery.SchemaField("avg_price", "FLOAT64"),
    bigquery.SchemaField("total_amount", "FLOAT64"),
    bigquery.SchemaField("currency", "STRING"),
]

DAILY_STOCK_PRICES_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("close_price", "FLOAT64"),
    bigquery.SchemaField("open_price", "FLOAT64"),
    bigquery.SchemaField("high_price", "FLOAT64"),
    bigquery.SchemaField("low_price", "FLOAT64"),
    bigquery.SchemaField("volume", "FLOAT64"),
]

DIVIDEND_HISTORY_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("ex_dividend_date", "DATE"),
    bigquery.SchemaField("record_date", "DATE"),
    bigquery.SchemaField("payment_date", "DATE"),
    bigquery.SchemaField("dividend_per_share", "FLOAT64"),
]

DAILY_EXCHANGE_RATES_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("rate", "FLOAT64"),
]

RISK_FREE_RATES_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("rate", "FLOAT64"),
]

ANALYSIS_SCHEMA = [
    bigquery.SchemaField("inquiry_date", "DATE"),
    bigquery.SchemaField("account_nickname", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("asset_class", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("quantity", "FLOAT64"),
    bigquery.SchemaField("avg_purchase_price", "FLOAT64"),
    bigquery.SchemaField("purchase_amount", "FLOAT64"),
    bigquery.SchemaField("current_price", "FLOAT64"),
    bigquery.SchemaField("eval_amount", "FLOAT64"),
    bigquery.SchemaField("usd_krw_rate", "FLOAT64"),
    bigquery.SchemaField("eval_amount_krw", "FLOAT64"),
    bigquery.SchemaField("eval_profit_loss_amount_krw", "FLOAT64"),
    bigquery.SchemaField("daily_dividend_income_krw", "FLOAT64"),
    bigquery.SchemaField("buy_amount_krw", "FLOAT64"),
    bigquery.SchemaField("sell_amount_krw", "FLOAT64"),
    bigquery.SchemaField("buy_avg_price", "FLOAT64"),
    bigquery.SchemaField("sell_avg_price", "FLOAT64"),
    bigquery.SchemaField(
        "weights", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("strategy", "STRING"),
            bigquery.SchemaField("weight", "FLOAT64"),
        ],
    ),
]

ASSET_CLASS_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("asset_class", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("exchange_code", "STRING"),
]

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "account_balances": ACCOUNT_BALANCES_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "daily_stock_prices": DAILY_STOCK_PRICES_SCHEMA,
    "dividend_history": DIVIDEND_HISTORY_SCHEMA,
    "daily_exchange_rates": DAILY_EXCHANGE_RATES_SCHEMA,
    "risk_free_rates": RISK_FREE_RATES_SCHEMA,
    "analysis": ANALYSIS_SCHEMA,
    "asset_class": ASSET_CLASS_SCHEMA,
}

def schema_to_ddl(schema_list: list[bigquery.SchemaField]) -> str:
    def field_to_sql(f: bigquery.SchemaField) -> str:
        if f.field_type.upper() == "RECORD":
            inner = ", ".join([field_to_sql(child) for child in f.fields])
            struct_sql = f"STRUCT<{inner}>"
            if f.mode == "REPEATED":
                return f"{f.name} ARRAY<{struct_sql}>"
            else:
                return f"{f.name} {struct_sql}"
        else:
            mode_sql = ""  
            return f"{f.name} {f.field_type}{mode_sql}"
    return ",\n  ".join(field_to_sql(f) for f in schema_list)

# ──────────────────── 공통 BQ 유틸 ────────────────────
def _sanitize_df_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(lambda x: x.decode() if isinstance(x, (bytes, bytearray)) else x)
    if "inquiry_date" in df.columns:
        df["inquiry_date"] = pd.to_datetime(df["inquiry_date"], errors="coerce").dt.date
    return df

def bq_load_df(table: str, df: pd.DataFrame, write_disposition="WRITE_APPEND"):
    if df is None or df.empty:
        return
    df = _sanitize_df_for_bq(df)

    table_name = table.split('.')[-1]
    schema = TABLE_SCHEMAS.get(table_name)  

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
        autodetect=(schema is None),
    )
    job = bq.load_table_from_dataframe(df, table, job_config=job_config, location=BQ_LOCATION)
    job.result()

def _bq_param(k, v):
    if isinstance(v, bool):
        t = "BOOL"
    elif isinstance(v, int):
        t = "INT64"
    elif isinstance(v, float) or isinstance(v, decimal.Decimal):
        t = "FLOAT64"
    elif isinstance(v, datetime):
        t = "TIMESTAMP"
    elif isinstance(v, date):
        t = "DATE"
    else:
        t = "STRING"
    return bigquery.ScalarQueryParameter(k, t, v)

def bq_exec(sql: str, params: dict | None = None):
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = [_bq_param(k, v) for k, v in params.items()]
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    job.result()

def bq_temp_table(suffix: str) -> str:
    return f"{BQ_PROJECT}.{BQ_DATASET}._stg_{suffix}_{uuid.uuid4().hex[:8]}"


# ──────────────────── BigQuery 준비(데이터셋/테이블/제약) ────────────────────
def ensure_all_tables():
    ds_id = f"{BQ_PROJECT}.{BQ_DATASET}"
    try:
        bq.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds, exists_ok=True)

    for tbl in [
        "asset_class",
        "assets",
        "account_balances",
        "orders",
        "daily_stock_prices",
        "dividend_history",
        "daily_exchange_rates",
        "risk_free_rates",
        "analysis",
    ]:
        schema = TABLE_SCHEMAS[tbl]
        bq_exec(f"""
            CREATE TABLE IF NOT EXISTS `{T(tbl)}` (
              {schema_to_ddl(schema)}
            );
        """)

    bq_exec(f"""
    DECLARE has_pk BOOL;
    SET has_pk = (
    SELECT COUNT(*) > 0
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS`
    WHERE table_name = 'asset_class'
        AND constraint_type = 'PRIMARY KEY'
    );

    IF NOT has_pk THEN
    EXECUTE IMMEDIATE 'ALTER TABLE `{T('asset_class')}` ADD PRIMARY KEY (ticker) NOT ENFORCED';
    END IF;
    """)

    logger.info("모든 BigQuery 테이블/제약 준비 완료.")

if os.getenv("INIT_ON_IMPORT") == "1":
    try:
        ensure_all_tables()
        logger.info("BigQuery 초기화 완료(모듈 로드 시).")
    except Exception as e:
        logger.exception("BigQuery 초기화 실패: %s", e)

# ──────────────────── BQ 준비 run-once 가드 ────────────────────
_BQ_READY = False

def ensure_bq_ready_once():
    global _BQ_READY
    if not _BQ_READY:
        ensure_all_tables()
        _BQ_READY = True

# ──────────────────── 시장 데이터 헬퍼 (KRX/pykrx) ────────────────────
def _is_krx_numeric(tk: str) -> bool:
    tk = str(tk)
    return tk.isdigit() and (5 <= len(tk) <= 6)

def _krx_numeric_code(tk: str) -> str | None:
    s = re.sub(r"\D", "", str(tk))  
    return s if s.isdigit() and (5 <= len(s) <= 6) else None

def _is_krx_candidate(tk: str) -> bool:
    return _krx_numeric_code(tk) is not None

def _fetch_krx_ohlcv(ticker: str, start_d: date, end_d: date) -> pd.DataFrame:
    try:
        from pykrx import stock as krx  # lazy import
    except Exception as e:
        logger.warning("pykrx import failed: %s", e)
        return pd.DataFrame()
    s = start_d.strftime("%Y%m%d")
    e = end_d.strftime("%Y%m%d")
    df = krx.get_market_ohlcv_by_date(s, e, ticker)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"시가":"open_price","고가":"high_price","저가":"low_price",
                            "종가":"close_price","거래량":"volume"})
    df = df.reset_index().rename(columns={"날짜":"inquiry_date"})
    df["inquiry_date"] = pd.to_datetime(df["inquiry_date"]).dt.date
    df["ticker"] = str(ticker)
    return df[["inquiry_date","ticker","close_price","open_price","high_price","low_price","volume"]]

# ──────────────────── KIS 토큰 · API 헬퍼 ────────────────────
def get_kis_configs() -> dict:
    project_id = (
        os.getenv("GCP_PROJECT_ID") or
        os.getenv("GOOGLE_CLOUD_PROJECT") or
        os.getenv("PROJECT_ID")
    )
    if not project_id:  
        import google.auth
        _, project_id = google.auth.default()

    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project_id}/secrets/KISAPI/versions/latest"
    resp   = client.access_secret_version(request={"name": name})
    return json.loads(resp.payload.data.decode())

def get_new_kis_token(app_key:str, app_secret:str) -> dict|None:
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    try:
        r = requests.post(url, json={
            "grant_type":"client_credentials",
            "appkey":app_key, "appsecret":app_secret}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return {
            "token": data["access_token"],
            "expires_at": datetime.now(timezone.utc)
                         + timedelta(seconds=int(data["expires_in"]) - 60)
        }
    except Exception as e:
        logger.error("토큰 발급 실패(appkey …%s): %s", app_key[-4:], e)
        return None

def get_or_refresh_token(app_key: str, app_secret: str) -> str | None:
    doc = db.collection("api_tokens").document(f"kis_token_{app_key}")

    # 캐시 확인
    try:
        snap = doc.get()
        if snap.exists:
            td = snap.to_dict()
            expires_at = td["expires_at"]
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at > datetime.now(timezone.utc):
                return td["token"]
    except Exception as e:
        logger.warning("토큰 캐시 조회 실패: %s", e)

    new = get_new_kis_token(app_key, app_secret)
    if not new:
        return None

    try:
        doc.set(new)
    except Exception as e:
        logger.warning("토큰 캐시 저장 실패: %s", e)

    return new["token"]

def fetch_kis_api(url:str, headers:dict, params:dict) -> dict|None:
    """연속조회 자동 처리"""
    out = {"output1": [], "output2": []}
    first, orig = True, params.copy()
    ctx_fk = "CTX_AREA_FK100" if "CTX_AREA_FK100" in params else "CTX_AREA_FK200"
    ctx_nk = "CTX_AREA_NK100" if "CTX_AREA_NK100" in params else "CTX_AREA_NK200"

    while True:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            logger.error("HTTP %s", r.status_code); return None
        d = r.json()
        if d.get("rt_cd") != "0":
            if d.get("msg_cd") not in ("EGW00121","APBK0013"):
                logger.warning("KIS 오류 %s(%s)", d.get("msg1"), headers["tr_id"])
            break
        if first and d.get("output2"):
            out["output2"] = d["output2"]; first=False
        if d.get("output1"):
            out["output1"].extend(d["output1"])
        if r.headers.get("tr_cont") in ("F","M"):
            params = orig.copy(); params["tr_cont"] = "N"
            params[ctx_fk], params[ctx_nk] = d.get(ctx_fk), d.get(ctx_nk)
            if not params[ctx_fk] or not params[ctx_nk]:
                break
        else:
            break
    return out if out["output1"] or out["output2"] else None

# ──────────────────── 1) KIS 동기화 ────────────────────
def sync_kis_data(event: dict = {}, context=None):
    ensure_bq_ready_once()
    today     = datetime.now(ZoneInfo("Asia/Seoul")).date()
    start_dt  = (today - timedelta(days=30)).strftime("%Y%m%d")
    end_dt    = today.strftime("%Y%m%d")
    accounts  = get_kis_configs().get("ACCOUNTS", [])
    asset_buf: list[dict] = []
    balances_buf: list[dict] = []
    orders_buf: list[dict] = []

    asset_keys: set[tuple] = set()        
    processed_balance: set[str] = set()

    for acc in accounts:
        nickname, cano = acc.get("nickname"), acc.get("cano")
        if not cano:
            continue

        token = get_or_refresh_token(acc["app_key"], acc["app_secret"])
        if not token:
            logger.warning("토큰 없음: %s", nickname)
            continue

        h = {
            "authorization": f"Bearer {token}",
            "appkey": acc["app_key"],
            "appsecret": acc["app_secret"],
            "tr_id": "",
        }
        prdt_cd = acc.get("prdt_cd", "01")

        # CMA 계좌
        if prdt_cd == "21":
            h["tr_id"] = "CTRP6548R"
            res = fetch_kis_api(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-account-balance",
                h, {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "INQR_DVSN_1":"", "BSPR_BF_DT_APLY_YN":""}
            )
            total_cma_asset_value = 0.0
            if res and res.get("output2"):
                b = res["output2"][0] if isinstance(res["output2"], list) else res["output2"]
                cash_amount     = float(b.get("dncl_amt", 0)      or 0)
                cma_eval_amount = float(b.get("cma_evlu_amt", 0)  or 0)
                total_cma_asset_value = cash_amount + cma_eval_amount
            if total_cma_asset_value == 0 and res and res.get("output1"):
                for item in res["output1"]:
                    if item.get("prdt_name") == "예수금+cma":
                        total_cma_asset_value = float(item.get("evlu_amt", 0) or 0); break
            if total_cma_asset_value > 0:
                key = (today, cano, "CMA_TOTAL_KRW")
                if key not in asset_keys:
                    asset_keys.add(key)
                    asset_buf.append(dict(
                        inquiry_date=today, account_nickname=nickname, account_number=cano,
                        ticker="CMA_TOTAL_KRW", product_name="CMA 예수금+평가금액",
                        quantity=1, avg_purchase_price=total_cma_asset_value,
                        current_price=total_cma_asset_value, purchase_amount=total_cma_asset_value,
                        eval_amount=total_cma_asset_value, eval_profit_loss_amount=0,
                        profit_loss_rate=0, currency="KRW", exchange_code=None,
                    ))
            continue

        # 국내 보유
        h["tr_id"] = "TTTC8434R"
        res_dom = fetch_kis_api(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance",
            h,
            {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "AFHR_FLPR_YN":"N", "OFL_YN":"",
             "INQR_DVSN":"02", "UNPR_DVSN":"01", "FUND_STTL_ICLD_YN":"N", "FNCG_AMT_AUTO_RDPT_YN":"N",
             "PRCS_DVSN":"01", "CTX_AREA_FK100":"", "CTX_AREA_NK100":""}
        )
        if res_dom:
            for r in res_dom["output1"]:
                if int(r["hldg_qty"]):
                    key = (today, cano, r["pdno"])
                    if key not in asset_keys:
                        asset_keys.add(key)
                        asset_buf.append(dict(
                            inquiry_date=today, account_nickname=nickname, account_number=cano,
                            ticker=r["pdno"], product_name=r["prdt_name"], exchange_code="KRX",
                            quantity=r["hldg_qty"], avg_purchase_price=r["pchs_avg_pric"],
                            current_price=r["prpr"], purchase_amount=r["pchs_amt"],
                            eval_amount=r["evlu_amt"], eval_profit_loss_amount=r["evlu_pfls_amt"],
                            profit_loss_rate=float(r["evlu_pfls_rt"] or 0), currency="KRW",
                        ))
            if res_dom["output2"]:
                cash = float(res_dom["output2"][0]["dnca_tot_amt"] or 0)
                if cash:
                    key = (today, cano, "CASH_KRW")
                    if key not in asset_keys:
                        asset_keys.add(key)
                        asset_buf.append(dict(
                            inquiry_date=today, account_nickname=nickname, account_number=cano,
                            ticker="CASH_KRW", product_name="예수금 (KRW)",
                            quantity=1, avg_purchase_price=cash, current_price=cash,
                            purchase_amount=cash, eval_amount=cash, eval_profit_loss_amount=0,
                            profit_loss_rate=0, currency="KRW", exchange_code=None,
                        ))

        # 해외 보유
        ex2cur = {"NASD":"USD","NYSE":"USD","AMEX":"USD","SEHK":"HKD","TSE":"JPY"}
        for excg in ("NASD", "NYSE", "AMEX", "SEHK", "TSE"):
            h["tr_id"] = "TTTS3012R"
            res_ov = fetch_kis_api(
                f"{KIS_BASE_URL}/uapi/overseas-stock/v1/trading/inquire-balance",
                h, {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "OVRS_EXCG_CD": excg,
                    "TR_CRCY_CD": ex2cur.get(excg, "USD"), "CTX_AREA_FK200":"", "CTX_AREA_NK200":""}
            )
            if not res_ov or not res_ov.get("output1"):
                continue
            for r in res_ov["output1"]:
                if int(r.get("ovrs_cblc_qty", 0)):
                    key = (today, cano, r["ovrs_pdno"])
                    if key not in asset_keys:
                        asset_keys.add(key)
                        asset_buf.append(dict(
                            inquiry_date=today, account_nickname=nickname, account_number=cano,
                            ticker=r["ovrs_pdno"], exchange_code=excg, product_name=r["ovrs_item_name"],
                            quantity=r["ovrs_cblc_qty"], avg_purchase_price=r["pchs_avg_pric"],
                            current_price=r["now_pric2"], purchase_amount=r["frcr_pchs_amt1"],
                            eval_amount=r["ovrs_stck_evlu_amt"], eval_profit_loss_amount=r["frcr_evlu_pfls_amt"],
                            profit_loss_rate=float(r["evlu_pfls_rt"] or 0), currency=ex2cur.get(excg,"USD"),
                        ))

        # 계좌별 종합잔고(하루 1회)
        if cano not in processed_balance:
            h["tr_id"] = "CTRP6548R"
            res_bal = fetch_kis_api(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-account-balance",
                h, {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "INQR_DVSN_1":"", "BSPR_BF_DT_APLY_YN":""}
            )
            bal = res_bal.get("output2")
            if isinstance(bal, dict): bal = [bal]
            if bal:
                b = bal[0]
                balances_buf.append({
                    "inquiry_date": today,
                    "account_nickname": nickname,
                    "account_number": cano,
                    "total_assets_amount": b.get("tot_asst_amt"),
                    "net_assets_amount": b.get("nass_tot_amt"),
                    "total_deposit_amount": b.get("tot_dncl_amt"),
                    "deposit_amount": b.get("dncl_amt"),
                    "cma_eval_amount": b.get("cma_evlu_amt"),
                    "total_eval_amount": b.get("evlu_amt_smtl"),
                })
            processed_balance.add(cano)

        # 주문(국내)
        h["tr_id"] = "TTTC0081R"
        res_ord = fetch_kis_api(
            f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            h,
            {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "INQR_STRT_DT": start_dt, "INQR_END_DT": end_dt,
             "ORD_GNO_BRNO":"", "ODNO":"", "INQR_DVSN_3":"00", "INQR_DVSN_1":"", "SLL_BUY_DVSN_CD":"00",
             "INQR_DVSN":"00", "CCLD_DVSN":"01", "PDNO":"", "CTX_AREA_FK100":"", "CTX_AREA_NK100":""}
        )
        if res_ord:
            for o in res_ord["output1"]:
                orders_buf.append({
                    "account_nickname": nickname,
                    "account_number": cano,
                    "order_date": datetime.strptime(o["ord_dt"], "%Y%m%d").date(),
                    "order_number": o["odno"],
                    "market": "국내",
                    "ticker": o["pdno"],
                    "product_name": o["prdt_name"],
                    "order_type": o["sll_buy_dvsn_cd_name"],
                    "total_quantity": o["tot_ccld_qty"],
                    "avg_price": o["avg_prvs"],
                    "total_amount": o["tot_ccld_amt"],
                    "currency": "KRW",
                })

        # 주문(해외)
        h["tr_id"] = "CTOS4001R"
        res_ovord = fetch_kis_api(
            f"{KIS_BASE_URL}/uapi/overseas-stock/v1/trading/inquire-period-trans",
            h,
            {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "ERLM_STRT_DT": start_dt, "ERLM_END_DT": end_dt,
             "OVRS_EXCG_CD":"", "PDNO":"", "SLL_BUY_DVSN_CD":"00", "LOAN_DVSN_CD":"",
             "CTX_AREA_FK100":"", "CTX_AREA_NK100":""}
        )
        if res_ovord:
            for o in res_ovord["output1"]:
                od_id = f"{o['trad_dt']}-{o['pdno']}-{o['ccld_qty']}-{o['tr_frcr_amt2']}"
                orders_buf.append({
                    "account_nickname": nickname,
                    "account_number": cano,
                    "order_date": datetime.strptime(o["trad_dt"], "%Y%m%d").date(),
                    "order_number": od_id,
                    "market": "해외",
                    "ticker": o["pdno"],
                    "product_name": o["ovrs_item_name"],
                    "order_type": o["sll_buy_dvsn_name"],
                    "total_quantity": o["ccld_qty"],
                    "avg_price": o["ovrs_stck_ccld_unpr"],
                    "total_amount": o["tr_frcr_amt2"],
                    "currency": o["crcy_cd"],
                })

    # ---------- BigQuery 적재 ----------
    # assets: 오늘자 삭제 후 적재
    if asset_buf:
        assets_df = pd.DataFrame(asset_buf)
        for c in ["quantity","avg_purchase_price","current_price","purchase_amount",
                  "eval_amount","eval_profit_loss_amount","profit_loss_rate"]:
            if c in assets_df.columns: assets_df[c] = pd.to_numeric(assets_df[c], errors="coerce")
        assets_df["inquiry_date"] = pd.to_datetime(assets_df["inquiry_date"]).dt.date
        bq_exec(f"DELETE FROM `{T('assets')}` WHERE inquiry_date = @d", {"d": today})
        bq_load_df(T("assets"), assets_df)

    # account_balances: 오늘자 삭제 후 적재
    if balances_buf:
        bal_df = pd.DataFrame(balances_buf)
        for c in ["total_assets_amount","net_assets_amount","total_deposit_amount",
                  "deposit_amount","cma_eval_amount","total_eval_amount"]:
            bal_df[c] = pd.to_numeric(bal_df[c], errors="coerce")
        bal_df["inquiry_date"] = pd.to_datetime(bal_df["inquiry_date"]).dt.date
        bq_exec(f"DELETE FROM `{T('account_balances')}` WHERE inquiry_date = @d", {"d": today})
        bq_load_df(T("account_balances"), bal_df)

    # orders: MERGE
    if orders_buf:
        odf = pd.DataFrame(orders_buf)
        for c in ["total_quantity", "avg_price", "total_amount"]:
            odf[c] = pd.to_numeric(odf[c], errors="coerce")
        odf["order_date"] = pd.to_datetime(odf["order_date"]).dt.date
        stg = bq_temp_table("orders")
        bq_load_df(stg, odf, write_disposition="WRITE_TRUNCATE")
        bq_exec(f"""
            MERGE `{T('orders')}` T
            USING `{stg}` S
            ON  T.account_number = S.account_number
            AND T.order_number   = S.order_number
            AND T.market         = S.market
            WHEN MATCHED THEN UPDATE SET
              account_nickname = S.account_nickname,
              order_date       = S.order_date,
              ticker           = S.ticker,
              product_name     = S.product_name,
              order_type       = S.order_type,
              total_quantity   = S.total_quantity,
              avg_price        = S.avg_price,
              total_amount     = S.total_amount,
              currency         = S.currency
            WHEN NOT MATCHED THEN INSERT (
              account_nickname, account_number, order_date, order_number, market,
              ticker, product_name, order_type, total_quantity, avg_price, total_amount, currency
            ) VALUES (
              S.account_nickname, S.account_number, S.order_date, S.order_number, S.market,
              S.ticker, S.product_name, S.order_type, S.total_quantity, S.avg_price, S.total_amount, S.currency
            );
        """)
        bq_exec(f"DROP TABLE `{stg}`")

    logger.info("KIS 동기화 완료(BigQuery only)")
    return "OK", 200

# ──────────────────── 2) 시장 데이터 동기화 ────────────────────
def _select_valid_yf_ticker(symbol:str) -> yf.Ticker|None:
    for suf in ("", ".KS", ".KQ"):
        t = yf.Ticker(symbol + suf)
        try:
            if not t.history(period="1d").empty:
                return t
        except Exception:
            pass
    return None

HIST_TICKERS    = ["SPY", "QQQ", "TLT", "GLD"]
HIST_START_DATE = date(2000, 1, 1)

def sync_market_data():
    ensure_bq_ready_once()
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    # 0) BigQuery에서 보유 티커 추출
    try:
        df_tk = bq.query(
            f"""
            SELECT DISTINCT ticker
            FROM `{T('assets')}`
            WHERE ticker NOT IN ('CASH_KRW','CMA_TOTAL_KRW','ISSUED_NOTE_KRW')
            """,
            location=BQ_LOCATION
        ).to_dataframe()
        tickers_from_assets = df_tk["ticker"].dropna().astype(str).tolist()
    except Exception:
        tickers_from_assets = []

    tickers = sorted(set(tickers_from_assets) | set(HIST_TICKERS))

    # 1) 모든 가격 정보를 담을 리스트(이번 변경의 핵심)
    all_prices_to_update: list[dict] = []

    for tk in tickers:
        # --- (A) KRX(국내) 처리 ---
        if _is_krx_candidate(tk):
            code = _krx_numeric_code(tk)
            last_p = None
            try:
                rows = list(
                    bq.query(
                        f"""
                        SELECT MAX(inquiry_date) AS max_date
                        FROM `{T('daily_stock_prices')}`
                        WHERE ticker=@tk
                        """,
                        job_config=bigquery.QueryJobConfig(
                            query_parameters=[_bq_param("tk", tk)]
                        ),
                        location=BQ_LOCATION,
                    ).result()
                )
                if rows and rows[0]["max_date"]:
                    last_p = rows[0]["max_date"]
            except Exception as e:
                if "Not found" not in str(e):
                    logger.warning("last price date query failed for %s: %s", tk, e)

            start_p = (last_p or (HIST_START_DATE - timedelta(days=1))) + timedelta(days=1)
            if start_p < today:
                krx_df = _fetch_krx_ohlcv(code, start_p, today)  
                if krx_df is not None and not krx_df.empty:
                    rename_map = {
                        "date": "inquiry_date", "Date": "inquiry_date", "trade_date": "inquiry_date",
                        "open": "open_price", "Open": "open_price",
                        "high": "high_price", "High": "high_price",
                        "low": "low_price", "Low": "low_price",
                        "close": "close_price", "Close": "close_price",
                        "volume": "volume", "Volume": "volume",
                    }
                    krx_df = krx_df.rename(columns=rename_map)
                    for col in ["open_price","high_price","low_price","close_price","volume"]:
                        if col not in krx_df.columns:
                            krx_df[col] = None
                    if "inquiry_date" not in krx_df.columns:
                        if isinstance(krx_df.index, pd.DatetimeIndex):
                            krx_df["inquiry_date"] = krx_df.index.date
                        else:
                            raise ValueError("KRX DF에 inquiry_date 컬럼이 없습니다.")
                    krx_df["ticker"] = tk

                    krx_df["inquiry_date"] = pd.to_datetime(krx_df["inquiry_date"]).dt.date
                    for c in ["open_price","high_price","low_price","close_price","volume"]:
                        if c in krx_df.columns:
                            krx_df[c] = pd.to_numeric(krx_df[c], errors="coerce")
                    krx_df = krx_df.dropna(subset=["inquiry_date"])
                    krx_df = krx_df.drop_duplicates(subset=["ticker","inquiry_date"], keep="last")

                    if last_p is not None:
                        krx_df = krx_df[krx_df["inquiry_date"] > last_p]

                    if not krx_df.empty:
                        all_prices_to_update.extend(krx_df[
                            ["inquiry_date","ticker","open_price","high_price","low_price","close_price","volume"]
                        ].to_dict("records"))
            continue  

        # --- (B) 해외(yfinance) 처리 ---
        yf_t = _select_valid_yf_ticker(tk)
        if yf_t is None:
            logger.warning("yfinance 티커 인식 실패: %s", tk)
            continue

        last_p = None
        try:
            r = list(
                bq.query(
                    f"""
                    SELECT MAX(inquiry_date)
                    FROM `{T('daily_stock_prices')}`
                    WHERE ticker = @tk
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[_bq_param("tk", tk)]
                    ),
                    location=BQ_LOCATION,
                ).result()
            )
            if r and r[0][0]:
                last_p = r[0][0]
        except Exception as e:
            if "Not found" not in str(e):
                raise

        start_p = (last_p or (HIST_START_DATE - timedelta(days=1))) + timedelta(days=1)
        if start_p < today:
            hist = yf_t.history(start=start_p, auto_adjust=False)
            if hist is not None and not hist.empty:
                recs = [
                    dict(
                        inquiry_date=idx.date(), ticker=tk,
                        open_price=row.get("Open"),  high_price=row.get("High"),
                        low_price=row.get("Low"),    close_price=row.get("Close"),
                        volume=row.get("Volume"),
                    )
                    for idx, row in hist.iterrows()
                    if isinstance(idx, (pd.Timestamp, datetime)) and idx.date() > (last_p or (start_p - timedelta(days=1)))
                ]
                if recs:
                    all_prices_to_update.extend(recs)

    # 2) 모아둔 가격을 한 번에 MERGE
    if all_prices_to_update:
        logger.info(f"총 {len(all_prices_to_update)}개의 일별 시세 후보 레코드 수집. 정규화 및 MERGE 수행.")
        prices_df = pd.DataFrame(all_prices_to_update)

        prices_df["inquiry_date"] = pd.to_datetime(prices_df["inquiry_date"]).dt.date
        prices_df["ticker"] = prices_df["ticker"].astype(str)
        for c in ["open_price","high_price","low_price","close_price","volume"]:
            if c in prices_df.columns:
                prices_df[c] = pd.to_numeric(prices_df[c], errors="coerce")

        prices_df = prices_df.dropna(subset=["inquiry_date","ticker","close_price"])
        prices_df = prices_df.drop_duplicates(subset=["ticker","inquiry_date"], keep="last")

        if not prices_df.empty:
            prices_df = prices_df[[
                "inquiry_date","ticker","open_price","high_price","low_price","close_price","volume"
            ]]
            stg_prices_table = bq_temp_table("daily_stock_prices")
            bq_load_df(stg_prices_table, prices_df, "WRITE_TRUNCATE")
           
            bq_exec(f"""
                MERGE `{T('daily_stock_prices')}` AS T
                USING `{stg_prices_table}` AS S
                ON T.ticker = S.ticker AND T.inquiry_date = S.inquiry_date
                WHEN MATCHED THEN UPDATE SET
                    T.open_price  = S.open_price,
                    T.high_price  = S.high_price,
                    T.low_price   = S.low_price,
                    T.close_price = S.close_price,
                    T.volume      = S.volume
                WHEN NOT MATCHED THEN INSERT
                    (inquiry_date, ticker, open_price, high_price, low_price, close_price, volume)
                VALUES
                    (S.inquiry_date, S.ticker, S.open_price, S.high_price, S.low_price, S.close_price, S.volume)
            """)
            bq_exec(f"DROP TABLE `{stg_prices_table}`")
            logger.info("일별 시세 MERGE 완료.")
        else:
            logger.info("정규화 이후 MERGE할 일별 시세 데이터가 없습니다.")
    else:
        logger.info("새로 업데이트할 일별 시세 데이터가 없습니다.")

    # 3) 배당 
    for tk in tickers:
        yf_t = _select_valid_yf_ticker(tk)
        if yf_t is None:
            continue
        divs = getattr(yf_t, "dividends", None)
        if divs is None or divs.empty:
            continue

        drec = [
            dict(ticker=tk, ex_dividend_date=dt.date(), dividend_per_share=float(val))
            for dt, val in divs.items()
            if pd.notna(val)
        ]
        if not drec:
            continue

        div_df = pd.DataFrame(drec)
        div_df["ex_dividend_date"] = pd.to_datetime(div_df["ex_dividend_date"]).dt.date
        div_df["ticker"] = div_df["ticker"].astype(str)
        div_df["dividend_per_share"] = pd.to_numeric(div_df["dividend_per_share"], errors="coerce")

        stg = bq_temp_table("dividends")
        bq_load_df(stg, div_df, "WRITE_TRUNCATE")
        bq_exec(f"""
            MERGE `{T('dividend_history')}` AS T
            USING `{stg}` AS S
            ON T.ticker = S.ticker AND T.ex_dividend_date = S.ex_dividend_date
            WHEN MATCHED THEN UPDATE SET
                T.dividend_per_share = S.dividend_per_share
            WHEN NOT MATCHED THEN INSERT (ticker, ex_dividend_date, dividend_per_share)
            VALUES (S.ticker, S.ex_dividend_date, S.dividend_per_share)
        """)
        bq_exec(f"DROP TABLE `{stg}`")

    # 4) 환율 
    last_fx = None
    try:
        r = list(
            bq.query(
                f"SELECT MAX(inquiry_date) FROM `{T('daily_exchange_rates')}`",
                location=BQ_LOCATION
            ).result()
        )
        if r and r[0][0]:
            last_fx = r[0][0]
    except Exception as e:
        if "Not found" not in str(e):
            raise

    if last_fx is None:
        last_fx = HIST_START_DATE - timedelta(days=1)
    start_fx = last_fx + timedelta(days=1)

    if start_fx <= today:
        fx_df = yf.download("KRW=X", start=start_fx, auto_adjust=False, progress=False)
        if fx_df is not None and not fx_df.empty:
            close_series = (
                fx_df["Close"]["KRW=X"]
                if isinstance(fx_df.columns, pd.MultiIndex) else fx_df["Close"]
            )
            close_series = close_series.dropna()
            fxrec = [
                {"inquiry_date": idx.date(), "rate": float(val)}
                for idx, val in close_series.items()
                if isinstance(idx, (pd.Timestamp, datetime)) and idx.date() > last_fx
            ]
            if fxrec:
                tmp = bq_temp_table("fx")
                tmp_df = pd.DataFrame(fxrec)
                tmp_df["inquiry_date"] = pd.to_datetime(tmp_df["inquiry_date"]).dt.date
                tmp_df["rate"] = pd.to_numeric(tmp_df["rate"], errors="coerce")
                bq_load_df(tmp, tmp_df, "WRITE_TRUNCATE")
                bq_exec(f"""
                    MERGE `{T('daily_exchange_rates')}` AS T
                    USING `{tmp}` AS S
                    ON T.inquiry_date = S.inquiry_date
                    WHEN MATCHED THEN UPDATE SET T.rate = S.rate
                    WHEN NOT MATCHED THEN INSERT (inquiry_date, rate)
                    VALUES (S.inquiry_date, S.rate)
                """)
                bq_exec(f"DROP TABLE `{tmp}`")

    logger.info("시장 데이터 동기화 완료(BigQuery only)")
    return "OK", 200

# ──────────────────── 3) Google Sheet → DataFrame ────────────────────
def get_manual_data_from_sheet(spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        if os.path.exists("service_account.json"):
            creds = service_account.Credentials.from_service_account_file("service_account.json", scopes=scopes)
        else:
            creds, _ = default(scopes=scopes)
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        rows = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=sheet_name
        ).execute().get("values", [])
        if len(rows) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(rows[1:], columns=rows[0])
        id_vars = ["account_nickname", "currency", "asset_class", "product_name", "ticker"]
        if "purchase_amount" in df.columns:
            id_vars.append("purchase_amount")
        melted = df.melt(id_vars=id_vars, var_name="inquiry_date", value_name="eval_amount_krw")
        melted = melted[melted["eval_amount_krw"].astype(str).str.strip() != ""]
        melted["inquiry_date"] = pd.to_datetime(melted["inquiry_date"], errors="coerce").dt.date
        melted.dropna(subset=["inquiry_date"], inplace=True)
        for c in ("eval_amount_krw", "purchase_amount"):
            if c in melted.columns:
                melted[c] = pd.to_numeric(melted[c].astype(str).str.replace(",", ""), errors="coerce")
        return melted
    except Exception as e:
        logger.error("시트 읽기 실패: %s", e)
        return pd.DataFrame()

# ──────────────────── 4) 국채금리 동기화 ────────────────────
def sync_risk_free_rate():
    ensure_bq_ready_once()
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    last_date = None
    try:
        r = list(bq.query(f"SELECT MAX(inquiry_date) FROM `{T('risk_free_rates')}`", location=BQ_LOCATION).result())
        if r and r[0][0]: last_date = r[0][0]
    except Exception as e:
        if "Not found" not in str(e): raise
    if last_date is None:
        last_date = date(2000,1,1) - timedelta(days=1)

    start_date = last_date + timedelta(days=1)
    end_date   = today + timedelta(days=1)
    
    if start_date >= end_date:
        logger.info("국채 금리 최신 상태 (start=%s, end=%s)", start_date, end_date)
        return "OK", 200

    try:
        df = yf.download("^IRX", start=start_date, end=end_date, auto_adjust=False, progress=False, threads=False)
        if df is None or df.empty:
            logger.warning("^IRX 수신 실패 데이터 없음 (start=%s, end=%s)", start_date, end_date)
            return "NO_DATA", 200

        ser = (df["Close"]["^IRX"] if isinstance(df.columns, pd.MultiIndex) else df["Close"]).astype(float).dropna() / 100.0
        records = [{"inquiry_date": idx.date(), "rate": float(val)} for idx, val in ser.items() if idx.date() >= start_date]
        if not records:
            logger.info("국채 금리 최신 상태")
            return "OK", 200

        stg = bq_temp_table("rfr")
        bq_load_df(stg, pd.DataFrame(records), "WRITE_TRUNCATE")
        bq_exec(f"""
            MERGE `{T('risk_free_rates')}` T
            USING `{stg}` S
            ON T.inquiry_date = S.inquiry_date
            WHEN MATCHED THEN UPDATE SET rate = S.rate
            WHEN NOT MATCHED THEN INSERT (inquiry_date, rate) VALUES (S.inquiry_date, S.rate)
        """)
        bq_exec(f"DROP TABLE `{stg}`")
        logger.info("국채 금리 %d건 동기화 완료 (yfinance ^IRX)", len(records))
    except Exception as e:
        logger.error("국채 금리 다운로드 오류: %s", e)

    return "OK", 200

# ──────────────────── 5) Analysis 동기화 ────────────────────
def sync_analysis_table():
    ensure_bq_ready_once()
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    last_asset = list(bq.query(
        f"SELECT MAX(inquiry_date) FROM `{T('assets')}`", location=BQ_LOCATION
    ).result())[0][0]
    if not last_asset:
        logger.info("assets 테이블이 비어 있음 → analysis 스킵")
        return "NO_ASSETS", 200

    try:
        last_anl = list(bq.query(
            f"SELECT MAX(inquiry_date) FROM `{T('analysis')}`", location=BQ_LOCATION
        ).result())[0][0]
    except Exception:
        last_anl = None

    if last_anl:
        start = min(last_asset, last_anl + timedelta(days=1))
    else:
        start = list(bq.query(
            f"SELECT MIN(inquiry_date) FROM `{T('assets')}`", location=BQ_LOCATION
        ).result())[0][0]

    manual_df = get_manual_data_from_sheet(
        "1te6C5qjOMsE7oPxEdC5z7F9gESBLs0LFoe8QINHzkEg", "포트폴리오_수동데이터"
    )
    min_manual_date = manual_df["inquiry_date"].min() if isinstance(manual_df, pd.DataFrame) and not manual_df.empty else None
    if min_manual_date and (not last_anl or min_manual_date < start):
        start = min_manual_date
        logger.info("과거 수동데이터 감지 → start=%s", start)

    if start is None or start > last_asset:
        logger.info("동기화할 구간 없음 (start=%s > last_asset=%s)", start, last_asset)
        return "UP_TO_DATE", 200

    logger.info("USING ASSETS TABLE = %s | USING ANALYSIS TABLE = %s", T("assets"), T("analysis"))
    logger.info("analysis 윈도우: %s ~ %s", start, last_asset)

    # ── 1) assets 로딩
    assets_df = bq.query(
        f"SELECT * FROM `{T('assets')}` WHERE inquiry_date BETWEEN @s AND @e",
        bigquery.QueryJobConfig(query_parameters=[_bq_param("s", start), _bq_param("e", last_asset)]),
        location=BQ_LOCATION
    ).to_dataframe()
    if assets_df.empty:
        logger.info("윈도우 내 assets 0건 → 종료")
        return "NO_ASSET_ROWS", 200

    assets_df["inquiry_date"] = pd.to_datetime(assets_df["inquiry_date"])
    assets_df["ticker"] = assets_df["ticker"].astype(str)
    assets_df["account_nickname"] = assets_df["account_nickname"].astype(str)

    # ── 2) 수동시트 병합 (있는 경우만)
    combined = assets_df.copy()
    if isinstance(manual_df, pd.DataFrame) and not manual_df.empty:
        manual_df["inquiry_date"] = pd.to_datetime(manual_df["inquiry_date"])
        # 숫자 컬럼 정리
        for col in ["eval_amount_krw", "purchase_amount"]:
            if col in manual_df.columns:
                manual_df[col] = pd.to_numeric(
                    manual_df[col].astype(str).str.replace(",", ""), errors="coerce"
                )
        assets_df = assets_df.sort_values("inquiry_date")
        manual_df = manual_df.sort_values("inquiry_date")

        # 같은 (계정,티커)에서 가장 가까운 과거 수동 값을 붙임
        combined = pd.merge_asof(
            assets_df, manual_df,
            on="inquiry_date", by=["account_nickname","ticker"],
            direction="backward", suffixes=("", "_manual"),
        )
        for col in ["eval_amount_krw","purchase_amount","asset_class","product_name","currency"]:
            mc = f"{col}_manual"
            if mc in combined.columns:
                combined[col] = combined[mc].combine_first(combined[col])
                combined.drop(columns=[mc], inplace=True, errors="ignore")

        # 순수 수동 전용 자산 ffill (해당 계정/티커가 assets에 전혀 없을 때)
        pure_manual_keys = manual_df[["account_nickname","ticker"]].drop_duplicates().merge(
            assets_df[["account_nickname","ticker"]].drop_duplicates(),
            how="left", indicator=True
        ).query('_merge=="left_only"').drop(columns="_merge")
        if not pure_manual_keys.empty:
            date_range = pd.to_datetime(pd.date_range(start, last_asset, freq="D"))
            template = pd.MultiIndex.from_product([date_range, pure_manual_keys.index],
                                                  names=["inquiry_date","key_idx"]).to_frame(index=False)
            template = template.merge(pure_manual_keys, left_on="key_idx", right_index=True).drop("key_idx", axis=1)
            pure_manual_df = pd.merge(
                template, manual_df, on=["inquiry_date","account_nickname","ticker"], how="left"
            ).sort_values(["account_nickname","ticker","inquiry_date"])
            fill_cols = [c for c in manual_df.columns if c not in ["inquiry_date","account_nickname","ticker"]]
            pure_manual_df[fill_cols] = pure_manual_df.groupby(["account_nickname","ticker"])[fill_cols].ffill()
            pure_manual_df.dropna(subset=["eval_amount_krw"], inplace=True)
            combined = pd.concat([combined, pure_manual_df], ignore_index=True)

    # ── 3) 자산분류/통화 매핑
    try:
        map_df = bq.query(
            f"SELECT ticker, asset_class AS asset_class_mapped, "
            f"currency AS currency_mapped, exchange_code AS exchange_code_mapped "
            f"FROM `{T('asset_class')}`",
            location=BQ_LOCATION
        ).to_dataframe()
    except Exception:
        map_df = pd.DataFrame(columns=["ticker","asset_class_mapped","currency_mapped","exchange_code_mapped"])

    combined = combined.merge(map_df, on="ticker", how="left")
    for col in ("asset_class", "currency"):
        if col not in combined.columns:
            combined[col] = np.nan
    combined["asset_class"] = combined["asset_class"].fillna(combined["asset_class_mapped"])
    combined["currency"]    = combined["currency"].fillna(combined["currency_mapped"])
    # 거래소 → 통화 추정
    ex2cur = {"NASD":"USD","NYSE":"USD","AMEX":"USD","XNAS":"USD","XNYS":"USD",
              "KRX":"KRW","KOSPI":"KRW","KOSDAQ":"KRW","SEHK":"HKD","TSE":"JPY"}
    mask_no_cur = combined["currency"].isna() & combined["exchange_code"].notna()
    combined.loc[mask_no_cur, "currency"] = combined.loc[mask_no_cur, "exchange_code"].map(ex2cur)
    combined.drop(columns=["asset_class_mapped","currency_mapped","exchange_code_mapped"],
                  inplace=True, errors="ignore")

    to_upsert = (combined.sort_values("inquiry_date", ascending=False)
                 .dropna(subset=["ticker","currency"])
                 .drop_duplicates(subset=["ticker"])
                 [["ticker","currency","exchange_code"]])
    if not to_upsert.empty:
        stg_cls = bq_temp_table("asset_class")
        bq_load_df(stg_cls, to_upsert, "WRITE_TRUNCATE")
        bq_exec(f"""
            MERGE `{T('asset_class')}` T
            USING `{stg_cls}` S
            ON T.ticker=S.ticker
            WHEN MATCHED THEN UPDATE SET currency=S.currency, exchange_code=S.exchange_code
            WHEN NOT MATCHED THEN INSERT(ticker,currency,exchange_code)
            VALUES(S.ticker,S.currency,S.exchange_code)
        """)
        bq_exec(f"DROP TABLE `{stg_cls}`")

    # ── 4) 환율/배당
    fx_df = bq.query(f"SELECT inquiry_date, rate AS usd_krw_rate FROM `{T('daily_exchange_rates')}`",
                     location=BQ_LOCATION).to_dataframe()
    fx_df["inquiry_date"] = pd.to_datetime(fx_df["inquiry_date"])
    combined = combined.merge(fx_df, on="inquiry_date", how="left")
    combined["usd_krw_rate"] = combined["usd_krw_rate"].ffill().bfill().fillna(1.0)

    # 배당
    div_df = bq.query(f"""
        SELECT ticker, ex_dividend_date, dividend_per_share
        FROM `{T('dividend_history')}`
        WHERE ex_dividend_date BETWEEN @s AND @e
    """, bigquery.QueryJobConfig(
        query_parameters=[_bq_param("s", start), _bq_param("e", last_asset)]
    ), location=BQ_LOCATION).to_dataframe()
    if not div_df.empty:
        div_df["ex_dividend_date"] = pd.to_datetime(div_df["ex_dividend_date"])
        combined = combined.merge(
            div_df, left_on=["ticker","inquiry_date"],
            right_on=["ticker","ex_dividend_date"], how="left"
        )
    if "daily_dividend_income_krw" not in combined.columns:
        combined["daily_dividend_income_krw"] = 0

    for c in ["quantity","avg_purchase_price","purchase_amount","current_price","eval_amount","usd_krw_rate","dividend_per_share"]:
        if c in combined.columns:
            combined[c] = pd.to_numeric(combined[c], errors="coerce")

    mask_div = combined.get("dividend_per_share").notna() & combined.get("quantity").notna() if "dividend_per_share" in combined.columns else pd.Series(False, index=combined.index)
    if mask_div.any():
        is_usd = combined["currency"].fillna("KRW").eq("USD")
        combined.loc[mask_div,"daily_dividend_income_krw"] = np.where(
            is_usd[mask_div],
            combined.loc[mask_div,"dividend_per_share"]*combined.loc[mask_div,"quantity"]*0.85*combined.loc[mask_div,"usd_krw_rate"],
            combined.loc[mask_div,"dividend_per_share"]*combined.loc[mask_div,"quantity"]
        )
    combined.drop(columns=["ex_dividend_date"], inplace=True, errors="ignore")

    # ── 5) 주문 집계 → LEFT MERGE (assets 기준)
    orders = bq.query(f"""
        SELECT * FROM `{T('orders')}`
        WHERE order_date BETWEEN @s AND @e
    """, bigquery.QueryJobConfig(
        query_parameters=[_bq_param("s", start), _bq_param("e", last_asset)]
    ), location=BQ_LOCATION).to_dataframe()

    if not orders.empty:
        orders["order_date"] = pd.to_datetime(orders["order_date"])
        fx_map = fx_df.set_index(fx_df["inquiry_date"].dt.date)["usd_krw_rate"].to_dict()
        orders["rate"] = np.where(
            orders["currency"].fillna("KRW")=="KRW",
            1.0,
            orders["order_date"].dt.date.map(lambda d: fx_map.get(d, 1.0))
        )
        for c in ["total_amount","avg_price"]:
            orders[c] = pd.to_numeric(orders[c], errors="coerce")
        orders["amount_krw"] = orders["total_amount"].astype(float) * orders["rate"]
        orders["order_type"] = orders["order_type"].fillna("").str.strip()
        orders = orders[~orders["order_type"].str.contains("정정|취소", regex=True)]
        buy_mask  = orders["order_type"].str.contains("매수", regex=False)
        sell_mask = orders["order_type"].str.contains("매도", regex=False)
        orders = orders.assign(
            buy_amount_krw  = np.where(buy_mask,  orders["amount_krw"], 0.0),
            sell_amount_krw = np.where(sell_mask, orders["amount_krw"], 0.0),
            buy_avg_price   = np.where(buy_mask,  orders["avg_price"], np.nan),
            sell_avg_price  = np.where(sell_mask, orders["avg_price"], np.nan),
            inquiry_date    = orders["order_date"],
        )
        agg = (orders.groupby(["inquiry_date","account_nickname","ticker"], as_index=False)
               .agg(buy_amount_krw=("buy_amount_krw","sum"),
                    sell_amount_krw=("sell_amount_krw","sum"),
                    buy_avg_price=("buy_avg_price","mean"),
                    sell_avg_price=("sell_avg_price","mean")))

        keys = ["inquiry_date","account_nickname","ticker"]

        combined["inquiry_date"] = pd.to_datetime(combined["inquiry_date"])
        combined["ticker"] = combined["ticker"].astype(str)
        combined["account_nickname"] = combined["account_nickname"].astype(str)
        agg["inquiry_date"] = pd.to_datetime(agg["inquiry_date"])
        agg["ticker"] = agg["ticker"].astype(str)
        agg["account_nickname"] = agg["account_nickname"].astype(str)

        combined = (combined.sort_values(keys)
                    .drop_duplicates(subset=keys, keep="last"))
        combined = pd.merge(combined, agg, on=keys, how="left")

    for c in ["buy_amount_krw","sell_amount_krw"]:
        if c not in combined.columns: combined[c] = 0.0
        combined[c] = combined[c].fillna(0.0)
    for c in ["buy_avg_price","sell_avg_price"]:
        if c not in combined.columns: combined[c] = np.nan

    # ── 6) 파생 계산 (NaN도 채우는 방식으로 보정)
    for c in ["quantity","avg_purchase_price","purchase_amount","current_price","eval_amount","usd_krw_rate"]:
        if c in combined.columns:
            combined[c] = pd.to_numeric(combined[c], errors="coerce")

    has_eval = "eval_amount" in combined.columns
    has_fx   = "usd_krw_rate" in combined.columns
    if has_eval and has_fx:
        is_krw = combined["currency"].fillna("KRW").eq("KRW")
        computed_eval_krw = np.where(
            is_krw, combined["eval_amount"],
            combined["eval_amount"] * combined["usd_krw_rate"]
        )
        if "eval_amount_krw" not in combined.columns:
            combined["eval_amount_krw"] = computed_eval_krw
        else:
            fill_mask = combined["eval_amount_krw"].isna()
            combined.loc[fill_mask, "eval_amount_krw"] = computed_eval_krw[fill_mask]

    if "book_cost_krw" not in combined.columns:
        combined["book_cost_krw"] = np.where(
            combined["currency"].fillna("KRW").eq("KRW"),
            combined["purchase_amount"],
            combined["purchase_amount"] * combined["usd_krw_rate"]
        )
    else:
        fill_mask = combined["book_cost_krw"].isna()
        combined.loc[fill_mask, "book_cost_krw"] = np.where(
            combined.loc[fill_mask, "currency"].fillna("KRW").eq("KRW"),
            combined.loc[fill_mask, "purchase_amount"],
            combined.loc[fill_mask, "purchase_amount"] * combined.loc[fill_mask, "usd_krw_rate"]
        )

    if "eval_profit_loss_amount_krw" not in combined.columns:
        combined["eval_profit_loss_amount_krw"] = combined["eval_amount_krw"] - combined["book_cost_krw"]
    else:
        fill_mask = combined["eval_profit_loss_amount_krw"].isna()
        combined.loc[fill_mask, "eval_profit_loss_amount_krw"] = (
            combined.loc[fill_mask, "eval_amount_krw"] - combined.loc[fill_mask, "book_cost_krw"]     )

    if "daily_dividend_income_krw" not in combined.columns:
        combined["daily_dividend_income_krw"] = 0
    combined["daily_dividend_income_krw"] = combined["daily_dividend_income_krw"].fillna(0)

    # ── 7) 전략 가중치(weights)
    try:
        w_df = bq.query(f"""
            WITH latest_alloc AS (
              SELECT ticker, strategy, weight,
                     ROW_NUMBER() OVER(PARTITION BY ticker, strategy ORDER BY run_date DESC) rn
              FROM `{T('target_allocations')}` WHERE run_date <= @today
            )
            SELECT ticker, strategy, weight FROM latest_alloc WHERE rn=1
        """, bigquery.QueryJobConfig(query_parameters=[_bq_param("today", today)]),
           location=BQ_LOCATION).to_dataframe()
    except Exception:
        w_df = pd.DataFrame()

    if w_df.empty:
        strategies: list[str] = []
        wm: dict[str, dict[str, float]] = {}
    else:
        w_df["weight"] = pd.to_numeric(w_df["weight"], errors="coerce").fillna(0.0).astype(float)
        strategies = sorted(w_df["strategy"].dropna().unique().tolist())
        wm = {t: {s: w for s, w in zip(g["strategy"], g["weight"])}
              for t, g in w_df.groupby("ticker")}

    def build_weights(ticker: str):
        if not strategies:
            return []
        m = wm.get(str(ticker), {})
        return [{"strategy": s, "weight": float(m.get(s, 0.0))} for s in strategies]

    combined["ticker"] = combined["ticker"].astype(str)
    combined["weights"] = combined["ticker"].apply(build_weights)

    # ── 8) 최종 업서트
    final_cols = [
        "inquiry_date","account_nickname","ticker","product_name","asset_class","currency",
        "quantity","avg_purchase_price","purchase_amount","current_price","eval_amount",
        "usd_krw_rate","eval_amount_krw","eval_profit_loss_amount_krw",
        "daily_dividend_income_krw","buy_amount_krw","sell_amount_krw",
        "buy_avg_price","sell_avg_price","weights"
    ]
    for c in final_cols:
        if c not in combined.columns: combined[c] = np.nan

    combined = combined[final_cols].copy()
    combined["inquiry_date"] = pd.to_datetime(combined["inquiry_date"]).dt.date
    combined["ticker"] = combined["ticker"].astype(str)
    combined["account_nickname"] = combined["account_nickname"].astype(str)

    if combined.empty:
        logger.info("업서트할 행이 없음 → MERGE 스킵")
        return "NO_COMBINED_ROWS", 200

    stg = bq_temp_table("analysis")
    bq_load_df(stg, combined, "WRITE_TRUNCATE")
    bq_exec(f"""
        MERGE `{T('analysis')}` T
        USING `{stg}` S
        ON  T.inquiry_date = S.inquiry_date
        AND T.account_nickname = S.account_nickname
        AND T.ticker = S.ticker
        WHEN MATCHED THEN UPDATE SET
          product_name=S.product_name, asset_class=S.asset_class, currency=S.currency,
          quantity=S.quantity, avg_purchase_price=S.avg_purchase_price, purchase_amount=S.purchase_amount,
          current_price=S.current_price, eval_amount=S.eval_amount, usd_krw_rate=S.usd_krw_rate,
          eval_amount_krw=S.eval_amount_krw, eval_profit_loss_amount_krw=S.eval_profit_loss_amount_krw,
          daily_dividend_income_krw=S.daily_dividend_income_krw, buy_amount_krw=S.buy_amount_krw,
          sell_amount_krw=S.sell_amount_krw, buy_avg_price=S.buy_avg_price, sell_avg_price=S.sell_avg_price,
          weights=S.weights
        WHEN NOT MATCHED THEN INSERT (
          inquiry_date, account_nickname, ticker, product_name, asset_class, currency,
          quantity, avg_purchase_price, purchase_amount, current_price, eval_amount,
          usd_krw_rate, eval_amount_krw, eval_profit_loss_amount_krw,
          daily_dividend_income_krw, buy_amount_krw, sell_amount_krw, buy_avg_price, sell_avg_price, weights
        ) VALUES (
          S.inquiry_date, S.account_nickname, S.ticker, S.product_name, S.asset_class, S.currency,
          S.quantity, S.avg_purchase_price, S.purchase_amount, S.current_price, S.eval_amount,
          S.usd_krw_rate, S.eval_amount_krw, S.eval_profit_loss_amount_krw,
          S.daily_dividend_income_krw, S.buy_amount_krw, S.sell_amount_krw, S.buy_avg_price, S.sell_avg_price, S.weights
        )
    """)
    bq_exec(f"DROP TABLE `{stg}`")

    logger.info("Analysis 동기화 완료(BigQuery) | upsert_rows=%d", len(combined))
    return "OK", 200

# ────────────────────  전체 파이프라인 ────────────────────
def sync_all_data():
    ensure_bq_ready_once()
    sync_kis_data()
    sync_market_data()
    sync_risk_free_rate()
    sync_analysis_table()
    return "OK",200

# ──────────────────── Cloud Run 엔드포인트 ────────────────────
@app.route("/manual", methods=["POST"])
def ep_kis():      return sync_kis_data()

@app.route("/sync-market-data", methods=["POST"])
def ep_mkt():      return sync_market_data()

@app.route("/sync-risk-free-rate", methods=["POST"])
def ep_rfr():      return sync_risk_free_rate()

@app.route("/sync-analysis", methods=["POST"])
def ep_anl():      return sync_analysis_table()

@app.route("/sync-all", methods=["POST"])
def ep_all():      return sync_all_data()

# ──────────────────── 로컬 테스트 ────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)