"""NBEATS.py
N-BEATS 기반 예상 연간수익률 산출
"""

import os, logging, warnings, math, uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, List

import numpy as np
import pandas as pd
import torch

from neuralforecast.losses.pytorch import MAE
from neuralforecast import NeuralForecast
from neuralforecast.models import NBEATS

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ─────────────────── 로그/기본 설정 ────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("nbeats-bq")
warnings.filterwarnings("ignore", category=UserWarning)

# ─────────────────── 환경변수 ────────────────────
BQ_PROJECT  = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET  = os.getenv("BQ_DATASET", "portfolio")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")

# 입력 테이블
TBL_PRICES  = os.getenv("TBL_PRICES",  "daily_stock_prices")
TBL_DIVS    = os.getenv("TBL_DIVS",    "dividend_history")
TBL_FX      = os.getenv("TBL_FX",      "daily_exchange_rates")
TBL_ASSET   = os.getenv("TBL_ASSET",   "asset_class")

# 출력 테이블
TBL_PRED    = os.getenv("TBL_PRED",    "predicted_expected_returns")

# 모델/데이터 윈도우
HORIZON            = int(os.getenv("HORIZON", 20))          
WINDOW_DAYS        = int(os.getenv("WINDOW_DAYS", 252*3))   
MIN_SERIES_LENGTH  = int(os.getenv("MIN_SERIES_LENGTH", 252*2))

# ─────────────────── BigQuery 클라이언트 ────────────────────
bq = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)

# ─────────────────── BigQuery 유틸 ────────────────────
def _ensure_bq_table():
    """데이터셋/출력테이블 없으면 생성 (run_date 파티션, ticker 클러스터)."""
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    try:
        bq.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        logger.info("Created dataset %s.%s (%s)", BQ_PROJECT, BQ_DATASET, BQ_LOCATION)

    table_ref = dataset_ref.table(TBL_PRED)
    try:
        bq.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("run_date", "DATE",   mode="REQUIRED"),
            bigquery.SchemaField("ticker",   "STRING", mode="REQUIRED"),
            bigquery.SchemaField("exp_return_annual", "NUMERIC", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        )
        table.clustering_fields = ["ticker"]
        bq.create_table(table)
        logger.info("Created table %s.%s.%s", BQ_PROJECT, BQ_DATASET, TBL_PRED)

def _q(sql: str, params: List[bigquery.ScalarQueryParameter] = None) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=params or [],
        use_legacy_sql=False
    )
    return bq.query(sql, job_config=job_cfg).result().to_dataframe(create_bqstorage_client=False)

def _load_inputs_bq(start_date: date) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.DataFrame]:
    """prices, divs, fx(series), asset_class(currency mapping) 로드"""
    sd = bigquery.ScalarQueryParameter("sd", "DATE", start_date)

    prices = _q(f"""
      SELECT inquiry_date, ticker, close_price
      FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_PRICES}`
      WHERE inquiry_date >= @sd
    """, [sd]).pivot(index="inquiry_date", columns="ticker", values="close_price").ffill()

    if prices.empty:
        return prices, pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    divs = _q(f"""
      SELECT ex_dividend_date AS inquiry_date, ticker, dividend_per_share
      FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_DIVS}`
      WHERE ex_dividend_date >= @sd
    """, [sd]).pivot(index="inquiry_date", columns="ticker", values="dividend_per_share") \
      .reindex(prices.index).fillna(0.0)
    divs = divs.reindex(columns=prices.columns, fill_value=0.0)

    fx = _q(f"""
      SELECT inquiry_date, rate
      FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_FX}`
      WHERE inquiry_date >= @sd
    """, [sd]).set_index("inquiry_date")["rate"].reindex(prices.index).ffill().bfill()

    cur_df = _q(f"""
      SELECT ticker, currency, exchange_code
      FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_ASSET}`
    """)

    return prices, divs, fx, cur_df

def _merge_upsert_predictions(df: pd.DataFrame):
    """DataFrame(run_date, ticker, exp_return_annual) → BigQuery MERGE UPSERT"""
    _ensure_bq_table()

    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    staging_name = f"_stg_pred_{uuid.uuid4().hex[:8]}"
    staging_ref = dataset_ref.table(staging_name)
    target_ref  = dataset_ref.table(TBL_PRED)

    # 타입 정리
    out = df.copy()
    out["run_date"] = pd.to_datetime(out["run_date"]).dt.date
    out["exp_return_annual"] = [
        Decimal(str(x)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        for x in out["exp_return_annual"].astype(float)
    ]

    # 스테이징 로드
    load_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("run_date", "DATE"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("exp_return_annual", "NUMERIC"),
        ],
    )
    bq.create_table(bigquery.Table(staging_ref, schema=load_cfg.schema))
    bq.load_table_from_dataframe(out, staging_ref, job_config=load_cfg).result()

    # MERGE
    merge_sql = f"""
    MERGE `{BQ_PROJECT}.{BQ_DATASET}.{TBL_PRED}` T
    USING `{BQ_PROJECT}.{BQ_DATASET}.{staging_name}` S
    ON T.run_date = S.run_date AND T.ticker = S.ticker
    WHEN MATCHED THEN
      UPDATE SET exp_return_annual = S.exp_return_annual
    WHEN NOT MATCHED THEN
      INSERT (run_date, ticker, exp_return_annual)
      VALUES (S.run_date, S.ticker, S.exp_return_annual)
    """
    _q(merge_sql)

    # 스테이징 정리
    bq.delete_table(staging_ref, not_found_ok=True)
    logger.info("%d predictions saved (MERGE) to %s.%s.%s",
                len(out), BQ_PROJECT, BQ_DATASET, TBL_PRED)

# ─────────────────── 모델 파이프라인 ────────────────────
def _prepare_log_returns(prices: pd.DataFrame, divs: pd.DataFrame,
                         fx: pd.Series, cur_df: pd.DataFrame) -> pd.DataFrame:
    """KRW 기준 총수익률(배당 포함) → 로그수익률 DataFrame"""
    if prices.empty:
        return pd.DataFrame()

    ex2cur = {"NASD":"USD","NYSE":"USD","AMEX":"USD","XNAS":"USD","XNYS":"USD","ARCX":"USD"}
    cur_df = cur_df.copy()
    m = cur_df["currency"].isna() & cur_df["exchange_code"].notna()
    cur_df.loc[m, "currency"] = cur_df.loc[m, "exchange_code"].map(ex2cur)
    cur_map = cur_df.set_index("ticker")["currency"].to_dict()

    usd_cols = [c for c in prices.columns if cur_map.get(c) == "USD"]
    if usd_cols:
        fx_arr = fx.to_numpy()[:, None]
        prices.loc[:, usd_cols] = prices[usd_cols].to_numpy() * fx_arr
        divs.loc[:, usd_cols]   = divs[usd_cols].to_numpy() * fx_arr

    shifted = prices.shift(1)
    log_r = np.log((prices + divs) / shifted)
    log_r.replace([np.inf, -np.inf], np.nan, inplace=True)
    return log_r

def _build_training_df(log_returns: pd.DataFrame) -> pd.DataFrame:
    """NBEATS 학습용 long-form(df: unique_id, ds, y) 생성"""
    series_list = []
    for tk in log_returns.columns:
        s = log_returns[tk].dropna()
        if len(s) >= MIN_SERIES_LENGTH:
            df = s.iloc[-WINDOW_DAYS:].to_frame("y").reset_index()
            df.rename(columns={"inquiry_date": "ds"}, inplace=True)
            df["unique_id"] = tk
            series_list.append(df)
    if not series_list:
        return pd.DataFrame(columns=["unique_id","ds","y"])
    out = pd.concat(series_list, ignore_index=True)
    out["ds"] = pd.to_datetime(out["ds"])
    return out

def _train_predict_nbeats(train_df: pd.DataFrame) -> pd.DataFrame:
    """NBEATS 학습/예측 → predictions long-form(df: unique_id, ds, NBEATS)"""
    if train_df.empty:
        return pd.DataFrame(columns=["unique_id","ds","NBEATS"])

    torch.manual_seed(42)
    np.random.seed(42)

    model = NBEATS(
        h=HORIZON,
        input_size=2*HORIZON,
        stack_types=("trend","seasonality"),
        max_steps=500,
        loss=MAE(),
    )
    nf = NeuralForecast(models=[model], freq="B")
    nf.fit(df=train_df)
    preds = nf.predict()  
    return preds

# ─────────────────── 메인 ────────────────────
def main():
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    logger.info("run_date=%s | horizon=%d | window=%d | minlen=%d",
                today, HORIZON, WINDOW_DAYS, MIN_SERIES_LENGTH)

    start_date = today - timedelta(days=WINDOW_DAYS + 90)  # 로드 여유
    logger.info("Loading from BigQuery since %s ...", start_date)

    prices, divs, fx, cur_df = _load_inputs_bq(start_date)
    if prices.empty or fx.empty or cur_df.empty:
        logger.error("Input data insufficient: prices/fx/asset_class check")
        return

    log_returns = _prepare_log_returns(prices, divs, fx, cur_df)
    train_df = _build_training_df(log_returns)
    if train_df.empty:
        logger.error("No series meet MIN_SERIES_LENGTH=%d", MIN_SERIES_LENGTH)
        return

    logger.info("Training NBEATS for %d tickers ...",
                train_df["unique_id"].nunique())
    preds = _train_predict_nbeats(train_df)
    if preds.empty:
        logger.error("No predictions produced.")
        return

    agg = preds.groupby("unique_id")["NBEATS"].mean().reset_index()
    agg["exp_return_annual"] = agg["NBEATS"].apply(lambda mu: math.exp(mu * 252) - 1)
    agg.rename(columns={"unique_id": "ticker"}, inplace=True)
    out_df = agg[["ticker","exp_return_annual"]].copy()
    out_df["run_date"] = today

    _merge_upsert_predictions(out_df)

if __name__ == "__main__":
    main()
