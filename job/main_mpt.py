"""main_mpt.py — BigQuery only
워크포워드 백테스트 + 8가지 자산배분 가중치 계산 (PG 제거, BQ 단독)
"""

import logging, os, sys, argparse
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay
import cvxpy as cp
from pypfopt import EfficientFrontier, HRPOpt, objective_functions
from zoneinfo import ZoneInfo
from google.cloud import bigquery

# ────────────────────── 로깅 ──────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("mpt-bq")

# ────────────────────── BigQuery 설정 ──────────────────────
BQ_PROJECT  = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET  = os.getenv("BQ_DATASET", "portfolio")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")
BQ_MIRROR   = (os.getenv("BQ_MIRROR", "1").lower() in ("1", "true", "yes"))
bq = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else None

def T(name: str) -> str:
    """project.dataset.table fully-qualified"""
    return f"{BQ_PROJECT}.{BQ_DATASET}.{name}"

def _infer_bq_type(py_val):
    import decimal
    from datetime import date as _date, datetime as _dt
    if isinstance(py_val, bool): return "BOOL"
    if isinstance(py_val, int): return "INT64"
    if isinstance(py_val, float) or isinstance(py_val, decimal.Decimal): return "FLOAT64"
    if isinstance(py_val, _dt): return "TIMESTAMP"
    if isinstance(py_val, _date): return "DATE"
    return "STRING"

def _bq_param(k, v):
    """Scalar/Array 자동 판별 파라미터 생성"""
    if isinstance(v, (list, tuple, set)):
        arr = list(v)
        elem_type = _infer_bq_type(arr[0]) if arr else "STRING"
        return bigquery.ArrayQueryParameter(k, elem_type, arr)
    else:
        return bigquery.ScalarQueryParameter(k, _infer_bq_type(v), v)

def bq_read(sql: str, params: dict | None = None) -> pd.DataFrame:
    """표준 SQL 실행 → DataFrame 반환"""
    if not bq:
        logger.warning("BigQuery 클라이언트가 초기화되지 않았습니다.")
        return pd.DataFrame()
    cfg = bigquery.QueryJobConfig()
    cfg.use_legacy_sql = False
    if params:
        cfg.query_parameters = [_bq_param(k, v) for k, v in params.items()]
    job = bq.query(sql, job_config=cfg, location=BQ_LOCATION)
    return job.result().to_dataframe()

def bq_exec(sql: str, params: dict | None = None):
    """DML/DDL 실행 (쓰기 동작). BQ_MIRROR==1 일 때만 수행"""
    if not (BQ_MIRROR and bq):
        return
    cfg = bigquery.QueryJobConfig()
    cfg.use_legacy_sql = False
    if params:
        cfg.query_parameters = [_bq_param(k, v) for k, v in params.items()]
    bq.query(sql, job_config=cfg, location=BQ_LOCATION).result()

def bq_temp_table(suffix: str) -> str:
    import uuid
    return f"{BQ_PROJECT}.{BQ_DATASET}._stg_{suffix}_{uuid.uuid4().hex[:8]}"

def bq_load_df(table: str, df: pd.DataFrame, write_disposition="WRITE_APPEND"):
    """DataFrame을 BQ 테이블로 적재 (autodetect)"""
    if not (BQ_MIRROR and bq) or df is None or df.empty:
        return
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(lambda x: x.decode() if isinstance(x, (bytes, bytearray)) else x)
    job = bq.load_table_from_dataframe(
        df, table,
        job_config=bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True),
        location=BQ_LOCATION
    )
    job.result()

def _ensure_bq_result_tables():
    """필요 결과 테이블 생성 (있으면 통과)"""
    if not BQ_MIRROR: return
    bq_exec(f"""
      CREATE TABLE IF NOT EXISTS `{T('target_allocations')}` (
        run_date DATE, strategy STRING, ticker STRING, weight FLOAT64
      );
    """)
    bq_exec(f"""
      CREATE TABLE IF NOT EXISTS `{T('target_portfolios')}` (
        run_date DATE, strategy STRING,
        expected_return FLOAT64, annual_volatility FLOAT64,
        sharpe_ratio FLOAT64, mdd FLOAT64
      );
    """)
    bq_exec(f"""
      CREATE TABLE IF NOT EXISTS `{T('backtest_nav')}` (
        run_date DATE, strategy STRING, nav_date DATE,
        nav FLOAT64, daily_return FLOAT64, cum_return FLOAT64
      );
    """)
    bq_exec(f"""
      CREATE TABLE IF NOT EXISTS `{T('backtest_allocations')}` (
        run_date DATE, strategy STRING, rebalance_date DATE,
        ticker STRING, weight FLOAT64
      );
    """)

# ────────────────────── 전략/안정화 파라미터 ──────────────────────
REPLACEMENTS = {
    # 필요 시 티커 프록시: 보유 데이터 가용성 문제 방지용 (직접 최적화엔 영향 없음)
    "SCHD": "SPY",
    "QQQM": "QQQ",
    "VGLT": "TLT",
    "VGIT": "TLT",
}

REBALANCE_FREQ       = os.getenv("REBALANCE_FREQ", "W-FRI")   # 'W-FRI' | 'M' | 'Q' | 'D'
SMOOTH_ALPHA         = float(os.getenv("SMOOTH_ALPHA", 0.30)) # EMA 블렌드 가중(신규 비중)
MAX_WEIGHT_DELTA     = float(os.getenv("MAX_WEIGHT_DELTA", 0.05))  # 1회 변경 상한(±5%p)
HYSTERESIS_ABS       = float(os.getenv("HYSTERESIS_ABS", 0.01))    # ±1%p 이하면 변경 없음
TURNOVER_LAMBDA      = float(os.getenv("TURNOVER_LAMBDA", 10.0))   # (w-w_prev)^2 패널티 강도
MU_SMOOTH_DAYS       = int(os.getenv("MU_SMOOTH_DAYS", 60))        # μ EWMA 일수
MU_CONFIDENCE        = float(os.getenv("MU_CONFIDENCE", 0.30))     # μ Shrink(0~1)
FORCE_DAILY_WEIGHTS  = os.getenv("FORCE_DAILY_WEIGHTS", "0").lower() in ("1","true","yes")

# ────────────────────── BigQuery 저장 유틸 ──────────────────────
def _bq_mirror_alloc_and_port(today: date, alloc_rows: list[dict], port_rows: list[dict]):
    if not (BQ_MIRROR and (alloc_rows or port_rows)): return
    _ensure_bq_result_tables()
    bq_exec(f"DELETE FROM `{T('target_allocations')}` WHERE run_date=@d", {"d": today})
    bq_exec(f"DELETE FROM `{T('target_portfolios')}` WHERE run_date=@d", {"d": today})

    if alloc_rows:
        df = pd.DataFrame(alloc_rows)
        df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
        stg = bq_temp_table("alloc")
        bq_load_df(stg, df, "WRITE_TRUNCATE")
        bq_exec(f"""
            INSERT INTO `{T('target_allocations')}` (run_date, strategy, ticker, weight)
            SELECT run_date, strategy, ticker, weight FROM `{stg}`;
        """)
        bq_exec(f"DROP TABLE `{stg}`")

    if port_rows:
        dfp = pd.DataFrame(port_rows)
        dfp["run_date"] = pd.to_datetime(dfp["run_date"]).dt.date
        stg = bq_temp_table("ports")
        bq_load_df(stg, dfp, "WRITE_TRUNCATE")
        bq_exec(f"""
          MERGE `{T('target_portfolios')}` T
          USING `{stg}` S
          ON T.run_date=S.run_date AND T.strategy=S.strategy
          WHEN MATCHED THEN UPDATE SET
            expected_return   = S.expected_return,
            annual_volatility = S.annual_volatility,
            sharpe_ratio      = S.sharpe_ratio
          WHEN NOT MATCHED THEN INSERT
            (run_date, strategy, expected_return, annual_volatility, sharpe_ratio, mdd)
          VALUES
            (S.run_date, S.strategy, S.expected_return, S.annual_volatility, S.sharpe_ratio, NULL);
        """)
        bq_exec(f"DROP TABLE `{stg}`")

def _bq_mirror_backtest_nav(today: date, nav_rows: list[dict]):
    if not (BQ_MIRROR and nav_rows): return
    _ensure_bq_result_tables()
    df = pd.DataFrame(nav_rows)
    if df.empty: return
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["nav_date"] = pd.to_datetime(df["nav_date"]).dt.date

    bq_exec(f"DELETE FROM `{T('backtest_nav')}` WHERE run_date=@d", {"d": today})
    stg = bq_temp_table("nav")
    bq_load_df(stg, df, "WRITE_TRUNCATE")
    bq_exec(f"""
      MERGE `{T('backtest_nav')}` T
      USING `{stg}` S
      ON T.run_date=S.run_date AND T.strategy=S.strategy AND T.nav_date=S.nav_date
      WHEN MATCHED THEN UPDATE SET
        nav=S.nav, daily_return=S.daily_return, cum_return=S.cum_return
      WHEN NOT MATCHED THEN INSERT
        (run_date, strategy, nav_date, nav, daily_return, cum_return)
      VALUES
        (S.run_date, S.strategy, S.nav_date, S.nav, S.daily_return, S.cum_return);
    """)
    bq_exec(f"DROP TABLE `{stg}`")

def _bq_mirror_backtest_allocs(today: date, rows: list[dict]):
    if not (BQ_MIRROR and rows): return
    _ensure_bq_result_tables()
    df = pd.DataFrame(rows)
    if df.empty: return
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    df["rebalance_date"] = pd.to_datetime(df["rebalance_date"]).dt.date

    bq_exec(f"DELETE FROM `{T('backtest_allocations')}` WHERE run_date=@d", {"d": today})
    stg = bq_temp_table("bt_allocs")
    bq_load_df(stg, df, "WRITE_TRUNCATE")
    bq_exec(f"""
      MERGE `{T('backtest_allocations')}` T
      USING `{stg}` S
      ON T.run_date=S.run_date AND T.strategy=S.strategy
         AND T.rebalance_date=S.rebalance_date AND T.ticker=S.ticker
      WHEN MATCHED THEN UPDATE SET T.weight=S.weight
      WHEN NOT MATCHED THEN INSERT (run_date, strategy, rebalance_date, ticker, weight)
      VALUES (S.run_date, S.strategy, S.rebalance_date, S.ticker, S.weight);
    """)
    bq_exec(f"DROP TABLE `{stg}`")

def _bq_update_mdd_today(today: date):
    if not BQ_MIRROR: return
    _ensure_bq_result_tables()
    bq_exec(f"""
      MERGE INTO `{T('target_portfolios')}` T
      USING (
        WITH series AS (
          SELECT
            strategy,
            nav_date,
            nav,
            nav / MAX(nav) OVER (
              PARTITION BY strategy
              ORDER BY nav_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - 1 AS dd
          FROM `{T('backtest_nav')}`
          WHERE run_date = @d
        ),
        mdd AS (
          SELECT strategy, MIN(dd) AS mdd
          FROM series
          GROUP BY strategy
        )
        SELECT * FROM mdd
      ) S
      ON T.run_date = @d AND T.strategy = S.strategy
      WHEN MATCHED THEN UPDATE SET T.mdd = S.mdd
    """, {"d": today})

def _bq_copy_last_alloc_and_port_as_today(today: date):
    if not BQ_MIRROR: return
    _ensure_bq_result_tables()
    bq_exec(f"DELETE FROM `{T('target_allocations')}` WHERE run_date=@d", {"d": today})
    bq_exec(f"""
      INSERT INTO `{T('target_allocations')}` (run_date, strategy, ticker, weight)
      SELECT @d, strategy, ticker, weight
      FROM `{T('target_allocations')}`
      WHERE run_date = (SELECT MAX(run_date) FROM `{T('target_allocations')}` WHERE run_date < @d)
    """, {"d": today})

    bq_exec(f"DELETE FROM `{T('target_portfolios')}` WHERE run_date=@d", {"d": today})
    bq_exec(f"""
      INSERT INTO `{T('target_portfolios')}` (run_date, strategy, expected_return, annual_volatility, sharpe_ratio, mdd)
      SELECT @d, strategy, expected_return, annual_volatility, sharpe_ratio, mdd
      FROM `{T('target_portfolios')}`
      WHERE run_date = (SELECT MAX(run_date) FROM `{T('target_portfolios')}` WHERE run_date < @d)
    """, {"d": today})

def _bq_copy_last_nav_as_today(today: date):
    if not BQ_MIRROR: return
    _ensure_bq_result_tables()
    bq_exec(f"DELETE FROM `{T('backtest_nav')}` WHERE run_date=@d", {"d": today})
    bq_exec(f"""
      INSERT INTO `{T('backtest_nav')}` (run_date, strategy, nav_date, nav, daily_return, cum_return)
      SELECT @d, strategy, nav_date, nav, daily_return, cum_return
      FROM `{T('backtest_nav')}`
      WHERE run_date = (SELECT MAX(run_date) FROM `{T('backtest_nav')}` WHERE run_date < @d)
    """, {"d": today})

def _bq_truncate_all():
    if not BQ_MIRROR: return
    _ensure_bq_result_tables()
    for tbl in ("target_allocations", "target_portfolios", "backtest_nav", "backtest_allocations"):
        bq_exec(f"TRUNCATE TABLE `{T(tbl)}`")

# ────────────────────── 도우미 계산기 ──────────────────────
def _sanitize_cov(Sigma: pd.DataFrame, eps: float = 1e-10) -> pd.DataFrame:
    """대칭화 + NaN/Inf 정리 + PSD(최소 고윳값) 보정."""
    if Sigma.empty:
        return Sigma
    S = Sigma.copy()
    S = S.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
    S = (S + S.T) / 2.0  
    vals, vecs = np.linalg.eigh(S.to_numpy())
    vals = np.clip(vals, eps, None)
    S_psd = (vecs @ np.diag(vals) @ vecs.T)
    return pd.DataFrame(S_psd, index=Sigma.index, columns=Sigma.columns)

def _get_risk_free_rate(default_rate: float = 0.035) -> float:
    try:
        df = bq_read(f"SELECT rate FROM `{T('risk_free_rates')}` ORDER BY inquiry_date DESC LIMIT 1")
        if df.empty:
            logger.warning("risk_free_rates 테이블 비어 있음 → 기본값 %.4f 사용", default_rate)
            return default_rate
        rate = float(df.iat[0, 0])
        logger.info("DB 무위험수익률 = %.4f", rate)
        return rate
    except Exception as e:
        logger.error("무위험수익률 조회 오류(%s) → 기본값 %.4f 사용", e, default_rate)
        return default_rate

def _get_cash_tickers(include_short_term_bond: bool) -> Tuple[str, ...]:
    excludes = ["현금", "고정"] + ([] if include_short_term_bond else ["단기채"])
    q = f"""
      SELECT ticker
      FROM `{T('asset_class')}`
      WHERE asset_class IN UNNEST(@classes)
    """
    df = bq_read(q, {"classes": excludes})
    return tuple(df["ticker"]) if not df.empty else ("",)

def _calc_returns(start: datetime, today: datetime, exclude_tickers: Tuple[str, ...]) -> pd.DataFrame:
    """배당/환율 반영 총수익률 (KRW 기준)"""
    # 가격
    prices = bq_read(f"""
        SELECT inquiry_date, ticker, close_price
        FROM `{T('daily_stock_prices')}`
        WHERE inquiry_date >= @start
    """, {"start": start.date()})
    if prices.empty:
        return pd.DataFrame()
    prices["inquiry_date"] = pd.to_datetime(prices["inquiry_date"])
    prices = prices.pivot(index="inquiry_date", columns="ticker", values="close_price").ffill()

    # 배당
    divs = bq_read(f"""
        SELECT ex_dividend_date, ticker, dividend_per_share
        FROM `{T('dividend_history')}`
        WHERE ex_dividend_date BETWEEN @start AND @today
    """, {"start": start.date(), "today": today.date()})
    if divs.empty:
        divs = pd.DataFrame(columns=["ex_dividend_date","ticker","dividend_per_share"])
    divs["ex_dividend_date"] = pd.to_datetime(divs["ex_dividend_date"])
    divs = (divs.pivot(index="ex_dividend_date", columns="ticker", values="dividend_per_share")
               .reindex(prices.index).fillna(0))

    # 환율 (USD → KRW; asset_class 기준 통화 결정)
    fx = bq_read(f"""
        SELECT inquiry_date, rate
        FROM `{T('daily_exchange_rates')}`
        WHERE inquiry_date BETWEEN @start AND @today
    """, {"start": start.date(), "today": today.date()})
    if fx.empty:
        logger.warning(f"{start.date()} ~ {today.date()} 기간에 환율 데이터가 없습니다. USD 자산은 계산에서 제외됩니다.")
        fx_series = pd.Series(dtype=float, index=prices.index, name="rate")
    else:
        fx["inquiry_date"] = pd.to_datetime(fx["inquiry_date"])
        fx_series = fx.set_index("inquiry_date")["rate"]
    fx_series = fx_series.reindex(prices.index).ffill().bfill()

    # 제외 티커 제거
    prices.drop(columns=[c for c in exclude_tickers if c in prices.columns], inplace=True, errors="ignore")
    divs.drop(columns=[c for c in exclude_tickers if c in divs.columns], inplace=True, errors="ignore")

    # 통화 매핑
    cur_df = bq_read(f"SELECT ticker, currency, exchange_code FROM `{T('asset_class')}`")
    ex2cur = {"NASD":"USD","NYSE":"USD","AMEX":"USD","XNAS":"USD","XNYS":"USD","ARCX":"USD",
              "SEHK":"HKD","TSE":"JPY","KRX":"KRW","KOSPI":"KRW","KOSDAQ":"KRW"}
    cur_df["currency_final"] = cur_df["currency"]
    m = cur_df["currency_final"].isna() & cur_df["exchange_code"].notna()
    cur_df.loc[m, "currency_final"] = cur_df.loc[m, "exchange_code"].map(ex2cur)
    cur_map = cur_df.set_index("ticker")["currency_final"].to_dict()

    usd_cols = [c for c in prices.columns if cur_map.get(c) == "USD"]
    if usd_cols:
        fx_arr = fx_series.to_numpy()[:, None]
        prices.loc[:, usd_cols] = prices[usd_cols].to_numpy() * fx_arr
        divs.loc[:, usd_cols]   = divs[usd_cols].to_numpy() * fx_arr

    # 총수익률 = 가격수익률 + 배당/전일가
    rets_price = prices.pct_change()
    rets_div   = divs.div(prices.shift(1))
    returns    = rets_price.add(rets_div, fill_value=0)

    # 유효성 필터(60영업일 이상 존재)
    valid_cols = (prices.notna().sum() >= 60)
    returns    = returns.loc[:, valid_cols]
    returns    = returns.replace([np.inf, -np.inf], np.nan)
    returns = returns.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
    
    return returns

def _is_rebalance_day(d: date) -> bool:
    ts = pd.Timestamp(d)
    if REBALANCE_FREQ == "D": return True
    if REBALANCE_FREQ == "W-FRI": return ts.weekday() == 4
    if REBALANCE_FREQ == "M": return ts == (ts + pd.offsets.BMonthEnd(0))
    if REBALANCE_FREQ == "Q": return ts == (ts + pd.offsets.BQuarterEnd(startingMonth=12))
    return True

def _pandas_freq_from_setting() -> str:
    if REBALANCE_FREQ == "D": return "D"
    if REBALANCE_FREQ == "W-FRI": return "W-FRI"
    if REBALANCE_FREQ == "M": return "M"
    if REBALANCE_FREQ == "Q": return "Q-DEC"
    return "Q-DEC"

def _make_boundaries(idx: pd.DatetimeIndex, f: str) -> list[pd.Timestamp]:
    if idx is None or len(idx) == 0:
        return []
    try:
        idx = idx.tz_localize(None)
    except Exception:
        pass
    idx = pd.DatetimeIndex(idx).sort_values()
    s = pd.Series(idx, index=idx)
    cuts = s.groupby(idx.to_period(f)).last().tolist()
    if cuts and cuts[-1] != idx[-1]:
        cuts.append(idx[-1])
    return cuts

    
def _get_live_weights(today: date, exclude_tickers=()) -> dict[str, float]:
    df = bq_read(f"""
      SELECT ticker, SUM(COALESCE(eval_amount_krw, 0)) AS amt
      FROM `{T('analysis')}`
      WHERE inquiry_date = @d
      GROUP BY ticker
    """, {"d": today})
    if df.empty: return {}
    if exclude_tickers:
        df = df[~df["ticker"].isin(exclude_tickers)]
    total = float(df["amt"].sum())
    if total == 0: return {}
    return dict(zip(df["ticker"], (df["amt"] / total).astype(float)))

def _load_prev_weights(strategy: str, today: date) -> dict[str, float]:
    df = bq_read(f"""
      SELECT ticker, weight
      FROM `{T('target_allocations')}`
      WHERE strategy = @s
        AND run_date = (
          SELECT MAX(run_date)
          FROM `{T('target_allocations')}`
          WHERE strategy = @s AND run_date < @d
        )
    """, {"s": strategy, "d": today})
    return dict(zip(df.ticker, df.weight.astype(float))) if not df.empty else {}

def _stabilize_weights(w_raw: dict, w_prev: dict) -> dict:
    keys = set(w_raw) | set(w_prev)
    w0 = {k: w_prev.get(k, 0.0) for k in keys}
    w1 = {k: (1 - SMOOTH_ALPHA) * w0[k] + SMOOTH_ALPHA * w_raw.get(k, 0.0) for k in keys}
    w2 = {}
    for k in keys:
        delta = w1[k] - w0[k]
        if abs(delta) < HYSTERESIS_ABS:
            w2[k] = w0[k]
        else:
            delta = max(-MAX_WEIGHT_DELTA, min(MAX_WEIGHT_DELTA, delta))
            w2[k] = w0[k] + delta
    w2 = {k: v for k, v in w2.items() if v > 1e-6}
    s = sum(w2.values())
    return {k: v / s for k, v in w2.items()} if s > 0 else w2

def _calc_perf(weights: Dict[str, float], mu: pd.Series, Sigma: pd.DataFrame, risk_free_rate: float) -> Dict[str, float]:
    w_vec = pd.Series(mu.index.map(weights), index=mu.index).fillna(0).to_numpy(dtype=float)
    exp_ret = float(np.dot(w_vec, mu))
    vol     = float(np.sqrt(w_vec @ Sigma.to_numpy() @ w_vec)) if Sigma.size else np.nan
    sharpe  = (exp_ret - risk_free_rate) / vol if vol else np.nan
    return {"expected_return": exp_ret, "annual_volatility": vol, "sharpe_ratio": sharpe}

def _efficient_frontier(
    returns: pd.DataFrame,
    risk_free_rate: float,
    strategy: str,
    weight_bounds: Tuple[float, float] = (0, 0.3),
    w_prev: Dict[str, float] | None = None,
    lam_turn: float = 0.0,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    r = returns.replace([np.inf, -np.inf], np.nan).dropna(how="all").fillna(0.0).astype(float)
    if r.shape[0] < 2 or r.shape[1] < 2:
        w = {t: 1.0 / max(1, r.shape[1]) for t in r.columns}
        mu, Sigma = r.mean() * 252, _sanitize_cov(r.cov() * 252)
        perf = _calc_perf(w, mu, Sigma, risk_free_rate)
        return w, perf

    mu, Sigma = r.mean() * 252, _sanitize_cov(r.cov() * 252)
    ef = EfficientFrontier(mu, Sigma, weight_bounds=weight_bounds)
    ef.add_objective(objective_functions.L2_reg, gamma=1)

    if lam_turn > 0 and w_prev:
        order = list(r.columns)
        w_prev_vec = np.array([w_prev.get(t, 0.0) for t in order], dtype=float)
        def _turnover_penalty(w, *args, **kwargs):
            return lam_turn * cp.sum_squares(w - w_prev_vec)
        ef.add_objective(_turnover_penalty)

    try:
        if strategy == "MIN_VOL":
            ef.min_volatility()
        elif strategy == "MAX_SHARPE":
            ef.max_sharpe(risk_free_rate=risk_free_rate)
        else:
            raise ValueError(f"unknown strategy {strategy}")
        w = ef.clean_weights()
        perf = ef.portfolio_performance(verbose=False, risk_free_rate=risk_free_rate)
        return w, {"expected_return": perf[0], "annual_volatility": perf[1], "sharpe_ratio": perf[2]}
    except Exception as e:
        logger.error("EF(%s) 실패 → 균등가중 폴백: %s", strategy, e)
        w = {t: 1.0 / r.shape[1] for t in r.columns}
        perf = _calc_perf(w, mu, Sigma, risk_free_rate)        
        return w, perf

def _efficient_frontier_from_mu(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    risk_free_rate: float,
    weight_bounds: Tuple[float, float] = (0, 0.3),
    w_prev: Dict[str, float] | None = None,
    lam_turn: float = 0.0,
):
    Sigma = _sanitize_cov(Sigma)
    ef = EfficientFrontier(mu, Sigma, weight_bounds=weight_bounds)
    ef.add_objective(objective_functions.L2_reg, gamma=1)
    if lam_turn > 0 and w_prev:
        order = list(mu.index)
        w_prev_vec = np.array([w_prev.get(t, 0.0) for t in order], dtype=float)
        def _turnover_penalty(w, *args, **kwargs):
            return lam_turn * cp.sum_squares(w - w_prev_vec)
        ef.add_objective(_turnover_penalty)
    try:
        ef.max_sharpe(risk_free_rate=risk_free_rate)
        w = ef.clean_weights()
        perf = ef.portfolio_performance(verbose=False, risk_free_rate=risk_free_rate)
        return w, {"expected_return": perf[0], "annual_volatility": perf[1], "sharpe_ratio": perf[2]}
    except Exception as e:
        logger.error("EF(mu) 실패 → 균등가중 폴백: %s", e)
        n = len(mu.index)
        w = {t: 1.0 / n for t in mu.index}
        perf = _calc_perf(w, mu, Sigma, risk_free_rate)
        return w, perf

def _hrp(returns: pd.DataFrame, risk_free_rate: float):
    r = (returns.replace([np.inf, -np.inf], np.nan)
                 .fillna(0.0)
                 .astype(float))

    std = r.std(axis=0)
    keep = std[std > 1e-12].index.tolist()
    r = r[keep]
    if r.shape[1] < 2:
        w = {t: 1.0 / max(1, len(keep)) for t in keep}
        mu = r.mean() * 252 if len(keep) else pd.Series(dtype=float)
        Sigma = pd.DataFrame(np.zeros((len(keep), len(keep))), index=keep, columns=keep)
        Sigma = _sanitize_cov(Sigma) if len(keep) else Sigma
        perf = _calc_perf(w, mu, Sigma, risk_free_rate)
        return w, perf
    Sigma = _sanitize_cov(r.cov() * 252)
    try:
        hrp = HRPOpt(cov_matrix=Sigma)
        hrp.optimize()
        w = hrp.clean_weights()
    except Exception as e:
        logger.error("HRP 실패 → 균등가중 폴백: %s", e)
        w = {t: 1.0 / len(keep) for t in keep}
    mu = r.mean() * 252
    perf = _calc_perf(w, mu, Sigma, risk_free_rate)
    return w, perf

def _blend(w1: Dict[str, float], w2: Dict[str, float], r1: float, r2: float) -> Dict[str, float]:
    keys = set(w1) | set(w2)
    return {k: w1.get(k, 0)*r1 + w2.get(k, 0)*r2 for k in keys if (w1.get(k, 0)*r1 + w2.get(k, 0)*r2) > 1e-10}

def _calc_mdd(nav: pd.Series) -> float:
    dd = nav / nav.cummax() - 1
    return float(dd.min())

# ────────────────────── μ 예측치 로더 (as-of) ──────────────────────
def _load_mu_pred_asof(asof_date: date, smooth_days: int, mu_conf: float) -> pd.Series:
    mu_hist = bq_read(f"""
      SELECT run_date, ticker, exp_return_annual
      FROM `{T('predicted_expected_returns')}`
      WHERE run_date BETWEEN @start AND @asof
    """, {"start": asof_date - timedelta(days=smooth_days), "asof": asof_date})
    if mu_hist.empty:
        mu_last = bq_read(f"""
          SELECT ticker, exp_return_annual
          FROM `{T('predicted_expected_returns')}`
          WHERE run_date = (SELECT MAX(run_date) FROM `{T('predicted_expected_returns')}` WHERE run_date <= @asof)
        """, {"asof": asof_date})
        return mu_conf * (mu_last.set_index("ticker")["exp_return_annual"] if not mu_last.empty else pd.Series(dtype=float))
    mu_hist = mu_hist.sort_values(["ticker", "run_date"])
    mu_smoothed = (mu_hist.groupby("ticker")["exp_return_annual"]
                         .apply(lambda s: s.ewm(halflife=smooth_days/2).mean().iloc[-1]))
    return mu_conf * mu_smoothed

# ────────────────────── ① 실행용: 오늘자 가중치 산출 ──────────────────────
def batch_weights(today: date, rets_inc_stb: pd.DataFrame, rets_exc_stb: pd.DataFrame):
    scenarios = [
        {"name": "INC_STB", "include_stb": True},
        {"name": "EXC_STB", "include_stb": False},
    ]
    alloc_rows, port_rows = [], []
    rf = _get_risk_free_rate()
    mu_pred_all_today = _load_mu_pred_asof(today, MU_SMOOTH_DAYS, MU_CONFIDENCE)

    cash_tks = _get_cash_tickers(True)
    live_w = _get_live_weights(today, exclude_tickers=cash_tks)
    if live_w:
        live_cols = [c for c in live_w if c in rets_inc_stb.columns]
        live_w_filtered = {k: v for k, v in live_w.items() if k in rets_inc_stb.columns}
        if live_w_filtered:
            total_w = sum(live_w_filtered.values())
            if total_w > 0:
                live_w_final = {k: v / total_w for k, v in live_w_filtered.items()}
                mu, Sigma = rets_inc_stb[live_w_final.keys()].mean()*252, rets_inc_stb[live_w_final.keys()].cov()*252
                perf_live = _calc_perf(live_w_final, mu, Sigma, rf)
                alloc_rows += [{"run_date": today, "strategy": "MY_PORTFOLIO", "ticker": t, "weight": w}
                               for t, w in live_w_final.items()]
                port_rows.append({"run_date": today, "strategy": "MY_PORTFOLIO", **perf_live})

    for sc in scenarios:
        rets = rets_inc_stb if sc["include_stb"] else rets_exc_stb
        if rets.shape[1] < 2:
            logger.warning("%s: 유효 종목 < 2 → 스킵", sc["name"])
            continue
        rets_opt = rets.fillna(0)
        raw_by_strat: dict[str, dict[str, float]] = {}

        # MPT (MIN_VOL, MAX_SHARPE)
        for strat in ("MIN_VOL", "MAX_SHARPE"):
            strat_name = f"{strat}_{sc['name']}"
            prev_w = _load_prev_weights(strat_name, today)
            w_raw, perf = _efficient_frontier(rets_opt, rf, strat, w_prev=prev_w, lam_turn=TURNOVER_LAMBDA)
            raw_by_strat[strat] = w_raw
            w_stable = _stabilize_weights(w_raw, prev_w)
            alloc_rows += [{"run_date": today, "strategy": strat_name, "ticker": t, "weight": v}
                           for t, v in w_stable.items()]
            port_rows.append({"run_date": today, "strategy": strat_name, **perf})

        # HRP
        w_hrp_raw, perf_hrp = _hrp(rets_opt, rf)
        strat_name = f"HRP_{sc['name']}"
        prev_w = _load_prev_weights(strat_name, today)
        w_hrp_stable = _stabilize_weights(w_hrp_raw, prev_w)
        alloc_rows += [{"run_date": today, "strategy": strat_name, "ticker": t, "weight": v}
                       for t, v in w_hrp_stable.items()]
        port_rows.append({"run_date": today, "strategy": strat_name, **perf_hrp})

        # BLEND 60:40 (Sharpe vs HRP 원시 비중 결합)
        w_sharpe_raw = raw_by_strat.get("MAX_SHARPE", {})
        w_blend_raw  = _blend(w_sharpe_raw, w_hrp_raw, 0.6, 0.4)
        mu, Sigma = rets_opt.mean()*252, rets_opt.cov()*252
        perf_blend = _calc_perf(w_blend_raw, mu, Sigma, rf)
        strat_name = f"BLEND60_40_{sc['name']}"
        prev_w = _load_prev_weights(strat_name, today)
        w_blend_stable = _stabilize_weights(w_blend_raw, prev_w)
        alloc_rows += [{"run_date": today, "strategy": strat_name, "ticker": t, "weight": v}
                       for t, v in w_blend_stable.items()]
        port_rows.append({"run_date": today, "strategy": strat_name, **perf_blend})

        # FCAST_MAX_SHARPE (오늘까지의 μ 예측치 사용)
        cols = rets.columns.intersection(mu_pred_all_today.index)
        if len(cols) >= 2:
            mu_pred = mu_pred_all_today.loc[cols]
            Sigma   = rets_opt[cols].cov() * 252
            strat_name = f"FCAST_MAX_SHARPE_{sc['name']}"
            prev_w = _load_prev_weights(strat_name, today)
            w_fcast_raw, perf_fcast = _efficient_frontier_from_mu(
                mu_pred, Sigma, rf, w_prev=prev_w, lam_turn=TURNOVER_LAMBDA
            )
            w_fcast_stable = _stabilize_weights(w_fcast_raw, prev_w)
            alloc_rows += [{"run_date": today, "strategy": strat_name, "ticker": t, "weight": v}
                           for t, v in w_fcast_stable.items()]
            port_rows.append({"run_date": today, "strategy": strat_name, **perf_fcast})
        else:
            logger.warning("%s: FCAST_MAX_SHARPE 대상 티커 < 2 → 스킵", sc["name"])

    if not alloc_rows:
        logger.warning("저장할 결과가 없습니다.")
        return

    _bq_mirror_alloc_and_port(today, alloc_rows, port_rows)
    logger.info("가중치 %d건 / 성과 %d건 저장 완료 (BQ)", len(alloc_rows), len(port_rows))

# ────────────────────── ② 평가용: 워크포워드 백테스트 ──────────────────────
def backtest_walk_forward(today: date, returns: pd.DataFrame, base_strategy: str,
                          use_mu_pred: bool = False) -> tuple[pd.DataFrame, list[dict]]:
    """
    returns: 일별 총수익률(배당/환율 포함, KRW 기준), index=date, columns=ticker
    base_strategy: "MIN_VOL" | "MAX_SHARPE" | "HRP" | "BLEND60_40" | "FCAST_MAX_SHARPE"
    반환: (nav_df, alloc_rows)  # alloc_rows: 리밸런싱 시점 스냅샷
    """
    if returns.empty or returns.shape[1] < 2:
        return pd.DataFrame(columns=["run_date","strategy","nav_date","nav","daily_return","cum_return"]), []

    freq = _pandas_freq_from_setting()
    idx = returns.index
    cuts = _make_boundaries(idx, freq)  
    start_idx = 0
    nav = 1.0
    nav_parts = []
    alloc_rows = []

    rf = _get_risk_free_rate()
    w_prev = {}

    first_valid = returns.apply(pd.Series.first_valid_index)

    for cut in cuts:
        seg = returns.iloc[start_idx : idx.get_loc(cut) + 1]
        t0 = seg.index[0]
        train = returns.loc[:t0].iloc[:-1] 
        if train.empty:
            w_raw = {}
        else:
            valid_cols = (train.notna().sum() >= 60)  
            train = train.loc[:, valid_cols].fillna(0)

        # 시점 t0에서의 가중치 산출
        if train.shape[1] >= 2:
            if base_strategy == "HRP":
                w_raw, _ = _hrp(train, rf)
            elif base_strategy == "MIN_VOL":
                w_raw, _ = _efficient_frontier(train, rf, "MIN_VOL", w_prev=w_prev, lam_turn=TURNOVER_LAMBDA)
            elif base_strategy == "MAX_SHARPE":
                w_raw, _ = _efficient_frontier(train, rf, "MAX_SHARPE", w_prev=w_prev, lam_turn=TURNOVER_LAMBDA)
            elif base_strategy == "BLEND60_40":
                w_sharpe, _ = _efficient_frontier(train, rf, "MAX_SHARPE", w_prev=w_prev, lam_turn=TURNOVER_LAMBDA)
                w_hrp, _    = _hrp(train, rf)
                w_raw       = _blend(w_sharpe, w_hrp, 0.6, 0.4)
            elif base_strategy == "FCAST_MAX_SHARPE" and use_mu_pred:
                asof_d = (pd.Timestamp(t0) - BDay(1)).date()
                mu_pred = _load_mu_pred_asof(asof_d, MU_SMOOTH_DAYS, MU_CONFIDENCE)
                cols = train.columns.intersection(mu_pred.index)
                if len(cols) >= 2:
                    mu = mu_pred.loc[cols]
                    Sigma = _sanitize_cov(train[cols].cov() * 252)
                    w_raw, _ = _efficient_frontier_from_mu(mu, Sigma, rf, w_prev=w_prev, lam_turn=TURNOVER_LAMBDA)
                else:
                    w_raw = {}
            else:
                w_raw = {}
        else:
            w_raw = {}

        w_stable = _stabilize_weights(w_raw, w_prev) if w_raw else w_prev

        # 이 기간(seg)에서 ‘활성’ 가능한 종목으로 정규화
        active_cols = [c for c in seg.columns if (pd.notna(first_valid[c]) and first_valid[c] <= t0)]
        if active_cols:
            w_act = {c: w_stable.get(c, 0.0) for c in active_cols}
            s = sum(w_act.values())
            if s > 0:
                w_act = {k: v/s for k, v in w_act.items()}
                daily_ret = seg[active_cols].fillna(0).mul(pd.Series(w_act)).sum(axis=1)
            else:
                daily_ret = pd.Series(0.0, index=seg.index)
        else:
            daily_ret = pd.Series(0.0, index=seg.index)

        seg_nav = (1 + daily_ret).cumprod() * nav
        nav = float(seg_nav.iloc[-1])
        nav_parts.append(seg_nav)

        # 리밸런싱 시점 스냅샷
        if w_stable:
            for tkr, wt in w_stable.items():
                if wt > 1e-10:
                    alloc_rows.append({
                        "run_date": today,
                        "strategy": base_strategy,  
                        "rebalance_date": t0.date(),
                        "ticker": tkr,
                        "weight": float(wt)
                    })

        w_prev = w_stable
        start_idx = idx.get_loc(cut) + 1

    nav_series = pd.concat(nav_parts)
    nav_df = pd.DataFrame({
        "run_date": today,
        "strategy": base_strategy,  
        "nav_date": nav_series.index,
        "nav": nav_series.values
    })
    nav_df["daily_return"] = nav_df["nav"].pct_change().fillna(0)
    nav_df["cum_return"] = nav_df["nav"] / nav_df["nav"].iloc[0] - 1
    return nav_df, alloc_rows

def batch_backtest(today: date, returns_inc: pd.DataFrame, returns_exc: pd.DataFrame):
    """워크포워드 백테스트 (INC_STB / EXC_STB 모두)"""
    scenarios = [
        ("MIN_VOL","INC_STB", returns_inc, False),
        ("MAX_SHARPE","INC_STB", returns_inc, False),
        ("HRP","INC_STB", returns_inc, False),
        ("BLEND60_40","INC_STB", returns_inc, False),        

        ("MIN_VOL","EXC_STB", returns_exc, False),
        ("MAX_SHARPE","EXC_STB", returns_exc, False),
        ("HRP","EXC_STB", returns_exc, False),
        ("BLEND60_40","EXC_STB", returns_exc, False),        
    ]

    nav_rows, alloc_rows_bt = [], []

    for base, tag, rets, use_mu in scenarios:
        if rets.shape[1] < 2:
            logger.warning("%s_%s: 유효 종목 < 2 → 스킵", base, tag)
            continue

        nav_df, alloc_bt = backtest_walk_forward(today, rets, base, use_mu_pred=use_mu)
        if nav_df.empty:
            logger.warning("%s_%s: NAV 없음/실패", base, tag)
            continue


        strat = f"{base}_{tag}"
        nav_df = nav_df.copy()
        nav_df["strategy"] = strat
        nav_df["nav_date"] = pd.to_datetime(nav_df["nav_date"]).dt.date
        nav_rows.extend(nav_df.to_dict("records"))

        for r in alloc_bt:
            r["strategy"] = strat  # base → strat로 치환
        alloc_rows_bt.extend(alloc_bt)

    _bq_mirror_backtest_nav(today, nav_rows)
    _bq_mirror_backtest_allocs(today, alloc_rows_bt)
    _bq_update_mdd_today(today)
    logger.info("워크포워드 NAV %d행 / 배분 스냅샷 %d행 저장 완료", len(nav_rows), len(alloc_rows_bt))

# ────────────────────── 테이블 초기화 ──────────────────────
def truncate_all():
    _bq_truncate_all()
    logger.info("🗑️  BigQuery 결과 테이블 TRUNCATE 완료")

# ────────────────────── 엔트리포인트 ──────────────────────
def batch_all():
    today  = datetime.now(ZoneInfo("Asia/Seoul")).date()
    is_rb  = _is_rebalance_day(today)

    if not is_rb and not FORCE_DAILY_WEIGHTS:
        _bq_copy_last_alloc_and_port_as_today(today)
        _bq_update_mdd_today(today)        
        return
    
    start_date = date(2000, 1, 1)
    today_datetime = datetime.combine(today, datetime.min.time())
    start_datetime = datetime.combine(start_date, datetime.min.time())

    logger.info("모든 자산의 수익률 데이터를 BigQuery에서 로딩합니다...")
    all_returns = _calc_returns(start_datetime, today_datetime, exclude_tickers=())
    if all_returns.empty:
        logger.error("수익률 데이터가 비어있어 계산을 중단합니다.")
        return

    exclude_tickers_inc = _get_cash_tickers(include_short_term_bond=True)
    exclude_tickers_exc = _get_cash_tickers(include_short_term_bond=False)

    cols_inc = [c for c in all_returns.columns if c not in exclude_tickers_inc]
    cols_exc = [c for c in all_returns.columns if c not in exclude_tickers_exc]

    returns_inc_stb = all_returns[cols_inc]
    returns_exc_stb = all_returns[cols_exc]
    
    logger.info(
        "수익률 데이터 준비 완료: INC_STB 시나리오 (%d개 자산), EXC_STB 시나리오 (%d개 자산)",
        len(cols_inc), len(cols_exc)
    )

    batch_weights(today, returns_inc_stb, returns_exc_stb)

    if is_rb or FORCE_DAILY_WEIGHTS:
        batch_backtest(today, returns_inc_stb, returns_exc_stb)

    logger.info("📎실행완료")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--truncate", action="store_true", help="결과 테이블 전체 TRUNCATE")
    args = parser.parse_args()
    if args.truncate:
        truncate_all()
    else:
        batch_all()
