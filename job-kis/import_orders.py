from flask import Flask, request
import os
import json
import requests
import traceback
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import sqlalchemy.orm
from google.cloud import firestore


# 기본 설정
START_DATE_STR = "20220101"
END_DATE_STR = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
app = Flask(__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# Firestore 클라이언트 초기화 (토큰 저장/조회용)
try:
    db = firestore.Client()
except Exception as e:
    logger.warning(f"Firestore 클라이언트 초기화 실패 (로컬 환경일 수 있음): {e}")
    db = None

# Cloud SQL 연결 설정
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "portfolio_db")
connector = Connector()
db_engine: sqlalchemy.Engine | None = None
Base = sqlalchemy.orm.declarative_base()

def get_conn():
    """Cloud SQL 인스턴스에 연결합니다."""
    logger.info("DB 연결 시도")
    return connector.connect(
        INSTANCE_CONNECTION_NAME, "pg8000", user=DB_USER,
        password=DB_PASS, db=DB_NAME, ip_type=IPTypes.PRIVATE
    )


# 테이블 정의 (Order 테이블만 필요)
class Order(Base):
    __tablename__ = "orders"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    account_nickname = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    account_number = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    order_date = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    order_number = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    market = sqlalchemy.Column(sqlalchemy.String(50))
    ticker = sqlalchemy.Column(sqlalchemy.String(50), nullable=False)
    product_name = sqlalchemy.Column(sqlalchemy.String(100), nullable=False)
    order_type = sqlalchemy.Column(sqlalchemy.String(50))
    total_quantity = sqlalchemy.Column(sqlalchemy.Numeric(20, 4))
    avg_price = sqlalchemy.Column(sqlalchemy.Numeric(20, 4))
    total_amount = sqlalchemy.Column(sqlalchemy.Numeric(20, 4))
    currency = sqlalchemy.Column(sqlalchemy.String(10))
    __table_args__ = (sqlalchemy.UniqueConstraint('account_number', 'order_number', 'market', name='_account_order_market_uc'),)


# KIS 헬퍼
def get_kis_configs():
    logger.info("SecretManager에서 KIS 계정 불러오는 중…")
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id: raise ValueError("GCP_PROJECT_ID 환경 변수가 설정되지 않았습니다.")
    name = f"projects/{project_id}/secrets/KISAPI/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode())

def get_new_kis_token(app_key: str, app_secret: str) -> dict | None:
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    payload = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}
    try:
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        data = res.json()
        expires_in = int(data.get("expires_in", 0)) - 60
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        return {"token": data.get("access_token"), "expires_at": expires_at}
    except Exception as e:
        logger.error("새 토큰 발급 실패 (appkey: ...%s): %s", app_key[-4:], e); return None

def get_or_refresh_token(app_key: str, app_secret: str) -> str | None:
    if not db:
        logger.warning("Firestore client가 없어 매번 새 토큰을 발급합니다.")
        new_token_data = get_new_kis_token(app_key, app_secret)
        return new_token_data.get('token') if new_token_data else None

    token_doc_ref = db.collection('api_tokens').document(f'kis_token_{app_key}')
    try:
        doc = token_doc_ref.get()
        if doc.exists:
            token_data = doc.to_dict()
            if token_data.get('expires_at') and token_data['expires_at'].replace(tzinfo=timezone.utc) > datetime.now(timezone.utc):
                logger.info("Firestore에서 유효한 토큰 발견 (appkey: ...%s)", app_key[-4:]); return token_data.get('token')
            else:
                logger.info("Firestore 토큰 만료, 새 토큰 발급 시도 (appkey: ...%s)", app_key[-4:])
    except Exception as e:
        logger.error(f"Firestore에서 토큰 읽기 실패: {e}")

    new_token_data = get_new_kis_token(app_key, app_secret)
    if new_token_data and new_token_data.get('token'):
        try:
            token_doc_ref.set(new_token_data)
            logger.info("새 토큰을 발급하여 Firestore에 저장 완료 (appkey: ...%s)", app_key[-4:]); return new_token_data.get('token')
        except Exception as e:
            logger.error(f"Firestore에 토큰 저장 실패: {e}"); return new_token_data.get('token')
    return None

def fetch_kis_api(url: str, headers: dict, params: dict) -> dict | None:
    all_outputs = {"output1": [], "output2": []}
    ctx_fk_key_map = {'TTTC0081R': 'CTX_AREA_FK100', 'CTSC9215R': 'CTX_AREA_FK100', 'CTOS4001R': 'CTX_AREA_FK100'}
    ctx_nk_key_map = {'TTTC0081R': 'CTX_AREA_NK100', 'CTSC9215R': 'CTX_AREA_NK100', 'CTOS4001R': 'CTX_AREA_NK100'}
    tr_id = headers.get("tr_id")

    if tr_id not in ctx_fk_key_map:
        logger.error(f"지원하지 않는 tr_id({tr_id})입니다. 연속 조회가 불가할 수 있습니다.")
        return None

    ctx_fk_key = ctx_fk_key_map[tr_id]
    ctx_nk_key = ctx_nk_key_map[tr_id]
    original_params = params.copy()

    while True:
        try:
            res = requests.get(url, headers=headers, params=params, timeout=20)
            res.raise_for_status()
            data = res.json()
            if data.get("rt_cd") != "0":
                if data.get("msg_cd") not in ["EGW00121", "APBK0013"]:
                    logger.warning("KIS API 오류: %s (tr_id: %s)", data.get("msg1"), tr_id)
                break

            output_key = "output" if "output" in data else "output1"
            if output_key in data and data[output_key]: all_outputs["output1"].extend(data[output_key])

            tr_cont = res.headers.get('tr_cont', '')
            if tr_cont in ('F', 'M'):
                params = original_params.copy()
                params['tr_cont'] = 'N'
                fk_val = data.get(ctx_fk_key.lower())
                nk_val = data.get(ctx_nk_key.lower())

                params[ctx_fk_key] = fk_val
                params[ctx_nk_key] = nk_val

                if not fk_val or not nk_val: break
                logger.info(f"연속 조회 진행... (tr_id: {tr_id})")
            else:
                break
        except Exception as e:
            logger.error("API 호출 실패: %s (tr_id: %s)", e, tr_id); return None

    return all_outputs if all_outputs["output1"] else None


# 메인 주문 기록 함수
def import_historical_orders(start_date_str, end_date_str):
    global db_engine
    if db_engine is None:
        db_engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=get_conn, pool_pre_ping=True)

    Base.metadata.create_all(db_engine)
    Session = sqlalchemy.orm.sessionmaker(bind=db_engine, expire_on_commit=False)
    session = Session()

    logger.info("DB에서 기존 주문 내역을 불러와 중복을 방지합니다...")
    try:
        existing_orders_query = session.query(Order.account_number, Order.order_number, Order.market).all()
        existing_orders = {f"{o.account_number}-{o.order_number}-{o.market}" for o in existing_orders_query}
        logger.info(f"현재 DB에 저장된 주문 수: {len(existing_orders)}건")
    except Exception as e:
        logger.error(f"기존 주문 내역 조회 실패: {e}")
        session.close()
        return

    try:
        configs = get_kis_configs()
        accounts = configs.get("ACCOUNTS", [])
    except Exception as e:
        logger.error("SecretManager 오류: %s", e)
        session.close()
        return

    total_new_orders = 0
    three_months_ago_str = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=90)).strftime("%Y%m%d")
    start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
    end_date = datetime.strptime(end_date_str, "%Y%m%d").date()
    current_start_date = start_date

    while current_start_date <= end_date:
        current_end_date = current_start_date + timedelta(days=89)
        if current_end_date > end_date:
            current_end_date = end_date

        chunk_start_str = current_start_date.strftime("%Y%m%d")
        chunk_end_str = current_end_date.strftime("%Y%m%d")
        logger.info(f"\n조회 구간 처리 시작: {chunk_start_str} ~ {chunk_end_str}")

        for acc in accounts:
            nickname, cano, prdt_cd = acc.get("nickname", "N/A"), acc.get("cano"), acc.get("prdt_cd", "01")
            if not cano:
                logger.warning("잘못된 계정 설정 발견(cano 없음), 건너뜁니다: %s", acc)
                continue

            logger.info("─" * 10 + f" 계좌 처리 시작: {nickname} ({cano}-{prdt_cd}) " + "─" * 10)
            token = get_or_refresh_token(acc["app_key"], acc["app_secret"])
            if not token:
                logger.error("토큰 없음 – %s 계좌 건너뜀", nickname)
                continue

            headers = {"authorization": f"Bearer {token}", "appkey": acc["app_key"], "appsecret": acc["app_secret"], "tr_id": ""}

            # 1. 국내 주식 주문 내역 조회
            try:
                if chunk_start_str < three_months_ago_str:
                    headers["tr_id"] = "CTSC9215R"
                else:
                    headers["tr_id"] = "TTTC0081R"
                logger.info(f"국내주식({headers['tr_id']}) 주문내역 조회")

                params_domestic = {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "INQR_STRT_DT": chunk_start_str, "INQR_END_DT": chunk_end_str, "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00", "PDNO": "", "CCLD_DVSN": "01", "ORD_GNO_BRNO": "", "ODNO": "", "INQR_DVSN_3": "00", "INQR_DVSN_1": "", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""}
                res_orders = fetch_kis_api(f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld", headers, params_domestic)
                
                if res_orders and res_orders.get("output1"):
                    added_in_call = 0
                    for o in res_orders["output1"]:
                        order_key = f"{cano}-{o['odno']}-국내"
                        if order_key in existing_orders: continue
                        
                        session.add(Order(account_nickname=nickname, account_number=cano, order_date=datetime.strptime(o["ord_dt"], "%Y%m%d").date(), order_number=o["odno"], market="국내", ticker=o["pdno"], product_name=o["prdt_name"], order_type=o["sll_buy_dvsn_cd_name"], total_quantity=o["tot_ccld_qty"], avg_price=o["avg_prvs"], total_amount=o["tot_ccld_amt"], currency="KRW"))
                        existing_orders.add(order_key)
                        added_in_call += 1
                    
                    if added_in_call > 0:
                        total_new_orders += added_in_call
                        logger.info(f"✔️ 국내주식: {len(res_orders['output1'])}건 조회, 신규 {added_in_call}건 추가")

            except Exception as e:
                logger.error(f"'{nickname}' 계좌 국내 주문 처리 중 오류: {e}", exc_info=True)

            # 2. 해외 주식 주문 내역 조회
            try:
                headers["tr_id"] = "CTOS4001R"
                logger.info(f"해외주식({headers['tr_id']}) 주문내역 조회")
                params_overseas = {"CANO": cano, "ACNT_PRDT_CD": prdt_cd, "ERLM_STRT_DT": chunk_start_str, "ERLM_END_DT": chunk_end_str, "OVRS_EXCG_CD": "", "PDNO": "", "SLL_BUY_DVSN_CD": "00", "LOAN_DVSN_CD": "", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""}
                res_overseas = fetch_kis_api(f"{KIS_BASE_URL}/uapi/overseas-stock/v1/trading/inquire-period-trans", headers, params_overseas)
                
                if res_overseas and res_overseas.get("output1"):
                    added_in_call = 0
                    for o in res_overseas["output1"]:
                        order_number_composite = f"{o['trad_dt']}-{o['pdno']}-{o['ccld_qty']}-{o['tr_frcr_amt2']}"
                        order_key = f"{cano}-{order_number_composite}-해외"
                        if order_key in existing_orders: continue

                        session.add(Order(account_nickname=nickname, account_number=cano, order_date=datetime.strptime(o["trad_dt"], "%Y%m%d").date(), order_number=order_number_composite, market="해외", ticker=o["pdno"], product_name=o["ovrs_item_name"], order_type=o["sll_buy_dvsn_name"], total_quantity=o["ccld_qty"], avg_price=o["ovrs_stck_ccld_unpr"], total_amount=o["tr_frcr_amt2"], currency=o["crcy_cd"]))
                        existing_orders.add(order_key)
                        added_in_call += 1
                    
                    if added_in_call > 0:
                        total_new_orders += added_in_call
                        logger.info(f"해외주식: {len(res_overseas['output1'])}건 조회, 신규 {added_in_call}건 추가")

            except Exception as e:
                logger.error(f"'{nickname}' 계좌 해외 주문 처리 중 오류: {e}", exc_info=True)
        
        # 다음 조회 기간으로 설정
        current_start_date = current_end_date + timedelta(days=1)

    try:
        if total_new_orders > 0:
            session.commit()
            logger.info(f"\n최종 커밋 완료! 총 {total_new_orders}건의 새로운 주문 내역을 DB에 저장했습니다.")
        else:
            logger.info("\n새로운 주문 내역이 없어 커밋할 내용이 없습니다.")
    except Exception as e:
        logger.error("최종 DB 커밋 실패: %s", e)
        session.rollback()
    finally:
        session.close()


# Cloud Run 수동 엔드포인트
@app.route("/import-orders", methods=["POST"])
def manual_trigger():
    logger.info(f"🛠️ 수동 /import-orders 트리거 호출 (조회 기간: {START_DATE_STR} ~ {END_DATE_STR})")
    try:
        import_historical_orders(START_DATE_STR, END_DATE_STR)
        return f"Order import complete. Processed period: {START_DATE_STR} to {END_DATE_STR}.", 200
    except Exception as e:
        logger.error("Manual Trigger 실패: %s", e, exc_info=True)
        return "Internal Server Error", 500

if __name__ == '__main__':
    logger.info("로컬에서 주문 기록 스크립트를 직접 실행합니다.")
    import_historical_orders(START_DATE_STR, END_DATE_STR)
    logger.info("스크립트 실행 완료.")