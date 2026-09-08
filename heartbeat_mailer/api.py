"""장비 관리 API. Kafka/Oracle/SMTP 설정 없이 독립 실행할 수 있다."""
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hmac
import json
import logging
import math
import os
import time

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, StrictBool
import psycopg

from .postgres import PostgresRepository


class MonitoringUpdate(BaseModel):
    """감시 활성화와 변경 사유 입력."""
    enabled: StrictBool
    reason: str = Field(default='', max_length=500)


def create_app(dsn=None, token=None, threshold=None):
    """입력: 선택적 DB/인증/기준값. 반환: 독립 실행 가능한 FastAPI 앱."""
    @asynccontextmanager
    async def lifespan(app):
        """입력: 앱. 반환: lifespan context. 필수 설정과 스키마를 기동 시 검증한다."""
        load_dotenv()
        app.state.dsn = dsn or os.environ['DATABASE_URL']
        app.state.token = token or os.environ['API_TOKEN']
        if len(app.state.token) < 24:
            raise ValueError('API_TOKEN must contain at least 24 characters')
        app.state.threshold = threshold if threshold is not None else float(os.getenv('HEARTBEAT_STALE_AFTER_SECONDS', '180'))
        if not math.isfinite(app.state.threshold) or app.state.threshold <= 0:
            raise ValueError('HEARTBEAT_STALE_AFTER_SECONDS must be positive')
        PostgresRepository(app.state.dsn).initialize()
        yield

    app = FastAPI(title='Collector Monitor API', version='1.0', lifespan=lifespan)
    security = HTTPBearer(auto_error=False)

    def authorize(credentials: HTTPAuthorizationCredentials | None = Depends(security)):
        """입력: Bearer 헤더. 반환: 없음. 인증 실패 시 401을 반환한다."""
        if not credentials or not hmac.compare_digest(
            credentials.credentials.encode(), app.state.token.encode()
        ):
            raise HTTPException(401, 'Invalid API token', headers={'WWW-Authenticate': 'Bearer'})

    def repository():
        """입력: 없음. 반환: 요청별 독립 저장소. 다른 요청의 트랜잭션과 공유하지 않는다."""
        return PostgresRepository(app.state.dsn)

    def present(row):
        """입력: DB 행. 반환: 공정 상태와 구분한 수신 상태 및 관리 설정 JSON."""
        last = row.get('last_seen_at')
        payload = json.loads(row['raw_payload']) if row.get('raw_payload') else {}
        data = payload.get('data', {})
        return {
            'deviceId': row['device_id'], 'enabled': row['enabled'], 'deleted': row['deleted'],
            'reason': row['reason'], 'updatedAt': row['updated_at'],
            'receptionStatus': 'UNKNOWN' if last is None else (
                'MISSING' if time.time() - last >= app.state.threshold else 'RECEIVING'),
            'lastSeenAt': datetime.fromtimestamp(last, timezone.utc) if last is not None else None,
            'thresholdSeconds': app.state.threshold,
            'missingAlertRegistered': row.get('stale_notified', False),
            'collectorStatus': data.get('status'), 'sourceInfo': data.get('sourceInfo'),
        }

    @app.exception_handler(psycopg.Error)
    async def database_error(request, exc):
        """입력: DB 예외. 반환: 접속정보를 포함하지 않는 503 응답."""
        from fastapi.responses import JSONResponse
        logging.getLogger(__name__).error('관리 API DB 작업 실패: type=%s', type(exc).__name__)
        return JSONResponse(status_code=503, content={'detail': 'Database unavailable'})

    @app.get('/health/live')
    def live():
        """입력: 없음. 반환: API 프로세스 생존 상태. Kafka 연결은 검사하지 않는다."""
        return {'status': 'alive'}

    @app.get('/health/ready')
    def ready(repo=Depends(repository)):
        """입력: 저장소. 반환: DB 연결 준비 상태."""
        with repo.connect() as conn:
            conn.execute('SELECT 1')
        return {'status': 'ready'}

    @app.get('/api/v1/devices', dependencies=[Depends(authorize)])
    def devices(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0),
                include_deleted: bool = False, repo=Depends(repository)):
        """입력: 페이지/삭제 포함 조건. 반환: 장비 목록과 페이지 정보."""
        return {'items': [present(row) for row in repo.list_devices(limit,offset,include_deleted)],
                'limit': limit, 'offset': offset}

    @app.get('/api/v1/devices/{device_id}', dependencies=[Depends(authorize)])
    def device(device_id: str, repo=Depends(repository)):
        """입력: 장비 ID. 반환: 상세 상태 또는 404."""
        row = repo.get_device(device_id)
        if row is None:
            raise HTTPException(404, 'Device not found')
        return present(row)

    @app.patch('/api/v1/devices/{device_id}/monitoring', dependencies=[Depends(authorize)])
    def monitoring(device_id: str, body: MonitoringUpdate, repo=Depends(repository)):
        """입력: 감시 활성화/사유. 반환: 변경 설정. 삭제 대상은 먼저 복원해야 한다."""
        with repo.transaction() as conn:
            repo.lock_management(device_id)
            row = conn.execute('SELECT * FROM devices WHERE device_id=%s FOR UPDATE', (device_id,)).fetchone()
            if row is None:
                raise HTTPException(404, 'Device not found')
            if row['deleted']:
                raise HTTPException(409, 'Restore deleted device first')
            repo.manage(device_id,body.enabled,False,body.reason)
            return present(repo.get_device(device_id))

    @app.delete('/api/v1/devices/{device_id}', dependencies=[Depends(authorize)])
    def delete(device_id: str, reason: str = Query('Deleted by API', max_length=500), repo=Depends(repository)):
        """입력: 장비 ID와 사유. 반환: 논리 삭제 설정. 이력과 재등록 방지 기록은 보존한다."""
        try:
            with repo.transaction():
                repo.manage(device_id,False,True,reason)
                return present(repo.get_device(device_id))
        except KeyError:
            raise HTTPException(404, 'Device not found')

    @app.post('/api/v1/devices/{device_id}/restore', dependencies=[Depends(authorize)])
    def restore(device_id: str, repo=Depends(repository)):
        """입력: 장비 ID. 반환: 복원 설정. 복원 뒤 감시 재개는 별도로 요청한다."""
        with repo.transaction() as conn:
            repo.lock_management(device_id)
            row = conn.execute('SELECT * FROM devices WHERE device_id=%s FOR UPDATE', (device_id,)).fetchone()
            if row is None:
                raise HTTPException(404, 'Device not found')
            if not row['deleted']:
                raise HTTPException(409, 'Device is not deleted')
            repo.manage(device_id,False,False,'Restored by API')
            return present(repo.get_device(device_id))

    return app


app = create_app()
