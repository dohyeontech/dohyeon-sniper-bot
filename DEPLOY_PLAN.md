# 🎯 24시간 0원 익명 클라우드 트레이딩 봇 & 대시보드 구축 플랜

> **작성일:** 2026-11-12  
> **목적:** 신용카드·휴대전화 번호 인증 0개 (100% 완전 익명) + 비용 0원(Free Tier)으로 24시간 무중단 실시간 코인 거래량 스나이퍼 봇 및 웹 대시보드 가동

---

## 1. 아키텍처 개요

```
[업비트 원화 마켓 (70~80개 알트)]
         │ 실시간 시세 / 5분봉 조회
         ▼
┌────────────────────────────────────────────────────────┐
│  Render Free Web Service (Python FastAPI)              │
│                                                        │
│  1. 백그라운드 스레드: 1분/5분 주기 실시간 거래량 스나이핑     │
│  2. 웹 대시보드 서빙: GET /dashboard (실시간 잔고/체결)     │
│  3. 헬스체크 엔드포인트: GET /health (Keep-Alive 수신)     │
└────────────────────────────────────────────────────────┘
    ▲                                    │ 매매 발생 시
    │ 10분마다 Ping                       │ state.json 동기화
    │                                    ▼
┌──────────────────────┐      ┌───────────────────────────┐
│ Cloudflare Worker    │      │ GitHub Gist (Private API) │
│ (Cron: */10 * * * *) │      │ 또는 Cloudflare KV        │
│ * 슬립(Spin-down) 방지│      │ * 컨테이너 재부팅 시 데이터 보존│
└──────────────────────┘      └───────────────────────────┘
```

---

## 2. 핵심 제약 극복 설계

| 도전 과제 | 해결 전략 | 비용 / 인증 |
| :--- | :--- | :--- |
| **카드 없는 24시간 호스팅** | **Render Free Web Service** (월 750시간 무료)<br>• 31일 기준 744시간 소모로 750시간 한도 내 안전 가동 | **0원** / 카드 0개 (이메일/깃허브 가입) |
| **15분 무요청 슬립 방지** | **Cloudflare Workers Cron Trigger**<br>• 10분마다 `/health` 엔드포인트로 자동 GET 핑 발송 | **0원** / 카드 0개 (일 10만 회 무료) |
| **플랫폼 어뷰징 제재 회피** | **실제 웹 대시보드 통합 서빙**<br>• 백그라운드 매크로가 아닌 정상적인 실시간 금융 웹앱으로 위장 | 합법적 Web Service 트래픽 인정 |
| **재부팅 시 데이터 증발** | **GitHub Gist API 원격 상태 동기화**<br>• 매매 체결 시 `state.json`을 Gist에 자동 커밋<br>• 컨테이너 시작 시 Gist에서 최신 잔고/전적 즉시 복원 | **0원** / 카드 0개 (기존 부캐 계정 활용) |

---

## 3. 단계별 상세 실행 계획 (Execution Roadmap)

### Phase 1. 봇 & 웹 대시보드 코드 통합 패키징 (Local)
1. **FastAPI 서버 구축 (`main.py`)**:
   - `GET /`: 대시보드 리다이렉트
   - `GET /dashboard`: 반응형 실시간 웹 대시보드 렌더링 (HTML/Tailwind)
   - `GET /health`: Cloudflare 핑 수신용 200 OK 응답
   - `GET /api/status`: JSON 상태 반환
2. **백그라운드 스나이퍼 루프 (`engine.py`)**:
   - FastAPI `lifespan` 또는 `asyncio.create_task`로 서버 시작 시 백그라운드 루프 구동
   - 1분/5분 주기로 업비트 70~80개 알트코인 거래량 급증 스캔 (`scanner.py`)
   - 진입 시 1분봉 기반 익절(+2.0%), 트레일링 스탑, 칼손절(-1.2%) 자동 실행
3. **영속성 모듈 (`persistence.py`)**:
   - 로컬 디스크 대신 GitHub Gist API를 통해 매매 체결 시 `state.json` 업로드/다운로드
4. **배포 설정 파일 작성**:
   - `requirements.txt`: `fastapi`, `uvicorn`, `requests`, `jinja2`
   - `render.yaml` 또는 기본 구동 커맨드: `uvicorn main:app --host 0.0.0.0 --port $PORT`

### Phase 2. 익명 깃허브 리포지토리 생성
1. '김도현' 부캐 깃허브 계정에 비공개 저장소 `dohyeon-sniper-bot` 생성
2. 코드 및 템플릿 푸시
3. Gist 접근용 GitHub Personal Access Token 발급

### Phase 3. Render 무료 웹 서비스 배포
1. Render.com 접속 후 깃허브 간편 로그인 (신용카드 등록 스킵)
2. `New +` -> `Web Service` 선택 후 `dohyeon-sniper-bot` 연결
3. 무료 인스턴스 타입(`Free`) 지정
4. 환경 변수 등록:
   - `GITHUB_TOKEN`: 상태 백업용 토큰
   - `GIST_ID`: `state.json` 저장용 Gist ID
5. 배포 완료 후 라이브 URL 확보 (`https://dohyeon-sniper.onrender.com`)

### Phase 4. Cloudflare Worker Keep-Alive 핑 설정
1. Cloudflare 대시보드 로그인 (카드 불필요)
2. `Workers & Pages` -> `Create Worker`
3. 핑 스크립트 작성 (초경량):
   ```javascript
   export default {
     async scheduled(event, env, ctx) {
       await fetch("https://dohyeon-sniper.onrender.com/health");
     }
   };
   ```
4. `Triggers` -> `Cron Triggers`에 `*/10 * * * *` (10분마다 실행) 등록

### Phase 5. 검증 및 모니터링 가동
1. 대시보드 웹페이지 접속 및 UI 실시간 갱신 확인
2. Cloudflare 핑 유입 및 Render 인스턴스 무중단 유지 확인
3. 업비트 거래량 급증 감지 및 가상 체결 정상 작동 확인
4. 인스턴스 수동 재시작 후 Gist 데이터 복원 무결성 검증

---

## 4. 기대 실익 및 결과물
- **완전 무인 24시간 실시간 스나이핑**: 낮 시간대/새벽 시간대 5분봉 거래량 급증 알트코인 누락 없이 100% 포착
- **실시간 대시보드**: 모바일 브라우저 북마크로 언제든 접속해 0초 딜레이 실시간 포트폴리오 확인
- **유지비 0원 & 익명성 100%**: 결제 수단 연결이 전혀 없으므로 과금 위험 제로, 신원 노출 위험 제로
