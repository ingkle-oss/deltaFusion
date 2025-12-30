# delta_fusion 사용 가이드

Delta Lake 테이블을 고성능으로 쿼리하기 위한 Python 라이브러리.
Rust + DataFusion 기반으로 GIL 해제 및 zero-copy Arrow 전송을 지원합니다.

## 목차

- [설치](#설치)
- [기본 사용법](#기본-사용법)
- [쿼리 방식](#쿼리-방식)
- [테이블 관리](#테이블-관리)
- [데이터 쓰기](#데이터-쓰기)
- [스키마 진화](#스키마-진화)
- [시계열 데이터](#시계열-데이터)
- [S3 스토리지](#s3-스토리지)
- [성능 팁](#성능-팁)

---

## 설치

```bash
# 기본 (의존성 없음, query_to_dicts만 사용 가능)
pip install delta_fusion

# PyArrow 지원
pip install delta_fusion[pyarrow]

# Polars 지원
pip install delta_fusion[polars]

# 모두 설치
pip install delta_fusion[all]
```

---

## 기본 사용법

### 엔진 생성

```python
from delta_fusion import DeltaEngine

# 로컬 파일시스템
engine = DeltaEngine()

# S3 스토리지
engine = DeltaEngine(storage_options={
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "aws_region": "ap-northeast-2",
})
```

### 테이블 등록 및 쿼리

```python
# 테이블 등록
engine.register_table("users", "/path/to/delta/table")

# SQL 쿼리 (PyArrow)
batches = engine.query("SELECT * FROM users WHERE age > 20")
table = pa.Table.from_batches(batches)
df = table.to_pandas()

# Polars로 쿼리
df = engine.query_polars("SELECT * FROM users")

# Dict로 쿼리 (의존성 없음)
rows = engine.query_to_dicts("SELECT * FROM users")
```

---

## 쿼리 방식

3가지 쿼리 방식을 지원합니다:

### 1. PyArrow (권장)

```python
# 필요: pip install delta_fusion[pyarrow]
import pyarrow as pa

batches = engine.query("SELECT * FROM t")
table = pa.Table.from_batches(batches)

# pandas 변환
df = table.to_pandas()

# 컬럼 접근
ages = table.column("age").to_pylist()
```

**장점**: Zero-copy, 가장 빠름, pandas/numpy 호환

### 2. Polars

```python
# 필요: pip install delta_fusion[polars]

df = engine.query_polars("SELECT * FROM t")

# Polars 연산 체이닝
result = (
    df.filter(pl.col("age") > 20)
    .group_by("city")
    .agg(pl.col("salary").mean())
)

# Lazy 평가
lazy_df = df.lazy().with_columns(...)
```

**장점**: 현대적 API, 빠른 연산, 메모리 효율적

### 3. Dict (의존성 없음)

```python
# 추가 설치 불필요

rows = engine.query_to_dicts("SELECT * FROM t")
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

# JSON 직렬화
import json
print(json.dumps(rows))

# 리스트 컴프리헨션
names = [row["name"] for row in rows]
```

**장점**: 외부 의존성 없음, 단순함
**단점**: 대용량 데이터에서 느림

---

## 테이블 관리

### 등록/해제

```python
# 테이블 등록
engine.register_table("sales", "/data/sales")

# 특정 버전으로 등록 (time travel)
engine.register_table("sales_v1", "/data/sales", version=1)

# 등록 확인
engine.is_registered("sales")  # True

# 목록 조회
engine.list_tables()  # ["sales", "sales_v1"]

# 등록 해제
engine.deregister_table("sales")
```

### 테이블 정보

```python
info = engine.table_info("/data/sales")
# {
#     "path": "/data/sales",
#     "version": 5,
#     "schema": "...",
#     "num_files": 42,
#     "partition_columns": ["date"]
# }
```

### 새로고침

```python
# 단일 테이블 새로고침 (외부에서 변경된 경우)
engine.refresh_table("sales")

# 모든 테이블 새로고침
engine.refresh_all()
```

---

## 데이터 쓰기

### 새 테이블 생성

```python
import pyarrow as pa

# 데이터로 테이블 생성
data = pa.table({
    "id": [1, 2, 3],
    "name": ["A", "B", "C"],
    "value": [10.0, 20.0, 30.0],
})
engine.write("/data/new_table", data, mode="error")

# 스키마만으로 빈 테이블 생성
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
])
engine.create_table("/data/empty_table", schema)

# 파티션 테이블 생성
engine.create_table(
    "/data/partitioned",
    schema,
    partition_columns=["date"]
)
```

### 쓰기 모드

```python
# append: 기존 데이터에 추가 (기본값)
engine.write(path, data, mode="append")

# overwrite: 기존 데이터 교체
engine.write(path, data, mode="overwrite")

# error: 테이블 존재 시 에러
engine.write(path, data, mode="error")

# ignore: 테이블 존재 시 무시
engine.write(path, data, mode="ignore")
```

### 등록된 테이블에 쓰기

```python
engine.register_table("users", "/data/users")

# 등록된 이름으로 쓰기 (자동 refresh)
engine.write_to_table("users", new_data, mode="append")
```

---

## 스키마 진화

append 모드에서 새 컬럼이 자동으로 추가됩니다.

```python
# 초기 테이블: id, name
engine.write(path, pa.table({
    "id": [1, 2],
    "name": ["A", "B"],
}), mode="error")

# 새 컬럼 포함 데이터 추가
engine.write(path, pa.table({
    "id": [3],
    "name": ["C"],
    "age": [30],  # 새 컬럼 - 자동 추가됨
}))

# 결과: id, name, age (기존 행의 age는 NULL)
```

### schema_mode 옵션

```python
# merge: 새 컬럼 자동 추가 (append 기본값)
engine.write(path, data, schema_mode="merge")

# overwrite: 스키마 전체 교체
engine.write(path, data, schema_mode="overwrite")

# 엄격 모드 (스키마 불일치 시 에러)
engine.write(path, data, schema_mode=None)
```

---

## 시계열 데이터

Delta 로그를 우회하여 파티션 기반으로 빠르게 시계열 데이터를 조회합니다.

### 단일 파티션 (date=YYYY-MM-DD)

```python
# 등록 (즉시 완료, 데이터 읽지 않음)
engine.register_time_series(
    name="sensor",
    path="/data/sensor",
    timestamp_col="timestamp",
    partition_col="date",  # 기본값
)

# 시간 범위 조회 (해당 파티션만 읽음)
batches = engine.read_time_range(
    "sensor",
    start="2024-01-01T08:00:00",
    end="2024-01-01T18:00:00",
)

# Polars로 조회
df = engine.read_time_range_polars(
    "sensor",
    start="2024-01-01T00:00:00",
    end="2024-01-02T00:00:00",
)
```

### 계층적 파티션 (year/month/day)

```python
# year=2024/month=01/day=15/ 형태
engine.register_time_series_hierarchical(
    name="events",
    path="/data/events",
    timestamp_col="event_time",
    partition_cols=["year", "month", "day"],
    partition_formats=["%Y", "%m", "%d"],
)

df = engine.read_time_range_polars(
    "events",
    start="2024-01-15T00:00:00",
    end="2024-01-20T00:00:00",
)
```

### 직접 조회 (등록 없이)

```python
batches = engine.read_time_range_direct(
    path="/data/sensor",
    timestamp_col="timestamp",
    start="2024-01-01T00:00:00",
    end="2024-01-02T00:00:00",
    partition_col="date",
)
```

---

## S3 스토리지

### AWS S3

```python
engine = DeltaEngine(storage_options={
    "aws_access_key_id": "AKIA...",
    "aws_secret_access_key": "...",
    "aws_region": "ap-northeast-2",
})

engine.register_table("logs", "s3://my-bucket/delta/logs")
```

### MinIO (S3 호환)

```python
engine = DeltaEngine(storage_options={
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "aws_endpoint": "http://localhost:9000",
    "aws_allow_http": "true",
})
```

### 환경 변수 사용

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=ap-northeast-2
```

```python
# storage_options 없이 자동 사용
engine = DeltaEngine()
engine.register_table("t", "s3://bucket/table")
```

---

## 성능 팁

### 1. 쿼리 방식 선택

```
대용량 데이터: query() 또는 query_polars()
소량 데이터:   query_to_dicts()
```

### 2. 프로젝션 푸시다운

```python
# 나쁨: 모든 컬럼 조회
engine.query("SELECT * FROM huge_table")

# 좋음: 필요한 컬럼만
engine.query("SELECT id, name FROM huge_table")
```

### 3. 필터 푸시다운

```python
# 나쁨: 전체 로드 후 필터
df = engine.query_polars("SELECT * FROM t")
df = df.filter(pl.col("date") == "2024-01-01")

# 좋음: SQL에서 필터
df = engine.query_polars("SELECT * FROM t WHERE date = '2024-01-01'")
```

### 4. 시계열은 전용 API 사용

```python
# 나쁨: Delta 로그 파싱 필요
engine.register_table("sensor", path)
engine.query("SELECT * FROM sensor WHERE date = '2024-01-01'")

# 좋음: 파티션 직접 접근
engine.register_time_series("sensor", path, "timestamp")
engine.read_time_range("sensor", "2024-01-01T00:00:00", "2024-01-02T00:00:00")
```

### 5. 테이블 캐싱

```python
# 등록 시 테이블 메타데이터 캐싱됨
engine.register_table("t", path)

# 반복 쿼리는 빠름
for i in range(100):
    engine.query(f"SELECT * FROM t WHERE id = {i}")

# 외부 변경 후에만 refresh
engine.refresh_table("t")
```

---

## 에러 처리

```python
from delta_fusion import DeltaEngine

engine = DeltaEngine()

# 존재하지 않는 테이블
try:
    engine.register_table("t", "/nonexistent")
except OSError as e:
    print(f"테이블을 찾을 수 없음: {e}")

# 잘못된 쿼리
try:
    engine.query("SELECT * FROM unknown_table")
except OSError as e:
    print(f"쿼리 에러: {e}")

# 스키마 불일치
try:
    engine.write(path, incompatible_data, schema_mode=None)
except OSError as e:
    print(f"스키마 에러: {e}")
```

---

## 예제 코드

전체 예제는 [examples/](../examples/) 디렉토리 참조:

- `01_basic_query.py` - 기본 쿼리
- `02_polars_integration.py` - Polars 연동
- `03_time_series.py` - 시계열 데이터
- `04_schema_evolution.py` - 스키마 진화
- `05_no_dependencies.py` - 의존성 없는 사용
- `06_join_and_time_travel.py` - JOIN 및 버전 조회
- `07_s3_storage.py` - S3 설정
