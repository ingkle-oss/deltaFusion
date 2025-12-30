# API Reference

## DeltaEngine

메인 엔진 클래스. 모든 Delta Lake 작업의 진입점입니다.

### 생성자

```python
DeltaEngine(storage_options: dict | None = None)
```

**Parameters:**
- `storage_options`: 스토리지 설정 (선택)
  - `aws_access_key_id`: AWS 액세스 키
  - `aws_secret_access_key`: AWS 시크릿 키
  - `aws_region`: AWS 리전
  - `aws_endpoint`: 커스텀 엔드포인트 (MinIO 등)
  - `aws_allow_http`: HTTP 허용 여부 (`"true"`)

**Example:**
```python
# 로컬
engine = DeltaEngine()

# S3
engine = DeltaEngine(storage_options={
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "aws_region": "us-east-1",
})
```

---

## 테이블 관리

### register_table

```python
register_table(name: str, path: str, version: int | None = None) -> None
```

Delta 테이블을 등록합니다.

**Parameters:**
- `name`: 쿼리에서 사용할 테이블 이름
- `path`: Delta 테이블 경로 (로컬 또는 `s3://`)
- `version`: 특정 버전 (time travel)

**Raises:**
- `OSError`: 경로가 유효하지 않거나 Delta 테이블이 아닌 경우

---

### deregister_table

```python
deregister_table(name: str) -> None
```

등록된 테이블을 해제합니다.

---

### refresh_table

```python
refresh_table(name: str) -> None
```

등록된 테이블을 새로고침하여 최신 변경사항을 반영합니다.

---

### refresh_all

```python
refresh_all() -> None
```

모든 등록된 테이블을 새로고침합니다.

---

### is_registered

```python
is_registered(name: str) -> bool
```

테이블 등록 여부를 확인합니다.

---

### list_tables

```python
list_tables() -> list[str]
```

등록된 모든 테이블 이름을 반환합니다.

---

### table_info

```python
table_info(name_or_path: str) -> dict
```

테이블 메타데이터를 반환합니다.

**Returns:**
```python
{
    "path": str,              # 테이블 경로
    "version": int,           # 현재 버전
    "schema": str,            # 스키마 문자열
    "num_files": int,         # 파일 수
    "partition_columns": list # 파티션 컬럼
}
```

---

## 쿼리

### query

```python
query(sql: str) -> list[pyarrow.RecordBatch]
```

SQL 쿼리를 실행하고 PyArrow RecordBatch 리스트를 반환합니다.

**Requires:** `pip install delta_fusion[pyarrow]`

**Example:**
```python
batches = engine.query("SELECT * FROM users")
table = pa.Table.from_batches(batches)
```

---

### query_polars

```python
query_polars(sql: str) -> polars.DataFrame
```

SQL 쿼리를 실행하고 Polars DataFrame을 반환합니다.

**Requires:** `pip install delta_fusion[polars]`

**Example:**
```python
df = engine.query_polars("SELECT * FROM users")
```

---

### query_to_dicts

```python
query_to_dicts(sql: str) -> list[dict]
```

SQL 쿼리를 실행하고 딕셔너리 리스트를 반환합니다.

**Note:** 외부 의존성 없음. 대용량 데이터에서는 느림.

**Example:**
```python
rows = engine.query_to_dicts("SELECT * FROM users")
# [{"id": 1, "name": "Alice"}, ...]
```

---

## 시계열

### register_time_series

```python
register_time_series(
    name: str,
    path: str,
    timestamp_col: str,
    partition_col: str = "date",
    partition_format: str | None = None,
) -> None
```

시계열 테이블을 등록합니다. 데이터를 읽지 않으며 즉시 완료됩니다.

**Parameters:**
- `name`: 시계열 이름
- `path`: 데이터 경로
- `timestamp_col`: 타임스탬프 컬럼명
- `partition_col`: 파티션 컬럼명 (기본: `"date"`)
- `partition_format`: 날짜 포맷 (기본: `"%Y-%m-%d"`)

---

### register_time_series_hierarchical

```python
register_time_series_hierarchical(
    name: str,
    path: str,
    timestamp_col: str,
    partition_cols: list[str],
    partition_formats: list[str],
) -> None
```

계층적 파티션 구조의 시계열을 등록합니다.

**Example:**
```python
# year=2024/month=01/day=15/ 구조
engine.register_time_series_hierarchical(
    "events",
    "/data/events",
    "timestamp",
    ["year", "month", "day"],
    ["%Y", "%m", "%d"],
)
```

---

### read_time_range

```python
read_time_range(
    name: str,
    start: str,
    end: str,
) -> list[pyarrow.RecordBatch]
```

시간 범위 데이터를 조회합니다. Delta 로그를 우회하여 빠릅니다.

**Parameters:**
- `name`: 등록된 시계열 이름
- `start`: 시작 시간 (ISO 8601, 예: `"2024-01-01T00:00:00"`)
- `end`: 종료 시간 (ISO 8601)

---

### read_time_range_polars

```python
read_time_range_polars(
    name: str,
    start: str,
    end: str,
) -> polars.DataFrame
```

시간 범위 데이터를 Polars DataFrame으로 반환합니다.

---

### read_time_range_direct

```python
read_time_range_direct(
    path: str,
    timestamp_col: str,
    start: str,
    end: str,
    partition_col: str = "date",
) -> list[pyarrow.RecordBatch]
```

등록 없이 직접 시간 범위 데이터를 조회합니다.

---

### deregister_time_series

```python
deregister_time_series(name: str) -> None
```

시계열 등록을 해제합니다.

---

### is_time_series_registered

```python
is_time_series_registered(name: str) -> bool
```

시계열 등록 여부를 확인합니다.

---

### list_time_series

```python
list_time_series() -> list[str]
```

등록된 모든 시계열 이름을 반환합니다.

---

## 쓰기

### create_table

```python
create_table(
    path: str,
    schema: pyarrow.Schema,
    partition_columns: list[str] | None = None,
) -> None
```

빈 Delta 테이블을 생성합니다.

**Requires:** `pip install delta_fusion[pyarrow]`

**Example:**
```python
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
])
engine.create_table("/data/new_table", schema)
```

---

### write

```python
write(
    path: str,
    data: pyarrow.Table | list[pyarrow.RecordBatch],
    mode: str = "append",
    partition_columns: list[str] | None = None,
    schema_mode: str | None = None,
) -> None
```

Delta 테이블에 데이터를 씁니다.

**Parameters:**
- `path`: 테이블 경로
- `data`: PyArrow Table 또는 RecordBatch 리스트
- `mode`: 쓰기 모드
  - `"append"`: 추가 (기본값)
  - `"overwrite"`: 교체
  - `"error"`: 존재 시 에러
  - `"ignore"`: 존재 시 무시
- `partition_columns`: 파티션 컬럼 (새 테이블만)
- `schema_mode`: 스키마 모드
  - `"merge"`: 새 컬럼 자동 추가 (append 기본값)
  - `"overwrite"`: 스키마 교체
  - `None`: 엄격 모드

---

### write_to_table

```python
write_to_table(
    name: str,
    data: pyarrow.Table | list[pyarrow.RecordBatch],
    mode: str = "append",
) -> None
```

등록된 테이블에 데이터를 씁니다. 쓰기 후 자동으로 refresh됩니다.

---

## 타입 힌트

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl

def process_data(engine: DeltaEngine) -> "pl.DataFrame":
    return engine.query_polars("SELECT * FROM t")
```
