# Hướng dẫn: Cách setup cập nhật bảng hàng ngày trên Airflow

---

## Mục lục

- [Airflow là gì?](#airflow-là-gì)
- [Khái niệm 1: DAG là gì?](#khái-niệm-1-dag-là-gì)
- [Khái niệm 2: Plugin là gì?](#khái-niệm-2-plugin-là-gì)
- [Tại sao tách thành 2 file riêng?](#tại-sao-tách-thành-2-file-riêng-dag--plugin)
- [Toàn bộ hệ thống hoạt động như thế nào?](#toàn-bộ-hệ-thống-hoạt-động-như-thế-nào)
- [Cấu trúc thư mục cần biết](#cấu-trúc-thư-mục-cần-biết)
- [Các loại pipeline trong repo](#các-loại-pipeline-pattern-trong-repo)
- [Hướng dẫn step-by-step: Thêm bảng mới](#hướng-dẫn-step-by-step-thêm-một-bảng-daily-update-mới-loại-a)
- [Backfill — Chạy bù dữ liệu lịch sử](#backfill--chạy-bù-dữ-liệu-lịch-sử)
- [Cài đặt giờ chạy (cron)](#cài-đặt-giờ-chạy-cron)
- [Khi có lỗi — Slack sẽ báo gì?](#khi-có-lỗi--slack-sẽ-báo-gì)
- [Lỗi thường gặp khi viết DAG](#lỗi-thường-gặp-khi-viết-dag)
- [Xem log lỗi ở đâu?](#xem-log-lỗi-ở-đâu)
- [Nguyên tắc nền khi setup](#nguyên-tắc-nền-khi-setup-airflow-pipeline)
- [Bảng DAG cần sửa](#bảng-dag-hiện-tại-cần-sửa-gì)
- [Setup hiện tại cần revamp gì](#setup-hiện-tại-cần-revamp-gì)
- [CI/CD là gì?](#cicd-là-gì--và-repo-này-đang-dùng-ở-mức-nào)
- [Nguồn tham khảo](#nguồn-tham-khảo)

---

## Airflow là gì?

**Airflow = Công cụ tự động hóa data pipeline** — viết một lần, chạy mỗi ngày đúng giờ, đúng thứ tự, tự alert Slack khi lỗi.

Lý do cần: Mỗi ngày hàng chục bảng cần được cập nhật — xóa số liệu cũ, điền số liệu mới. Làm tay thì dễ quên, dễ chạy sai thứ tự, và không ai biết khi nào thì xong. Airflow giải quyết cả 3 vấn đề đó.

---

## Khái niệm 1: DAG là gì?

**DAG** *(Directed Acyclic Graph)* **= Kịch bản mô tả công việc cần làm theo đúng thứ tự, chạy tự động theo lịch.**

Lý do cần DAG: pipeline thường gồm nhiều bước phụ thuộc nhau (xóa trước, rồi mới điền; điền xong mới check). Nếu không có DAG, bước nào chạy trước bước nào phụ thuộc vào người nhớ — sai thứ tự là ra dữ liệu sai. DAG biến thứ tự đó thành code, Airflow tự đảm bảo luôn đúng thứ tự, đúng giờ, và retry đúng bước khi lỗi.

Ví dụ, để cập nhật bảng `daily_stock_balance` mỗi ngày, cần 2 bước:

```
Bước 1: Xóa dữ liệu cũ trong bảng
Bước 2: Điền dữ liệu mới vào bảng
```

DAG file là file Python mô tả:
- **Lịch chạy:** "Chạy lúc 7h30 sáng (0h30 UTC) mỗi ngày"
- **Danh sách bước:** Bước 1 xong rồi mới chạy Bước 2
- **Phản ứng khi lỗi:** Gửi thông báo Slack

DAG file **không chứa logic xử lý dữ liệu** — nó chỉ là "kịch bản" nói rằng "làm A rồi làm B".

---

## Khái niệm 2: Plugin là gì?

**Plugin = File chứa logic xử lý thực sự** — câu SQL, hàm tính toán, kết nối BigQuery.

Lý do cần tách riêng: DAG file chỉ nên nói "làm gì, thứ tự nào" — còn *làm thế nào* (câu SQL cụ thể, logic tính AUM) nên nằm ở chỗ khác để dễ đọc, dễ test, dễ sửa độc lập. Plugin là chỗ đó.

Nếu DAG là **kịch bản phim** (cảnh 1: xóa, cảnh 2: thêm mới), thì Plugin là **diễn viên** thực sự lên sân khấu và làm việc đó.

Ví dụ Plugin cho Bước 2:

```python
def insert_table(dataset_name, table_name):
    query = f"""
        INSERT INTO {dataset_name}.{table_name}
        SELECT user_id, date, balance
        FROM raw_mysql.portfolio_transaction
        WHERE date = current_date - 1
    """
    # Gửi câu query này lên BigQuery để chạy
    client.query(query).result()
```

---

## Tại sao tách thành 2 file riêng (DAG + Plugin)?

Vì **lịch chạy** và **logic xử lý** là hai việc khác nhau, thay đổi vì lý do khác nhau:

| | DAG file | Plugin file |
|---|---|---|
| **Chứa gì** | Lịch chạy, thứ tự bước, cài đặt retry | SQL, logic tính toán |
| **Thay đổi khi nào** | Muốn đổi giờ chạy, thêm/bớt bước | Muốn sửa logic tính, sửa câu SQL |
| **Ai thường sửa** | Người setup pipeline | Người viết data |

Nếu nhét tất cả vào một file, khi sửa SQL dễ vô tình làm hỏng lịch chạy, hoặc ngược lại.

---

## Quy trình setup pipeline

```
1. Test plugin local (VS Code / Colab) → chạy được
         ↓
2. Viết DAG file + Plugin file
         ↓
3. Push code lên GitHub (nhánh main)
         ↓ Airflow tự kéo về trong 60 giây
4. Đến giờ hẹn (vd: 0h30 UTC mỗi đêm):
   ┌─ Task 1: INSERT dữ liệu mới (atomic — dùng CREATE OR REPLACE TABLE)
   └─ Task 2: Data quality check (đếm rows > 0, nếu fail → raise)
         ↓
5a. Thành công → ghi log, chờ hôm sau
5b. Thất bại   → Variable.set error → gửi thông báo đỏ lên Slack
```

Airflow chạy trên Kubernetes (GKE), được cài bằng Helm. Không cần biết chi tiết Kubernetes — chỉ cần biết: **bạn push code lên GitHub, Airflow tự nhận, tự chạy đúng giờ.**

---

## Cấu trúc thư mục cần biết

```
airflow/
├── dags/               ← Bỏ DAG files vào đây (kịch bản + lịch chạy)
│   ├── trading/
│   ├── brokerage/
│   └── ...
└── plugins/            ← Bỏ Plugin files vào đây (SQL, logic)
    ├── libs/
    │   ├── bigquery_utils.py   ← Hàm xóa bảng (truncate), merge bảng
    │   └── slack_utils.py      ← Gửi alert Slack khi lỗi
    └── trading/
        └── bigquery_insert_...py  ← SQL của từng bảng
```

**Quy tắc đặt tên file:**

- DAG: `airflow/dags/{tên_domain}/reload_{tên_bảng}_table.py`
- Plugin: `airflow/plugins/{tên_domain}/bigquery_insert_{tên_bảng}_table.py`

---

## Các loại pipeline (pattern) trong repo

Repo có 4 loại pipeline khác nhau tùy nguồn dữ liệu:

### Loại A — BigQuery → BigQuery *(phổ biến nhất)*

Dữ liệu nguồn đã có sẵn trong BigQuery, chỉ cần tính toán lại và điền vào bảng đích.

```
[Xóa bảng cũ] → [Điền dữ liệu mới bằng SQL trong BigQuery]
```

*Ví dụ các bảng dùng Loại A:*

| DAG | Bảng đích |
|---|---|
| `schedule_bigquery_reload_raw_transaction_table` | `trading.raw_transaction` |
| `schedule_bigquery_reload_daily_stock_balance_table` | `trading.daily_stock_balance` |
| `schedule_bigquery_reload_trader_churn_status_table` | `trading.trader_churn_status` |
| `schedule_bigquery_reload_anfin_daily_aum_per_user_table` | `trading.anfin_daily_aum_per_user` |

### Loại B — MySQL → BigQuery *(full copy)*

Copy toàn bộ bảng từ database MySQL của app sang BigQuery. Chạy để refresh hoàn toàn.

```
[Lấy toàn bộ data từ MySQL] → [Lưu tạm vào Google Cloud Storage] → [Load vào BigQuery]
```

*Ví dụ: các bảng master data từ MySQL như `users`, `accounts` — bảng nhỏ, copy toàn bộ mỗi ngày cho đơn giản.*

### Loại C — MySQL → BigQuery *(chỉ lấy phần thay đổi)*

Giống Loại B nhưng thông minh hơn: chỉ lấy rows đã thay đổi trong 24h qua, rồi upsert (thêm mới hoặc cập nhật) vào BigQuery. Phù hợp với bảng có nhiều dữ liệu.

```
[Tính khung giờ 24h qua]
    → [Lấy rows mới/thay đổi từ MySQL]
    → [Lưu tạm vào GCS]
    → [Load vào bảng trung gian (landing)]
    → [Upsert vào bảng chính (MERGE)]
```

*Ví dụ: `portfolio_transaction`, `orders` — bảng lớn hàng triệu rows, chỉ sync phần mới thay đổi thay vì copy toàn bộ.*

### Loại D — Firestore → BigQuery

Dữ liệu từ Firestore được flatten sẵn vào dataset `flattened_firestore`, sau đó các pipeline đọc từ đó như Loại A bình thường.

*Ví dụ: `flattened_firestore.user_kyc`, `flattened_firestore.user_profile` — đọc từ đây rồi join vào các bảng trading.*

---

## Hướng dẫn step-by-step: Thêm một bảng daily update mới (Loại A)

### Bước 1 — Tạo bảng trong BigQuery trước

Vào BigQuery console, tạo bảng đích với đúng schema. Bảng **phải tồn tại trước** khi chạy DAG, vì code không tự tạo bảng mới.

### Bước 2 — Tạo file Plugin (chứa SQL)

Tạo file: `airflow/plugins/{domain}/bigquery_insert_{tên_bảng}_table.py`

```python
import google.auth
from google.cloud import bigquery

def get_bq_client():
    creds, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=creds, project=project)

def insert_table(dataset_name: str, table_name: str):
    client = get_bq_client()  # khởi tạo trong function, không phải module level
    query = f"""
        INSERT INTO {dataset_name}.{table_name}

        -- Viết câu SELECT ở đây
        SELECT
            user_id,
            transaction_date,
            SUM(amount) AS total_amount
        FROM raw_mysql.orders
        WHERE transaction_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2
    """
    client.query(query).result()
```

### Bước 3 — Tạo file DAG (lịch chạy + thứ tự)

Tạo file: `airflow/dags/{domain}/reload_{tên_bảng}_table.py`

Copy nguyên template này, chỉ cần đổi 3 chỗ có comment `← ĐỔI`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ← ĐỔI: import đúng tên file và hàm ở Bước 2
from trading.bigquery_insert_daily_orders_table import insert_table
from libs.bigquery_utils import truncate_table
from libs.slack_utils import task_fail_slack_alert

DATASET = "trading"            # ← ĐỔI: tên dataset trong BigQuery
TABLE   = "daily_orders"       # ← ĐỔI: tên bảng trong BigQuery

default_args = {
    "start_date": datetime(2022, 6, 1),   # không cần đổi
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,  # tự động báo Slack khi lỗi
}

with DAG(
    dag_id=f"schedule_bigquery_{TABLE}_table",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=16,
    schedule_interval="45 00 * * *",   # 7h45 sáng giờ Việt Nam (0h45 UTC)
    default_args=default_args,
    tags=["bigquery", "reload", TABLE, "schedule"],
) as dag:

    # Bước 1: Xóa dữ liệu cũ
    truncate_task = PythonOperator(
        task_id="truncate_bigquery_table",
        python_callable=truncate_table,
        op_kwargs={"dataset_name": DATASET, "table_name": TABLE},
    )

    # Bước 2: Điền dữ liệu mới
    insert_task = PythonOperator(
        task_id="insert_into_bigquery_table",
        python_callable=insert_table,
        op_kwargs={"dataset_name": DATASET, "table_name": TABLE},
    )

    # Thứ tự: xóa trước, rồi mới điền
    truncate_task >> insert_task
```

### Bước 4 — Push lên GitHub

```bash
git pull origin main
git add airflow/dags/trading/reload_daily_orders_table.py
git add airflow/plugins/trading/bigquery_insert_daily_orders_table.py
git commit -m "Add daily reload DAG for daily_orders"
git push origin main
```

**Sau tối đa 60 giây**, DAG tự xuất hiện trên Airflow UI. Vào UI trigger thủ công lần đầu để test, nếu chạy xanh thì xong.

---

## Backfill — Chạy bù dữ liệu lịch sử

Tất cả DAG trong repo đang set `catchup=False`. Nghĩa là nếu DAG bị tắt 3 ngày, khi bật lại Airflow **sẽ bỏ qua 3 ngày đó** — không tự chạy bù.

Đây là lựa chọn đúng cho daily reload — không cần tự động chạy bù. Lưu ý: `airflow dags backfill` CLI vẫn hoạt động bình thường, `execution_date` được set đúng ngày lịch sử, không phải ngày hôm nay.

### Khi nào cần backfill?

- Thêm bảng mới, cần dữ liệu từ 3 tháng trước
- DAG bị lỗi nhiều ngày, cần chạy lại từ ngày X
- Sửa logic SQL, cần tính lại toàn bộ lịch sử

### Cách backfill thủ công

**Cách 1 — CLI (chạy từ trong pod scheduler):**

```bash
# Vào pod trước
kubectl exec -it airflow-scheduler-0 -n <namespace> -- bash

# Rồi chạy backfill
airflow dags backfill \
  --start-date 2026-08-01 \
  --end-date 2026-08-31 \
  schedule_bigquery_reload_daily_stock_balance_table
```

**Cách 2 — UI (đơn giản hơn cho 1–2 ngày):**

Airflow UI → chọn DAG → Grid view → click vào run của ngày cần chạy lại → **Clear** → DAG sẽ chạy lại ngày đó.

**Lưu ý:** Backfill chỉ có ý nghĩa nếu pipeline là idempotent (nguyên tắc 1) — chạy lại ngày nào cũng ra đúng dữ liệu của ngày đó.

---

## Cài đặt giờ chạy (cron)

Airflow dùng giờ UTC (Việt Nam = UTC+7), nên cần trừ 7 giờ:

| Muốn chạy lúc (Việt Nam) | Ghi vào schedule_interval |
|---|---|
| 7h00 sáng | `00 00 * * *` |
| 7h30 sáng | `30 00 * * *` |
| 8h00 sáng | `00 01 * * *` |
| 12h00 trưa | `00 05 * * *` |
| 17h00 chiều | `00 10 * * *` |

Cú pháp: `{phút} {giờ_UTC} * * *`

---

## Khi có lỗi — Slack sẽ báo gì?

Mọi DAG đều có `on_failure_callback: task_fail_slack_alert`. Khi task nào đó fail, Slack tự nhận được thông báo màu đỏ. Tên task trong thông báo nói lên ngay vấn đề ở đâu:

**Lỗi kỹ thuật** (SQL sai, quyền, import thiếu):
```
🔴 Task Failed
DAG:  schedule_bigquery_reload_daily_stock_balance_table
Task: insert_into_bigquery_table
Time: 2026-09-09 00:47:03 UTC
Log:  http://airflow.internal/log?dag_id=...&task_id=insert_into_bigquery_table
```

**Lỗi data quality** (INSERT xong nhưng data bất thường):
```
🔴 Task Failed
DAG:  schedule_bigquery_reload_daily_stock_balance_table
Task: check_data_quality
Time: 2026-09-09 00:48:21 UTC
Log:  http://airflow.internal/log?dag_id=...&task_id=check_data_quality

ValueError: daily_stock_balance: chỉ có 0 rows hôm nay
```

- `insert_into_bigquery_table` fail → lỗi kỹ thuật, xem log Python/SQL
- `check_data_quality` fail → INSERT chạy được nhưng data có vấn đề, xem BigQuery

---

## Lỗi thường gặp khi viết DAG

### Variable.get() ở module level — DAG chết không báo rõ lý do

Đây là lỗi phổ biến nhất. Airflow parse lại tất cả DAG file mỗi 30 giây. Nếu `Variable.get()` nằm ngoài function, nó chạy mỗi 30 giây — token hết hạn, connection bị lỗi → DAG tự chết, thường chỉ thấy task duration 1–2 giây rồi fail mà không rõ tại sao.

```python
# SAI — chạy mỗi 30s khi Airflow parse DAG
token = Variable.get("fb_ads_token")

def my_task():
    call_api(token)

# ĐÚNG — chỉ chạy khi task thật sự execute
def my_task():
    token = Variable.get("fb_ads_token")
    call_api(token)
```

### Credential BigQuery — không hardcode project ID

```python
# SAI — hardcode → 403 Permission Denied khi deploy
client = bigquery.Client(project="anfin-prod-123")

# ĐÚNG — lấy project từ environment, không cần hardcode
import google.auth

creds, project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
client = bigquery.Client(credentials=creds, project=project)
```

### Error capture bằng Variable.set — workaround khi log không đọc được

Khi gặp "Log file does not exist" (như DAG `anfin_daily_aum_per_user` hiện tại), thêm try/except + `Variable.set` để capture lỗi vào Airflow Variables — không cần log, không cần kubectl:

```python
def run_task():
    try:
        insert_data()
    except Exception as e:
        Variable.set("error_insert_aum_table", f"{type(e).__name__}: {e}")
        raise  # vẫn raise để DAG báo fail
```

Đọc lỗi tại: Airflow UI → **Admin → Variables** → tìm key `error_insert_aum_table`.

---

## Xem log lỗi ở đâu?

**DAG báo lỗi mà không biết lỗi gì** — đây là vấn đề phổ biến khi mới dùng Airflow. Log lỗi nằm ngay trong Airflow UI, không phải ở chỗ nào khác.

### Cách xem log nhanh nhất

```
Airflow UI → tìm DAG bị lỗi → click vào run màu đỏ
→ click vào task đỏ → tab "Log" ở góc trên bên phải
→ scroll xuống cuối cùng → tìm dòng "ERROR" hoặc "Traceback"
```

Nếu không biết Airflow UI ở đâu, hỏi team xem webserver đang chạy ở URL nào — port mặc định là `8080`.

### Các lỗi thường gặp và ý nghĩa

| Dòng lỗi trong log | Nghĩa là gì |
|---|---|
| `ModuleNotFoundError: No module named 'xxx'` | Import thiếu, file plugin không tồn tại hoặc đường dẫn sai |
| `google.api_core.exceptions.NotFound: 404` | Tên dataset hoặc tên bảng trong BigQuery không đúng |
| `google.api_core.exceptions.Forbidden: 403` | Service account không có quyền truy cập bảng/dataset đó |
| `SyntaxError` hoặc `IndentationError` | Lỗi cú pháp Python trong DAG file hoặc plugin file |
| `KeyError: 'xxx'` | Thiếu key trong dict, thường do query BigQuery trả về cột khác tên mong đợi |
| `ValueError: ...` | Thường do check thủ công raise lỗi (như check rows = 0) |

### Trường hợp cụ thể: `schedule_bigquery_reload_anfin_daily_aum_per_user_table`

DAG này đang fail và log hiển thị:

```
*** Log file does not exist: /opt/airflow/logs/...
*** Failed to fetch log file from worker. [Errno 2] Name or service not known
```

**Đây không phải lỗi của DAG** — log bị mất vì scheduler pod đã restart. Airflow cố fetch log từ pod `airflow-scheduler-0` nhưng không kết nối được.

**Cách xem log ngay:**

```bash
# Vào thẳng pod scheduler
kubectl exec -it airflow-scheduler-0 -n <namespace> -- bash

# Xem log của DAG
ls /opt/airflow/logs/dag_id=schedule_bigquery_reload_anfin_daily_aum_per_user_table/
```

Hoặc: trigger lại DAG thủ công → vào tab Log ngay khi task đang chạy để xem real-time.

**Vấn đề gốc rễ: Log lưu trên local filesystem của scheduler pod.** Pod restart là mất hết. Fix đúng là cấu hình Remote Logging ra GCS:

```yaml
# helm-airflow-prod.yaml
config:
  logging:
    remote_logging: 'True'
    remote_base_log_folder: 'gs://your-bucket/airflow-logs'
    remote_log_conn_id: 'google_cloud_default'
```

Sau đó mọi log được upload lên GCS sau khi task xong — không bao giờ mất dù pod restart.

---

## Tóm tắt trong 1 câu

> **Viết SQL vào Plugin file, viết lịch chạy vào DAG file, push lên GitHub, Airflow tự làm phần còn lại.**

---

## Nguyên tắc nền khi setup Airflow pipeline

Trước khi setup bất kỳ pipeline nào, cần nắm 5 nguyên tắc này. Vi phạm bất kỳ cái nào đều sẽ gặp vấn đề sớm hay muộn.

---

**1. Idempotent — chạy lại bao nhiêu lần cũng ra kết quả giống nhau**

Pipeline bị lỗi giữa chừng rồi retry là vấn đề thường gặp. Câu hỏi là: sau khi retry, dữ liệu trong bảng có đúng không?

*Nếu vi phạm:* Giả sử INSERT không có TRUNCATE trước. Lần đầu chạy xong, bảng có 1000 rows. Lần sau retry, INSERT thêm 1000 rows nữa → bảng có 2000 rows, số liệu nhân đôi. Dashboard sai mà không ai biết.

*Cách đảm bảo:* TRUNCATE trước khi INSERT — xóa hết rồi viết lại từ đầu. Chạy lần 1 hay lần 10 thì kết quả như nhau.

> ⚠️ **TRUNCATE đảm bảo idempotent nhưng tạo ra rủi ro mới:** nếu TRUNCATE xong mà INSERT fail → bảng rỗng, dashboard thấy số 0. Đây là lý do cần thêm nguyên tắc 2 (Atomic) — không dùng một mình TRUNCATE+INSERT mà không có temp table.

*Repo này:* Tất cả Loại A đang dùng TRUNCATE + INSERT — idempotent đúng, nhưng nếu INSERT fail thì bảng sẽ rỗng. Cần đọc nguyên tắc 2 để fix.

---

**2. Atomic — hoặc thành công hoàn toàn, hoặc không có gì thay đổi**

Không được có trạng thái "pipeline chạy một nửa". Nếu bảng đang phục vụ dashboard và pipeline fail giữa chừng, bảng phải vẫn có dữ liệu cũ — không phải bảng rỗng.

*Nếu vi phạm:* TRUNCATE xong → bảng rỗng → INSERT bắt đầu → INSERT fail → bảng vẫn rỗng. Airflow retry sau 5 phút nhưng trong 5 phút đó dashboard trả về số 0. Ai đang xem báo cáo lúc đó thấy dữ liệu bị mất.

*Cách đảm bảo:* Có 3 cách, tùy quy mô bảng:

**Cách 1 — `CREATE OR REPLACE TABLE AS SELECT` (đơn giản nhất, atomic sẵn)**
```sql
-- Query fail → bảng cũ giữ nguyên. Query xong → bảng mới swap vào.
CREATE OR REPLACE TABLE dataset.table_name AS
SELECT ... FROM ...;
```
Phù hợp bảng nhỏ/vừa, không cần quản lý temp. Đây là cách phổ biến nhất ở production.

**Cách 2 — Partition overwrite (bảng lớn có partition)**
```sql
-- Chỉ replace đúng partition ngày hôm qua, không đụng data cũ
INSERT INTO dataset.table OVERWRITE PARTITIONS (date = '2026-09-09')
SELECT ... WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
```
Rẻ hơn nhiều vì chỉ scan đúng partition cần replace. Chuẩn dùng ở các team lớn (dbt BigQuery dùng chiến lược này).

**Cách 3 — Temp table → RENAME (khi cần validate trước khi swap)**
```sql
CREATE OR REPLACE TABLE dataset.table_temp AS SELECT ...;
-- validate temp ở đây nếu cần
ALTER TABLE dataset.table_temp RENAME TO table_name;
```

*Repo này:* Tất cả DAG hiện tại đang vi phạm nguyên tắc này — đang dùng TRUNCATE + INSERT không atomic.

---

**3. Tách biệt lịch chạy và logic xử lý**

Lịch chạy (chạy lúc mấy giờ, retry mấy lần) và logic xử lý (câu SQL, cách tính toán) là hai thứ thay đổi vì lý do hoàn toàn khác nhau. Nếu để chung một file, sửa SQL dễ vô tình làm hỏng lịch chạy và ngược lại.

*Lý do thực tế:* Người viết SQL không cần biết Airflow. Người setup lịch chạy không cần biết logic tính toán. Tách ra thì hai người làm độc lập được, review cũng dễ hơn.

*Repo này:* Đã tách đúng — DAG file và Plugin file riêng. Nhưng SQL đang nhúng trong Python string (xem revamp), khiến SQL không test riêng được.

---

**4. Observable — hệ thống phải tự báo khi có vấn đề**

Pipeline chạy xanh không có nghĩa dữ liệu đúng. Cần phân biệt 2 loại lỗi:
- **Lỗi kỹ thuật:** Python crash, SQL syntax sai → Airflow tự phát hiện, Slack báo đỏ
- **Lỗi dữ liệu:** 0 rows, null hàng loạt, số giảm bất thường → Airflow không biết, không báo gì

*Nếu vi phạm:* Câu WHERE viết sai, bảng insert 0 rows. DAG vẫn xanh. Slack không báo. Sáng hôm sau mở dashboard thấy số 0, mất nửa ngày tìm nguyên nhân.

*Cách đảm bảo:* Thêm task check sau INSERT, raise lỗi nếu bất thường:
- `COUNT(*) = 0` → raise lỗi
- `COUNTIF(user_id IS NULL) > 0` → raise lỗi
- Rows hôm nay < 50% hôm qua → raise lỗi

*Repo này:* Chưa có lớp kiểm tra nào — đây là điểm thiếu quan trọng nhất.

---

**5. Dependency rõ ràng — bảng nào phụ thuộc bảng nào phải được khai báo**

Nhiều bảng dùng kết quả của bảng khác. Nếu không khai báo dependency, tất cả chạy theo giờ cố định và hy vọng bảng nguồn đã xong trước bảng đích.

*Nếu vi phạm:* Bảng A chạy 7h và thường xong lúc 7h15. Bảng B dùng dữ liệu từ A, được cài chạy lúc 7h10. Hôm A chậm vì query nặng hơn, B chạy lúc 7h10 nhưng A chưa xong → B lấy dữ liệu thiếu, kết quả sai.

*Cách đảm bảo:* Dùng `TriggerDagRunOperator` để B chỉ chạy sau khi A thành công. Hoặc dùng Airflow Dataset (tính năng mới hơn).

*Repo này:* Tất cả DAG chạy theo giờ cố định, không có khai báo dependency nào.

---

**6. SLA / Data freshness — data phải sẵn sàng trước giờ X**

Biết pipeline fail là một chuyện. Nhưng pipeline chạy xong muộn hơn cam kết cũng là vấn đề — và Airflow không tự báo khi điều này xảy ra.

*Nếu vi phạm:* DAG chạy lúc 7h, team mở dashboard lúc 8h và thấy data thiếu. Airflow không fail, Slack không báo gì. Hoặc: pipeline đột nhiên chạy nặng hơn, đến 9h mới xong trong khi báo cáo sáng đã được gửi đi với data cũ.

*Cách đảm bảo:* Khai báo SLA trong DAG:

```python
with DAG(
    dag_id="...",
    sla_miss_callback=sla_miss_alert,   # callback khi miss SLA
    default_args={
        "sla": timedelta(hours=2),       # task phải xong trong 2h kể từ scheduled_time
    }
)
```

*Repo này:* Chưa có khai báo SLA nào.

---

**7. Lineage — bảng nào từ đâu ra, ai own**

Khi có vấn đề data, câu hỏi đầu tiên là: bảng này được tính từ bảng nào? Bảng nguồn đó lại từ đâu? Không có lineage thì tìm root cause mất rất nhiều thời gian.

*Nếu vi phạm:* `anfin_daily_aum_per_user` bị sai số. Team mất 2 tiếng mới trace ra là `raw_transaction` bị lỗi từ hôm qua, mà `raw_transaction` lại lấy từ MySQL pipeline cũng đang có vấn đề.

*Cách đảm bảo:* Tối thiểu: mô tả rõ nguồn dữ liệu trong DAG description và tags. Nâng cao: dùng dbt docs, OpenLineage, hoặc DataHub để tự động track lineage qua các bảng.

*Repo này:* Tags đã có nhưng không đủ để trace. Không có lineage graph.

---

## Bảng DAG hiện tại cần sửa gì

Dưới đây là danh sách các vấn đề và DAG nào bị ảnh hưởng:

| Vấn đề | Ảnh hưởng đến | Cần sửa gì |
|---|---|---|
| Không có data check sau INSERT | **Tất cả DAG** | Thêm task `check_data_quality` sau insert_task |
| TRUNCATE+INSERT không atomic | **Tất cả DAG Loại A** | Đổi sang CREATE OR REPLACE TABLE temp → RENAME |
| SQL nhúng trong Python | **Tất cả plugin file** | Tách ra file `.sql` riêng |
| Tên DAG không nhất quán | `insert_bigquery_*`, `reload_*` | Đổi về 1 convention thống nhất |
| Không có dependency giữa DAG | DAG dùng bảng của DAG khác | Thêm TriggerDagRunOperator |
| Password admin mặc định | Airflow webserver | Đổi ngay trong Helm config |
| LocalExecutor + PostgreSQL built-in | Toàn bộ hệ thống | Migrate sang Cloud SQL + CeleryExecutor (dài hạn) |

### DAG đang có vấn đề cụ thể cần check

| DAG | Vấn đề được biết |
|---|---|
| `schedule_bigquery_reload_anfin_daily_aum_per_user_table` | Đang fail, chưa rõ nguyên nhân — xem phần log lỗi phía trên |

---

## Setup hiện tại cần revamp gì

Nhìn vào repo hiện tại, so với 5 nguyên tắc trên:

---

### Vi phạm nguyên tắc 2 — TRUNCATE + INSERT không atomic

**Vấn đề:** Nếu TRUNCATE xong mà INSERT fail, bảng rỗng. Dashboard thấy số 0. Retry sẽ fix nhưng trong thời gian đó data đã sai.

**Fix — chọn 1 trong 3 cách:**

```sql
-- Cách 1: CREATE OR REPLACE TABLE (đơn giản nhất, atomic sẵn)
-- Query fail → bảng cũ giữ nguyên
CREATE OR REPLACE TABLE dataset.table_name AS
SELECT ... FROM ...;

-- Cách 2: Partition overwrite (bảng lớn, chỉ replace partition hôm qua)
INSERT INTO dataset.table OVERWRITE PARTITIONS (date = '2026-09-09')
SELECT ... WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- Cách 3: Temp table → RENAME (khi cần validate trước khi swap)
CREATE OR REPLACE TABLE dataset.table_name_temp AS SELECT ...;
ALTER TABLE dataset.table_name_temp RENAME TO table_name;
```

---

### Vi phạm nguyên tắc 4 — Không có data check sau khi chạy

**Vấn đề:** DAG xanh chỉ nghĩa là không có lỗi Python/SQL. Không có nghĩa data đúng. 0 rows, null toàn bộ — vẫn xanh.

**Fix:** Thêm task check sau INSERT:

```python
def check_table(dataset_name, table_name, min_rows=100):
    count = list(client.query(
        f"SELECT COUNT(*) as cnt FROM {dataset_name}.{table_name}"
    ).result())[0].cnt
    if count < min_rows:
        raise ValueError(f"{table_name}: chỉ có {count} rows")
```

Flow mới: `truncate >> insert >> check`

---

### Vi phạm nguyên tắc 5 — Dependency giữa DAG không được khai báo

**Vấn đề:** Nhiều bảng dùng kết quả của bảng khác, nhưng tất cả chỉ chạy theo giờ cố định. Nếu bảng nguồn fail hoặc chậm, bảng downstream vẫn chạy và lấy dữ liệu thiếu.

**Fix:** Dùng `TriggerDagRunOperator` hoặc Airflow Dataset để downstream DAG chờ upstream xong mới chạy.

---

### Vấn đề vận hành — Tên DAG không nhất quán

**Vấn đề:** Cùng 1 repo có 3 convention đặt tên:
- `schedule_bigquery_{table}_table`
- `insert_bigquery_{table}_table`
- `reload_{table}_table`

Khi có 50+ DAG trên UI, không nhìn tên mà biết được domain nào, chạy khi nào, loại gì.

**Fix:** Chọn 1 convention và áp toàn bộ, ví dụ:
```
{domain}__{table}__{frequency}
trading__raw_transaction__daily
datamart__daily_active_funded_account__daily
```

---

### Vấn đề maintainability — SQL nhúng trong Python

**Vấn đề:** SQL nằm trong Python string, không test riêng được, không lint được, review khó.

**Fix:** Tách SQL ra file `.sql` riêng trong `plugins/{domain}/sql/`:

```python
sql_path = Path(__file__).parent / "sql" / f"{table_name}.sql"
query = sql_path.read_text().format(dataset=dataset_name)
client.query(query).result()
```

---

### Vấn đề infrastructure — LocalExecutor và PostgreSQL built-in

Hai cái này ổn ở quy mô hiện tại nhưng có điểm yếu dài hạn:

- **LocalExecutor** — mọi task chạy trên 1 pod scheduler. Scheduler đang được cấp 10GB RAM, đó là dấu hiệu đang bị đẩy giới hạn. Khi số DAG tăng thêm, nên chuyển sang `CeleryExecutor` hoặc `KubernetesExecutor`.
- **PostgreSQL built-in** — Helm chart tự ghi là "not recommended for production". Pod chết là mất hết lịch sử Airflow. Nên chuyển sang Cloud SQL (GCP managed) có backup tự động.

---

### Partitioned table — TRUNCATE+INSERT toàn bảng rất tốn kém

**Vấn đề:** TRUNCATE + INSERT trên bảng không partition = BigQuery scan toàn bộ bảng mỗi ngày. Bảng càng lớn, tiền càng nhiều, query càng chậm.

**Fix:** Dùng partitioned table theo ngày, chỉ replace đúng partition hôm qua:

```sql
-- Khai báo khi tạo bảng
CREATE TABLE dataset.table
PARTITION BY date

-- Insert hàng ngày — chỉ ghi vào partition ngày hôm qua
INSERT INTO dataset.table
SELECT ...
WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
```

Không cần TRUNCATE nữa — BigQuery tự ghi đè partition của ngày đó. Idempotent, nhanh hơn, rẻ hơn.

---

### Schema evolution — khi source thêm/bớt cột

**Vấn đề:** Developer backend thêm cột mới vào MySQL mà không báo. Pipeline Loại B/C copy sang BQ có thể: (a) fail rõ ràng nếu schema BQ strict, hoặc (b) chạy xanh nhưng bỏ sót cột mới — không ai biết.

**Fix:** Thêm bước kiểm tra schema trước khi load, hoặc dùng `SCHEMA_UPDATE_OPTIONS` trong BQ để tự động thêm cột mới:

```python
job_config = bigquery.LoadJobConfig(
    schema_update_options=[
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    ]
)
```

---

### Cost monitoring — query scan tốn tiền không ai biết

**Vấn đề:** BigQuery tính tiền theo lượng data scanned. Một câu SQL viết sai (thiếu WHERE, không dùng partition) có thể scan TB dữ liệu. Không có alert thì không ai biết cho đến khi thấy hóa đơn.

**Fix:** Bật BigQuery budget alert trên GCP console + set maximum bytes billed per query:

```python
job_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=10 * 1024**3  # tối đa 10GB per query
)
```

---

### Connection management — service account key lưu ở đâu?

**Vấn đề:** Airflow cần credentials để kết nối BigQuery, MySQL, GCS. Credentials này đang được quản lý thế nào? SSH key cho git-sync đang lưu trong K8s secret `airflow-gke-git-secret` — key này có được rotate định kỳ không?

**Fix:** Dùng Workload Identity (GKE) thay vì service account key file — không cần lưu key ở đâu cả, GKE tự authenticate với GCP.

---

### Bảo mật — Password mặc định

Helm config hiện có `username: admin / password: admin`. Nếu webserver đang expose qua LoadBalancer mà chưa đổi — cần làm ngay.

---

### Tóm tắt ưu tiên

| | Vấn đề | Nguyên tắc vi phạm |
|---|---|---|
| 🔴 Ngay | Đổi password admin | Bảo mật |
| 🔴 Ngay | Thêm data check sau INSERT | Observable |
| 🟡 Sớm | Đổi sang CREATE OR REPLACE TABLE hoặc partition overwrite | Atomic |
| 🟡 Sớm | Thống nhất tên DAG | Vận hành |
| 🟠 Khi có time | Tách SQL ra file `.sql` | Maintainability |
| 🟠 Khi có time | Khai báo dependency giữa DAG | Dependency rõ ràng |
| 🔴 Ngay | Setup Remote Logging ra GCS | Log bị mất khi pod restart |
| 🟡 Sớm | Dùng partitioned table trong BQ | Cost + hiệu năng |
| 🟡 Sớm | Khai báo SLA trong DAG | Data freshness |
| 🟠 Khi có time | Xử lý schema evolution | Khi MySQL thêm/bớt cột |
| 🟠 Khi có time | Cost monitoring BQ | Query scan tốn tiền không ai biết |
| 🟠 Khi có time | Lineage documentation | Trace root cause khi data sai |
| ⚪ Dài hạn | Cloud SQL + CeleryExecutor | Scale |

---

## Data Quality là gì — và cần làm gì trong pipeline?

**Data Quality = Lớp kiểm tra tự động để phát hiện data sai trước khi lên dashboard.**

Lý do cần: Airflow chỉ biết Python có crash hay không — không biết bảng có 0 rows, `user_id` bị null toàn bộ, hay số liệu giảm 90% so với hôm qua. Không có data quality check thì pipeline xanh nhưng dashboard sai, và thường mất nửa ngày mới phát hiện ra.

Pipeline chạy xanh không có nghĩa dữ liệu đúng. Có thể:
- Bảng có 0 rows vì câu WHERE quá chặt
- Cột `user_id` bị null toàn bộ vì join sai
- Số liệu hôm nay thấp bất thường vì source MySQL có vấn đề

Airflow vẫn xanh, Slack không báo gì, nhưng dashboard đang sai.

### Các lớp kiểm tra Data Quality

**Lớp 1 — Kiểm tra cơ bản (nên có ngay)**

Thêm task sau INSERT, raise lỗi nếu bất thường:

```python
def check_data_quality(dataset_name, table_name):
    query = f"""
        SELECT
            COUNT(*)                              AS total_rows,
            COUNTIF(user_id IS NULL)              AS null_user_id,
            COUNTIF(date < '2020-01-01')          AS invalid_date
        FROM {dataset_name}.{table_name}
        WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    row = list(client.query(query).result())[0]

    if row.total_rows == 0:
        raise ValueError("Bảng không có dữ liệu hôm nay")
    if row.null_user_id > 0:
        raise ValueError(f"Có {row.null_user_id} rows bị null user_id")
    if row.invalid_date > 0:
        raise ValueError(f"Có {row.invalid_date} rows date không hợp lệ")
```

Flow mới: `truncate >> insert >> check_data_quality`

**Lớp 2 — Kiểm tra so sánh với hôm qua (nâng cao)**

Phát hiện số liệu bất thường bằng cách so sánh với ngày hôm trước:

```sql
SELECT
    today.total_rows,
    yesterday.total_rows AS yesterday_rows,
    SAFE_DIVIDE(today.total_rows, yesterday.total_rows) AS ratio
FROM
    (SELECT COUNT(*) AS total_rows FROM dataset.table WHERE date = CURRENT_DATE - 1) today,
    (SELECT COUNT(*) AS total_rows FROM dataset.table WHERE date = CURRENT_DATE - 2) yesterday
```

Nếu `ratio < 0.5` (hôm nay chỉ bằng 50% hôm qua) → raise lỗi.

**Lớp 3 — dbt tests (dài hạn)**

Nếu có dùng dbt, nó có sẵn framework test: `not_null`, `unique`, `accepted_values`, `relationships`. Đây là cách chuyên nghiệp nhất để quản lý data quality ở quy mô lớn.

### Repo hiện tại

Hiện tại không có lớp kiểm tra nào sau INSERT — đây là điểm cần bổ sung sớm nhất trong revamp.

---

## Daily Monitoring DAG — Tóm tắt pipeline qua Slack mỗi sáng

Khi chạy 100 bảng mỗi ngày, cần một DAG riêng chạy sau tất cả và gửi 1 tin Slack tổng hợp: bao nhiêu bảng thành công, bao nhiêu fail, và lỗi cụ thể là gì.

### Cách hoạt động

**Bước 1 — Mỗi DAG business capture lỗi vào Variable**

Dùng 2 key Variable riêng để monitoring DAG phân biệt được lỗi kỹ thuật vs data quality:

```python
def run_insert(dataset_name, table_name):
    try:
        # ... logic insert ...
        Variable.delete(f"error__{table_name}")  # xóa lỗi cũ nếu success
    except Exception as e:
        Variable.set(f"error__{table_name}", f"{type(e).__name__}: {e}")
        raise

def check_data_quality(dataset_name, table_name):
    try:
        # ... logic check rows ...
        Variable.delete(f"dq__{table_name}")
    except ValueError as e:
        Variable.set(f"dq__{table_name}", str(e))  # key riêng cho DQ
        raise
```

**Bước 2 — Monitoring DAG chạy sau cùng**

```python
# dags/monitoring/daily_pipeline_summary.py
from datetime import datetime
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun, DagModel
import requests

def send_daily_summary():
    session = settings.Session()
    today = datetime.utcnow().date()

    # Tự động lấy danh sách DAG có tag "schedule" — không cần hardcode
    dag_ids = [
        d.dag_id for d in session.query(DagModel)
        .filter(DagModel.tags.any(name="schedule"))
        .filter(DagModel.is_active == True)
        .all()
    ]

    today_dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id.in_(dag_ids),
        DagRun.execution_date >= today_dt  # dùng datetime, không phải date
    ).all()
    run_map = {run.dag_id: run for run in dag_runs}

    failed, success_count, dq_count = [], 0, 0

    for dag_id in dag_ids:
        run = run_map.get(dag_id)
        if not run:
            continue
        table = dag_id.replace("schedule_bigquery_reload_", "").replace("_table", "")

        if run.state == "failed":
            # Dùng 2 key riêng: dq__table cho data quality, error__table cho lỗi kỹ thuật
            dq_error = Variable.get(f"dq__{table}", default_var="")
            tech_error = Variable.get(f"error__{table}", default_var="")
            if dq_error:
                dq_count += 1
            elif tech_error:
                failed.append(table)
        elif run.state == "success":
            success_count += 1

    total = len(dag_ids)
    fail_count = len(failed)
    success_pct = round(success_count / total * 100) if total > 0 else 0
    fail_pct = 100 - success_pct
    color = "#2EB67D" if fail_count == 0 and dq_count == 0 else "#E53935"

    requests.post(Variable.get("slack_webhook_monitoring"), json={
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"📊 Pipeline Health Report · {today}"}
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{total} tables  ·  {success_pct}% success  ·  {fail_pct}% fail*\n<http://airflow.internal|→ View in Airflow>"
                    }
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"```✅  Thành công              {success_count}\n❌  Lỗi                      {fail_count}\n⚠️  Dữ liệu bất thường       {dq_count}```"
                    }
                }
            ]
        }]
    })
    session.close()

with DAG(
    dag_id="monitoring_daily_pipeline_summary",
    schedule_interval="30 2 * * *",  # 9h30 sáng VN — sau khi tất cả DAG 7h-8h đã xong
    catchup=False,
    default_args={"start_date": datetime(2026, 1, 1)},
    tags=["monitoring", "slack"],
) as dag:
    PythonOperator(task_id="send_slack_summary", python_callable=send_daily_summary)
```

### Kết quả Slack mỗi sáng

```
📊 Pipeline Health Report · 2026-09-09     ← viền đỏ nếu có lỗi, xanh nếu all good
100 tables  ·  96% success  ·  4% fail
→ View in Airflow

✅  Thành công              96
❌  Lỗi                      3
⚠️  Dữ liệu bất thường       1
```

### Lưu ý khi setup

- Schedule lúc `30 2 * * *` (UTC) = 9h30 sáng VN — để sau khi toàn bộ DAG business đã xong
- DAG tự discover bằng tag `schedule` — thêm bảng mới chỉ cần gắn tag đúng, không cần sửa monitoring DAG
- Webhook URL lưu trong Airflow Variable `slack_webhook_monitoring`, không hardcode vào code

---

## Môi trường Dev và Prod — tại sao cần và cách setup

**Dev/Prod = Hai bản chạy riêng biệt của cùng một pipeline, trỏ vào dataset khác nhau.**

Lý do cần tách: Hiện tại mọi thay đổi đang test thẳng trên production. Viết sai SQL là bảng production bị ảnh hưởng ngay — không có chỗ thử an toàn. Với Dev environment, developer test thoải mái trên dataset `trading_dev` trước, chỉ merge vào `main` khi đã ổn.

Cần tách thành 2 môi trường:
- **Dev** — để test, thử nghiệm, không ảnh hưởng ai
- **Prod** — chỉ chạy code đã được kiểm tra kỹ

### Cách hoạt động trong Airflow

Repo hiện tại đã có sẵn một phần nền tảng: GCS bucket đã dùng `raw-mysql-{env}`, env lấy từ Airflow Variable `Variable.get("env")`. Cần mở rộng pattern này ra toàn bộ.

**Cấu trúc đề xuất:**

| | Dev | Prod |
|---|---|---|
| Git branch | `develop` | `main` |
| Airflow instance | Riêng (hoặc namespace riêng) | Instance hiện tại |
| BigQuery dataset | `trading_dev`, `datamart_dev` | `trading`, `datamart` |
| GCS bucket | `raw-mysql-dev` | `raw-mysql-prod` |
| Airflow Variable `env` | `dev` | `prod` |
| Schedule | Tắt hoặc chạy thưa | Chạy theo lịch thật |

### Cách implement

**Bước 1 — Dùng Variable để phân biệt dataset**

Sửa plugin files để dataset tự động theo env:

```python
from airflow.models import Variable

ENV = Variable.get("env", default_var="dev")  # mặc định dev cho an toàn

def get_dataset(base_name: str) -> str:
    if ENV == "prod":
        return base_name
    return f"{base_name}_{ENV}"   # → "trading_dev"
```

**Bước 2 — Tạo BigQuery datasets cho dev**

Tạo thêm các datasets `trading_dev`, `datamart_dev`, `brokerage_dev`… với schema giống prod nhưng chứa dữ liệu test (có thể là subset của prod).

**Bước 3 — Git branch strategy**

```
develop  →  Airflow dev  →  BQ datasets *_dev
main     →  Airflow prod →  BQ datasets prod
```

Mọi thay đổi đều đi qua `develop` trước. Chạy ổn trên dev rồi mới merge vào `main`.

**Bước 4 — Helm: deploy 2 instance hoặc dùng namespace riêng**

Cách đơn giản nhất: deploy thêm 1 Helm release cho dev với git-sync trỏ vào branch `develop`:

```yaml
# helm-airflow-dev.yaml
dags:
  gitSync:
    branch: develop   # ← khác với prod (main)
```

### Thứ tự ưu tiên để làm

1. Trước mắt: tạo BQ datasets dev + set Variable `env=dev` trên instance dev
2. Dùng hàm `get_dataset()` trong mọi plugin file thay vì hardcode tên dataset
3. Sau đó: tách git branch `develop` → `main`
4. Dài hạn: deploy Airflow instance riêng cho dev

---

## CI/CD là gì — và repo này đang dùng ở mức nào?

**CI/CD** *(Continuous Integration / Continuous Deployment)* = Hệ thống tự động kiểm tra và triển khai code mỗi khi có thay đổi, thay vì làm tay.

- **CI (Continuous Integration):** Tự động chạy kiểm tra khi có Pull Request — syntax, import, DAG parse — trước khi code vào `main`. Lý do cần: nếu không có CI, code sai syntax vẫn merge được và chỉ bị phát hiện sau khi Airflow đã deploy — khi đó DAG đã fail trên production rồi.

- **CD (Continuous Deployment):** Code vào `main` là tự động lên production, không cần ai nhớ deploy. Lý do cần: khi có nhiều người thay đổi DAG, deploy tay dễ quên, dễ nhầm version, dễ thiếu file.

**Repo này đang dùng CD nhưng chưa có CI:**

- ✅ **CD có rồi** — git-sync kéo code từ `main` về mỗi 60 giây, DAG tự xuất hiện trên Airflow. Đây là CD ở dạng đơn giản nhất.
- ❌ **CI chưa có** — không có bước kiểm tra tự động nào trước khi code vào `main`. Nếu viết sai Python syntax trong DAG file, Airflow sẽ báo lỗi sau khi đã deploy rồi.

**Nếu muốn thêm CI**, có thể dùng GitHub Actions để mỗi khi mở Pull Request, tự động chạy:
```
- python -m py_compile airflow/dags/**/*.py   # kiểm tra syntax
- airflow dags list                            # kiểm tra DAG parse được
```

Như vậy lỗi cơ bản bị chặn trước khi vào `main`, không phải sau khi đã deploy.

---

## Nguồn tham khảo

**Apache Airflow (chính thống nhất)**
- [Best Practices — Apache Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) — nguồn gốc rễ, bao gồm idempotency, cách viết DAG đúng, tránh top-level code
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag) — cụ thể về cách tổ chức DAG file

**Astronomer (công ty đứng sau Airflow, blog chất lượng cao)**
- [Airflow Best Practices](https://www.astronomer.io/blog/airflow-best-practices/) — đi sâu hơn docs chính thức, có ví dụ thực tế
- [Idempotency in Airflow](https://www.astronomer.io/blog/idempotency-in-airflow/) — giải thích nguyên tắc idempotent cụ thể trong context Airflow
- [DAG Authoring Best Practices](https://docs.astronomer.io/learn/dag-best-practices) — checklist đầy đủ khi viết DAG

**Google Cloud / BigQuery**
- [BigQuery best practices: Loading data](https://cloud.google.com/bigquery/docs/best-practices-performance-input) — hướng dẫn chính thức về TRUNCATE, MERGE, temp table pattern
- [BigQuery Data Ingestion: Internals, Tradeoffs](https://medium.com/google-cloud/bigquery-data-ingestion-methods-tradeoffs-e1f15c6ca2f6) — so sánh các phương pháp: CREATE OR REPLACE, partition overwrite, MERGE
- [dbt BigQuery partition copy](https://docs.getdbt.com/blog/bigquery-ingestion-time-partitioning-and-partition-copy-with-dbt) — cách các team lớn dùng partition overwrite trong production
- [Airflow on Google Cloud Composer](https://cloud.google.com/composer/docs/best-practices-dags) — best practices từ Google cho Airflow chạy trên GCP

**Khái niệm nền (ACID, idempotency)**
- [ACID properties — Wikipedia](https://en.wikipedia.org/wiki/ACID) — nguồn gốc của Atomic, Consistent, Isolated, Durable
- *Fundamentals of Data Engineering* — Joe Reis & Matt Housley (O'Reilly, 2022) — cuốn sách bao quát nhất về data engineering hiện tại, có chương về pipeline design và orchestration
