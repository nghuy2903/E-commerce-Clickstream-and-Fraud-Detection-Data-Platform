# E-commerce & Banking Fraud Detection Data Platform

Dự án xây dựng một nền tảng dữ liệu (Data Platform) hoàn chỉnh kết hợp xử lý thời gian thực (**Real-time - Hot Path**) và xử lý lô (**Batch - Cold Path**) theo kiến trúc Lambda/Kappa để phát hiện giao dịch gian lận tài chính trực tuyến sử dụng Machine Learning (XGBoost & Isolation Forest).

---

## 1. Sơ đồ Kiến trúc Hệ thống (Architecture Diagram)

![Sơ đồ kiến trúc Data Engineering](images/architecture_diagram.png)

Hệ thống được chia làm 4 phân lớp cốt lõi:
1.  **Data Generation Layer (Tầng sinh dữ liệu):** Simulator giả lập hành vi người dùng thật (Normal User) và Botnet tấn công (Fraud Bot) đẩy vào Kafka.
2.  **Streaming Layer (Tầng xử lý thời gian thực):** Apache Flink tiêu thụ sự kiện từ Kafka, thực hiện đếm tần suất giao dịch (Stateful), nhận diện IP lạ động và chấm điểm rủi ro bằng mô hình XGBoost Champion.
3.  **Batch Layer (Tầng xử lý lô - Lakehouse):** Apache Spark Streaming đẩy dữ liệu thô vào hồ dữ liệu **Apache Iceberg (Medallion Architecture: Bronze -> Silver -> Gold)**. Spark Batch thực hiện đồng bộ, xử lý SCD Type 2 cho tài khoản và tổng hợp dữ liệu hiệu năng hệ thống.
4.  **Serving Layer (Tầng dịch vụ & Trực quan):** API FastAPI cung cấp cổng giao tiếp, thực thi đồng thời hai mô hình AI thực tế (XGBoost & Isolation Forest) để đối chiếu (Champion vs Challenger). Giao diện Dashboard HTML/JS hiển thị biểu đồ rủi ro thời gian thực.

---

## 2. Cấu trúc Thư mục Dự án

```bash
banking_fraud_data_platform/
├── docker-compose.yml              # Cấu hình cụm Kafka, Flink, Spark, Postgres, pgAdmin
├── start_system.ps1                # Script PowerShell tự động khởi chạy cụm hệ thống từ A-Z
├── flink-connector-jdbc.jar        # Driver kết nối Database cho Flink
├── flink-sql-connector-kafka.jar   # Connector kết nối Kafka cho Flink
├── postgresql.jar                  # Driver Postgres cho Flink/Spark
├── flink-shaded-guava.jar          # Thư viện bổ trợ cho Flink
│
├── data_generation/                # Mã nguồn giả lập dữ liệu clickstream thời gian thực
│   └── generate_clickstream.py
│
├── streaming_layer/                # Tầng xử lý thời gian thực (Apache Flink)
│   ├── fraud_detector_v2.py        # Flink Stateful Job chính
│   └── models/                     # Thư mục lưu trữ Model AI dùng cho Flink inference
│
├── batch_layer/                    # Tầng xử lý lô (Apache Spark & Lakehouse Iceberg)
│   └── jobs/
│       ├── init_iceberg_tables.py  # Khởi tạo cấu trúc hồ dữ liệu Iceberg
│       ├── ingest_kafka_to_iceberg.py # Spark Streaming ghi dữ liệu vào Lakehouse
│       └── sync_gold_layer.py      # Spark Batch tổng hợp dữ liệu ra các bảng Gold
│
├── serving_layer/                  # Tầng giao diện Dashboard & FastAPI API
│   ├── api.py                      # FastAPI Backend (Chứa XGBoost & Isolation Forest thực tế)
│   ├── index.html                  # Giao diện chính Admin Dashboard
│   └── index.css / index.js        # Cấu hình giao diện và biểu đồ thời gian thực
│
└── ml_training/                    # Notebook huấn luyện và tối ưu các mô hình XGBoost, Isolation Forest
```

---

## 3. Cấu hình Cổng Kết nối (Port Map)

Khi hệ thống hoạt động, bạn có thể truy cập các dịch vụ qua các cổng sau trên máy host:

| Dịch vụ | Địa chỉ truy cập | Tên đăng nhập / Mật khẩu |
| :--- | :--- | :--- |
| **FastAPI API** | `http://localhost:8000` | *Không yêu cầu* |
| **Flink Web UI** | `http://localhost:8081` | *Không yêu cầu* |
| **Spark Master UI** | `http://localhost:8080` | *Không yêu cầu* |
| **pgAdmin (Postgres UI)** | `http://localhost:5050` | `admin@admin.com` / `admin123` |
| **PostgreSQL Database** | `localhost:5432` | DB: `banking_mlops` \| User: `admin` / `admin123` |
| **Kafka Broker (Host)** | `localhost:9092` | *Trong Docker Network: `kafka:29092`* |

---

## 4. Hướng dẫn Khởi chạy Toàn bộ Hệ thống (Quick Start)

### Bước 1: Tạo môi trường ảo Python và cài đặt thư viện
```bash
# Tạo môi trường ảo tên fl_env
conda create -n fl_env python=3.10 -y
conda activate fl_env

# Cài đặt các thư viện cần thiết
pip install apache-flink==1.19.0 xgboost pandas uvicorn fastapi psycopg2-binary confluent-kafka
```

### Bước 2: Khởi chạy một mạch từ A-Z hệ thống (Docker & Jobs & API)
Mở một cửa sổ PowerShell với quyền Administrator tại thư mục gốc của dự án và chạy:
```powershell
./start_system.ps1
```
*Script này sẽ tự động dựng Docker, cấu hình cài đặt môi trường cho các container, copy các file Model AI, khởi tạo cấu trúc Iceberg, nộp Flink Job, Spark Job chạy ngầm và kích hoạt API Server.*

### Bước 3: Khởi chạy luồng sinh dữ liệu thời gian thực (Simulator)
Mở một cửa sổ PowerShell mới, kích hoạt môi trường Conda và chạy:
```powershell
conda activate fl_env
python data_generation/generate_clickstream.py
```

### Bước 4: Trải nghiệm Dashboard
*   Click đúp vào file `serving_layer/index.html` để mở giao diện Dashboard.
*   Bạn sẽ nhìn thấy dữ liệu biểu đồ thời gian thực nhảy số liệu liên tục.
*   Hãy thử chuyển sang tab **"Giao dịch nhanh"**, thực hiện chuyển tiền (số tiền lớn hoặc nhập các IP lạ để test cơ chế tự động chặn rủi ro và khóa tài khoản bằng mã FaceID).

---

## 5. Lưu ý tối ưu cho thiết bị phần cứng giới hạn (8GB RAM)
Nếu máy tính của bạn có dung lượng RAM hạn chế (8GB RAM), hãy áp dụng các cấu hình tối ưu sau để tránh bị đơ máy khi demo:
1.  **Giới hạn RAM cho WSL2:** Tạo file `C:\Users\<Tên_User>\.wslconfig` và thêm nội dung sau để Docker không ngốn quá 2.5GB RAM:
    ```ini
    [wsl2]
    memory=2.5GB
    ```
2.  **Chạy phân đoạn luồng (Segmented Demo):** 
    *   Khi demo luồng thời gian thực: Tắt container Spark để tiết kiệm ~1.5GB RAM (`docker stop spark-master spark-worker`). Luồng Flink và API vẫn hoạt động bình thường.
    *   Khi demo luồng xử lý lô (Batch): Tắt Flink và bật Spark lên để chạy các job tổng hợp dữ liệu lịch sử vào hồ Iceberg.