# start_system.ps1
Clear-Host
Write-Host "KHỞI ĐỘNG HỆ THỐNG BANKING FRAUD DETECTION" -ForegroundColor Yellow
Write-Host "=================================================" -ForegroundColor Yellow

# 1. Khởi động hệ sinh thái Docker Compose
Write-Host "`n[1/5] Đang khởi động hạ tầng Docker Compose..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "Đang đợi cơ sở dữ liệu PostgreSQL khởi động và ở trạng thái healthy..." -ForegroundColor DarkGray
do {
    Start-Sleep -Seconds 2
    try {
        $status = (docker inspect --format='{{.State.Health.Status}}' postgres 2>$null)
    } catch {
        $status = "starting"
    }
} while ($status -ne "healthy")
Write-Host "PostgreSQL đã sẵn sàng!" -ForegroundColor Green

# 2. Cài đặt các JAR Connectors cho Flink (nếu chưa có) và khởi động lại Flink nếu cần
Write-Host "`n[2/5] Đang kiểm tra các file JAR Connectors trong Flink..." -ForegroundColor Cyan
$jarChecked = $false
try {
    $hasJar = docker exec flink-jobmanager ls /opt/flink/lib/postgresql.jar 2>$null
    if ($hasJar -match "postgresql.jar") {
        $jarChecked = $true
    }
} catch {
    $jarChecked = $false
}

if (-not $jarChecked) {
    Write-Host "Copying Connector JARs to Flink JobManager & TaskManager..." -ForegroundColor DarkGray
    docker cp flink-connector-jdbc.jar flink-jobmanager:/opt/flink/lib/
    docker cp flink-shaded-guava.jar flink-jobmanager:/opt/flink/lib/
    docker cp flink-sql-connector-kafka.jar flink-jobmanager:/opt/flink/lib/
    docker cp postgresql.jar flink-jobmanager:/opt/flink/lib/
    
    docker cp flink-connector-jdbc.jar flink-taskmanager:/opt/flink/lib/
    docker cp flink-shaded-guava.jar flink-taskmanager:/opt/flink/lib/
    docker cp flink-sql-connector-kafka.jar flink-taskmanager:/opt/flink/lib/
    docker cp postgresql.jar flink-taskmanager:/opt/flink/lib/
    
    Write-Host "Restarting Flink containers to apply classpath changes..." -ForegroundColor DarkGray
    docker restart flink-jobmanager flink-taskmanager
    Start-Sleep -Seconds 10
} else {
    Write-Host "Các JAR Connectors đã có sẵn." -ForegroundColor Green
}

# 3. Cài đặt Python & các thư viện cần thiết trong container Flink (nếu chưa có)
Write-Host "`n[3/5] Đang kiểm tra môi trường Python & các thư viện cho Flink..." -ForegroundColor Cyan
$pythonInstalled = $false
try {
    $hasPython = docker exec flink-jobmanager which python3 2>$null
    if ($hasPython -match "python") {
        $pythonInstalled = $true
    }
} catch {
    $pythonInstalled = $false
}

if (-not $pythonInstalled) {
    Write-Host "Đang cài đặt Python3 & pip cho flink-jobmanager..." -ForegroundColor DarkGray
    docker exec flink-jobmanager bash -c "apt-get update && apt-get install -y python3 python3-pip python3-dev build-essential"
    docker exec flink-jobmanager ln -sf /usr/bin/python3 /usr/bin/python
    
    Write-Host "Đang cài đặt Python3 & pip cho flink-taskmanager..." -ForegroundColor DarkGray
    docker exec flink-taskmanager bash -c "apt-get update && apt-get install -y python3 python3-pip python3-dev build-essential"
    docker exec flink-taskmanager ln -sf /usr/bin/python3 /usr/bin/python
}

$libsInstalled = $false
try {
    $hasXGB = docker exec flink-jobmanager pip3 show xgboost 2>$null
    if ($hasXGB -match "Name: xgboost") {
        $libsInstalled = $true
    }
} catch {
    $libsInstalled = $false
}

if (-not $libsInstalled) {
    Write-Host "Đang cài đặt thư viện Python (apache-flink, xgboost, pandas)..." -ForegroundColor DarkGray
    docker exec flink-jobmanager pip3 install apache-flink==1.19.0 xgboost pandas
    docker exec flink-taskmanager pip3 install apache-flink==1.19.0 xgboost pandas
}
Write-Host "Môi trường Python trong Flink đã sẵn sàng!" -ForegroundColor Green

# 4. Copy mã nguồn Flink Job và file Models vào container
Write-Host "`n[4/5] Đang đồng bộ file Flink Job và Model AI..." -ForegroundColor Cyan
docker exec flink-jobmanager mkdir -p /tmp/models
docker exec flink-taskmanager mkdir -p /tmp/models

docker cp streaming_layer/models/xgboost_fraud_model.json flink-jobmanager:/tmp/models/
docker cp streaming_layer/models/preprocessor_artifact.json flink-jobmanager:/tmp/models/
docker cp streaming_layer/models/xgboost_fraud_model.json flink-taskmanager:/tmp/models/
docker cp streaming_layer/models/preprocessor_artifact.json flink-taskmanager:/tmp/models/

docker cp streaming_layer/fraud_detector_v2.py flink-jobmanager:/tmp/fraud_detector.py
Write-Host "Đồng bộ file thành công!" -ForegroundColor Green

# 5. Dọn dẹp Checkpoint và Khởi tạo Lakehouse Iceberg
Write-Host "`n[5/5] Đang khởi tạo hồ dữ liệu Apache Iceberg..." -ForegroundColor Cyan
docker exec spark-master bash -c "rm -rf /tmp/checkpoints/*"
docker exec spark-master /spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 /app/batch_layer/jobs/init_iceberg_tables.py

# 6. Kích hoạt Flink Real-time (chạy ngầm -d để không chặn tiến trình)
Write-Host "`n⚡ Đang nộp Job Flink Real-time (Đang chạy ngầm)..." -ForegroundColor Yellow
docker exec -d -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e POSTGRES_HOST=postgres flink-jobmanager ./bin/flink run -d -py /tmp/fraud_detector.py

# 7. Kích hoạt Spark Streaming (chạy ngầm -d)
Write-Host "⚡ Đang nộp Job Spark Streaming (Đang chạy ngầm)..." -ForegroundColor Yellow
docker exec -d spark-master /spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 /app/batch_layer/jobs/ingest_kafka_to_iceberg.py

# 8. Bật FastAPI Backend trong cửa sổ PowerShell mới sử dụng đúng môi trường fl_env
Write-Host "⚡ Đang khởi động Backend API trong cửa sổ riêng..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "C:\Users\PC\miniconda3\envs\fl_env\python.exe -m uvicorn serving_layer.api:app --host 0.0.0.0 --port 8000 --reload"

Write-Host "`n=================================================" -ForegroundColor Yellow
Write-Host "HỆ THỐNG ĐÃ KHỞI CHẠY THÀNH CÔNG TỪ A-Z!" -ForegroundColor Green
Write-Host "Hãy click đúp vào file serving_layer/index.html để trải nghiệm giao diện Dashboard." -ForegroundColor White
Write-Host "Để sinh dữ liệu demo, hãy chạy lệnh sau ở cửa sổ PowerShell mới:" -ForegroundColor Yellow
Write-Host "   python data_generation/generate_clickstream.py" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Yellow