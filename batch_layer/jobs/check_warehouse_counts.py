import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from batch_layer.config.iceberg_spark import build_iceberg_spark
from pyspark.sql import functions as F

def main():
    spark = build_iceberg_spark(app_name="check_warehouse_counts")
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.table("local.raw.raw_banking_events")
        print(f"📊 Tổng số sự kiện thô (Bronze): {df.count()}")
        df.groupBy("event_type").count().orderBy(F.desc("count")).show()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
