#!/usr/bin/env python3
"""
TFL Gold Layer Spark Job
Reads the star-schema Hive tables and produces 7 aggregated
analytics Parquet datasets in the HDFS gold layer.
"""

from pyspark.sql import SparkSession

GOLD_BASE = '/tmp/gokul/tfl_project1/gold'
HIVE_DB   = 'gokul_tfl_proj'


def write_table(spark, name, sql):
    print(f"Writing gold table: {name}")
    spark.sql(sql).write.mode("overwrite").parquet(f"{GOLD_BASE}/{name}")
    print(f"  ✓ {GOLD_BASE}/{name}")


def main():
    spark = SparkSession.builder \
        .appName("TFL_Gold_Layer") \
        .enableHiveSupport() \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"USE {HIVE_DB}")

    # 1. Busiest Stations by total passenger flow
    write_table(spark, "busiest_stations", """
        SELECT s.station_name,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_stations s ON f.station_id = s.station_id
        GROUP  BY s.station_name
        ORDER  BY total_passengers DESC
    """)

    # 2. Yearly Passenger Totals
    write_table(spark, "yearly_passengers", """
        SELECT d.year,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_date d ON f.date_id = d.date_id
        GROUP  BY d.year
        ORDER  BY d.year
    """)

    # 3. Traffic by Tube Line
    write_table(spark, "line_traffic", """
        SELECT l.line_name,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   fact_station_lines sl ON f.station_id = sl.station_id
        JOIN   dim_lines l           ON sl.line_id   = l.line_id
        GROUP  BY l.line_name
        ORDER  BY total_passengers DESC
    """)

    # 4. Traffic by Network Type
    write_table(spark, "network_analysis", """
        SELECT n.network_name,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_stations s ON f.station_id = s.station_id
        JOIN   dim_networks n ON s.network_id = n.network_id
        GROUP  BY n.network_name
        ORDER  BY total_passengers DESC
    """)

    # 5. Interchange Stations (number of connecting lines)
    write_table(spark, "interchange_stations", """
        SELECT s.station_name,
               COUNT(sl.line_id) AS num_lines
        FROM   fact_station_lines sl
        JOIN   dim_stations s ON sl.station_id = s.station_id
        WHERE  sl.is_interchange = 1
        GROUP  BY s.station_name
        ORDER  BY num_lines DESC
    """)

    # 6. Quarterly Ridership Trends
    write_table(spark, "quarterly_trends", """
        SELECT d.year,
               d.quarter,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_date d ON f.date_id = d.date_id
        GROUP  BY d.year, d.quarter
        ORDER  BY d.year, d.quarter
    """)

    # 7. Night Tube vs Day Tube Passenger Metrics
    write_table(spark, "night_tube_metrics", """
        SELECT l.line_name,
               l.is_night_tube,
               SUM(f.entry_count + f.exit_count) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   fact_station_lines sl ON f.station_id = sl.station_id
        JOIN   dim_lines l           ON sl.line_id   = l.line_id
        GROUP  BY l.line_name, l.is_night_tube
        ORDER  BY l.is_night_tube DESC, total_passengers DESC
    """)

    print(f"\nGold layer complete → {GOLD_BASE}")
    spark.stop()


if __name__ == '__main__':
    main()
