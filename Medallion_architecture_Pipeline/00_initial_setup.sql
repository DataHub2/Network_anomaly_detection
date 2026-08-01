CREATE CATALOG IF NOT EXISTS network_anomaly_detection;

USE CATALOG network_anomaly_detection;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE VOLUME IF NOT EXISTS network_anomaly_detection.bronze.raw;