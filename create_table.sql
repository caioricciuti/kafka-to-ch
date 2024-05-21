CREATE TABLE iot_data (
    device_id UInt64,
    timestamp UInt64,
    temperature Float64,
    vibration Float64
) ENGINE = MergeTree()
ORDER BY timestamp;
