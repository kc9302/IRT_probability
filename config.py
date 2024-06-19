CASSANDRA_INFO = {
    "SERVER_HOST": [
        "cassandra-0.cassandra.default.svc.cluster.local",
        "cassandra-1.cassandra.default.svc.cluster.local",
        "cassandra-2.cassandra.default.svc.cluster.local"
         ],
    "HOST": ["HOST"],
    "PORT": "PORT",
    "KEYSPACE": "KEYSPACE",
    "RAW_DATA_TABLE": "RAW_DATA_TABLE",
    "IRT_3PL": "irt_3pl",
    "IRT_ABILITY": "irt_ability",
    "IRT_PROBABILITY": "irt_probability"
}

SPARK_INFO = {
    "READ_FORMAT": "org.apache.spark.sql.cassandra",
    "LOG_LEVEL": "WARN"
}