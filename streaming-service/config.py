TOPIC_NAME_OUT = "ioiimm_out"
TOPIC_NAME_IN = "ioiimm_in"

postgresql_settings = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "driver": "org.postgresql.Driver",
    "dbtable": "subscribers_feedback",
    "user": "jovyan",
    "password": "jovyan",
}

kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";"
}

kafka_bootstrap_servers = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
