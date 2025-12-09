import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType
import config


# метод для записи данных в 2 target: в PostgreSQL для фидбэков
# и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново
    # перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    (df
     .withColumn("feedback", lit(""))
     .write.format("jdbc")
     .options(**config.postgresql_settings)
     .mode("append")
     .save())
    # создаём df для отправки в Kafka. Сериализация в json.
    df_to_kafka = (df.select(to_json(struct(
        "restaurant_id",
        "adv_campaign_id",
        "adv_campaign_content",
        "adv_campaign_owner",
        "adv_campaign_owner_contact",
        "adv_campaign_datetime_start",
        "adv_campaign_datetime_end",
        "datetime_created",
        "client_id",
        "trigger_datetime_created"))
        .alias("value")))
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (df_to_kafka
     .write.format("kafka")
     .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
     .options(**config.kafka_security_options)
     .option("topic", config.TOPIC_NAME_OUT)
     .save()
     )
    # очищаем память от df
    df.unpersist()


# создаём spark сессию с необходимыми библиотеками в spark_jars_packages
# для интеграции с Kafka и PostgreSQL
spark = (SparkSession.builder
         .appName("RestaurantSubscribeStreamingService")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.jars.packages", config.spark_jars_packages)
         .getOrCreate())

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (spark.readStream
                             .format("kafka")
                             .option("kafka.bootstrap.servers",
                                     config.kafka_bootstrap_servers)
                             .options(**config.kafka_security_options)
                             .option("subscribe", config.TOPIC_NAME_IN)
                             .load())

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", LongType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])


# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
def current_timestamp_utc():
    return int(round(datetime.utcnow().timestamp()))


# десериализуем из value сообщения json и фильтруем по времени старта
# и окончания акции
filtered_read_stream_df = (restaurant_read_stream_df
                           .withColumn("parsed_key_value", from_json(col("value").cast("string"), incomming_message_schema))
                           .select("parsed_key_value.*")
                           .filter((current_timestamp_utc() >= col("adv_campaign_datetime_start"))
                                   & (current_timestamp_utc() <= col("adv_campaign_datetime_end")))
                           )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (spark.read
                             .format("jdbc")
                             .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
                             .option("driver", "org.postgresql.Driver")
                             .option("dbtable", "subscribers_restaurants")
                             .option("user", "student")
                             .option("password", "de-student")
                             .load())

# джойним данные из сообщения Kafka с пользователями подписки по
# restaurant_id (uuid). Добавляем время создания события.
result_df = (filtered_read_stream_df
             .join(subscribers_restaurant_df, "restaurant_id")
             .withColumn("trigger_datetime_created", lit(current_timestamp_utc()))
             .drop("subscribers_restaurant_df.restaurant_id")
             )

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", "checkpoint_dir") \
    .start() \
    .awaitTermination()
