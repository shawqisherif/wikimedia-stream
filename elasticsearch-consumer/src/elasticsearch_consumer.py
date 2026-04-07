#!../.venv/bin/python3.14
import io
import json
import logging
import sys
import time

import snappy
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="elastic_consumer.log",
)

consumer_id = ""
if len(sys.argv) > 1:
    consumer_id = sys.argv[1]
    logger = logging.getLogger(f"ElasticConsumer-{consumer_id}")
else:
    logger = logging.getLogger("ElasticConsumer")
topic = "wikimedia-recentchange"
group_id = "wikimedia"
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "partition.assignment.strategy": "cooperative-sticky",
    # Optional: ensure your session timeout is high enough to handle the 2-phase shift
    "session.timeout.ms": 45000,
    "enable.auto.commit": "false",
}
consumer = Consumer(conf)
consumer.subscribe([topic])

es = Elasticsearch("http://localhost:9200")
index = "wikimedia"


def recceive_report(records):
    if records is None:
        logger.info(f"Consumer, No messages waiting...")
        time.sleep(3)
    else:
        bulk_json = []
        for record in records:
            if record.error():
                if record.error().code() == "KafkaError._PARTITION_EOF":
                    continue
                else:
                    logger.error(
                        f"Consumer message received error!: {record.error().code()}"
                    )
                    break
            if not (record.value() is None and record.value() == ""):
                lines = record.value().splitlines()
                for line in lines:
                    try:
                        line_utf8 = line.decode("utf8")
                        try:
                            if line_utf8 is not None and line_utf8.startswith("data: "):
                                data = line_utf8.replace("data: ", "").strip()
                                json_data = json.loads(data)
                                document_id = json_data["meta"]["id"]
                                bulk_json.append(
                                    {
                                        "_index": index,
                                        "_id": document_id,
                                        "source": json_data,
                                    }
                                )
                                # bulk_json.append(json_data)
                                # es.index(index="wikimedia", document=json_data, id=document_id)
                        except Exception as e:
                            with open("./DLQ.txt", "+a") as dlq:
                                dlq.write(record.value().decode("utf8") + "\n")
                            logger.error(f"Error, record sent to DLQ: {e}")
                    except Exception as e:
                        logger.error(f"Decoding error record failed {line}")
                logger.info(
                    f"message sent successfully to Elasticsearch: topic {record.topic()}, partition [{record.partition()}], offset [{record.offset()}]"
                )
        try:
            bulk(es, bulk_json)
        except Exception as e:
            logger.error(f"Elasticsearch Error : {e}")
        return True


if __name__ == "__main__":
    try:
        while True:
            records = consumer.consume(num_messages=500, timeout=1.0)
            if records is not None:
                recceive_report(records)
                consumer.commit()
    except KeyboardInterrupt:
        logger.info("User interruptted consumer, exitting....")
    except KafkaException as e:
        logger.info("Consumer wake up exception!")
    except Exception as e:
        logger.error(f"Consumer exception: {e}")
    finally:
        consumer.close()
        logger.info("Consumer now greacefully closed")
