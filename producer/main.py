from kafka import KafkaProducer
import time
import pandas as pd
import json

if __name__ == '__main__':
    kafka_producer_obj = KafkaProducer(bootstrap_servers="127.0.0.1:9092",
                                       value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    df = pd.read_csv("data/res4.csv")
    df1 = df.fillna(0)
    for row in df1.iterrows():
        msg = row[1].to_dict()
        time.sleep(0.5)
        print("sent", msg)
        kafka_producer_obj.send("anhlq36_electric", msg)
        kafka_producer_obj.flush()