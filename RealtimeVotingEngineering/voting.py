import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

# Consumer doc tu kafka
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":
    # Connect db
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Query du lieu ung cu vien
    candidates_query = cur.execute("""
            SELECT row_to_json(t)
            FROM (
                SELECT * FROM candidates
            ) t;
        """)

    # Lay du lieu ung cu vien va in
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    # Dang ky voi consumner de lang nghe tu topic
    consumer.subscribe(['voters_topic'])

    # Xu ly doc lien tuc tu kafka
    try:
        while True:
            # Lay tin nhan voi thoi gian cho 1s
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8')) # Chuyen doi json -> python object
                chosen_candidate = random.choice(candidates) # Chon random ung cu vien
                # Cử tri, ung cu vien duoc chon, thời gian bầu cử
                vote = voter | chosen_candidate | {
                    "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "vote": 1
                }

                # Xử lý và lưu phiếu bầu vào csdl postgresQL
                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                                INSERT INTO votes (voter_id, candidate_id, voting_time)
                                VALUES (%s, %s, %s)
                            """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                    conn.commit()

                    # Gửi tin chứa phiếu bầu vào votes_topic khác
                    producer.produce(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report # Callback xac nhan gui fail/success
                    )
                    producer.poll(0) # Đẩy tin nhan ra ngoai
                except Exception as e:
                    print("Error: {}".format(e))
                    # conn.rollback()
                    continue
            time.sleep(0.2)
    except KafkaException as e:
        print(e)
