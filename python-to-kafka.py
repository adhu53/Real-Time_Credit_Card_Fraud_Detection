#! /usr/bin/python3
import time
import pymysql
from kafka import KafkaProducer
import json
import datetime
import time

producer=KafkaProducer(bootstrap_servers="localhost:29092",key_serializer=lambda k:k.encode("utf-8"),value_serializer=lambda v:json.dumps(v).encode("utf-8"))

def send_transaction(trans_id,card_no,amount,trans_status,trans_time):
    trnx={"trans_id":trans_id,"card_no":card_no,"amount":str(amount),"trans_status":trans_status,"trans_time":str(trans_time)}
    producer.send(topic="credit_card_transactions",key=card_no,value=trnx)
    producer.flush()
    print("record sent")

def process_transactions(last_processed_time):
    try:
        conn=pymysql.connect(host="localhost",database="kafka",user="hadoop",password="hadoop")
        cursor=conn.cursor()
        if last_processed_time is None:
            print("if executed")
            sql="select * from transactions where time_stamp < now() order by time_stamp"
            cursor.execute(sql)
            data=cursor.fetchall()
            print("row")
            for row in data:
                trans_id=row[0]
                card_no=row[1]
                amount=row[2]
                trans_status=row[3]
                trans_time=row[4]
                send_transaction(trans_id,card_no,amount,trans_status,trans_time)
                last_processed_time=row[4]
        else:
            print("else executing")
            sql="select * from transactions where time_stamp>%s order by time_stamp"
            cursor.execute(sql,(last_processed_time,))
            data=cursor.fetchall()
            for row in data:
                trans_id=row[0]
                card_no=row[1]
                amount=row[2]
                trans_status=row[3]
                trans_time=row[4]
                send_transaction(trans_id,card_no,amount,trans_status,trans_time)
                last_processed_time=row[4]
        return last_processed_time
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



if __name__=="__main__":
    last_processed_time=None
    while True:
        print("main mentod started")
        last_processed_time=process_transactions(last_processed_time)
        time.sleep(2)
