#!/usr/bin/python3

from kafka import KafkaConsumer
import json
import ast
import time
import pymysql
import cryptography
import smtplib
from email.message import EmailMessage


def fetch_card_details(card_no):
    print("fetching card deatils")
    try:
        conn=pymysql.connect(host="localhost",user="hadoop",password="hadoop",database="kafka")
        cursor=conn.cursor()
        sql="select card_number,username,card_status,useremail_id from card_details where card_number=%s"
        cursor.execute(sql,(card_no))
        datas=cursor.fetchone()
        print(datas)
        if datas!=None:
            card_number=datas[0]
            username=datas[1]
            card_status=datas[2]
            useremail_id=datas[3]
            #block the card
            if card_status!="Blocked":
                print("blocking the card")
                sql="update card_details set card_status=%s where card_number=%s"
                cursor.execute(sql,("Blocked",card_no))
                conn.commit()
            return card_number,username,card_status,useremail_id
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def send_email(card_details, alert_type, count, alert_time):
    # Tuple unpacking - card_details = (card_number, username, card_status, useremail_id)

    card_number, username, card_status, useremail_id = card_details
    ## Email configuration
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    SENDER_EMAIL = "@gmail.com"  # replace with senders email ID.
    SENDER_PASSWORD = "************" # replace the password from gmail app password   

    #### Create message
    msg = EmailMessage()
    msg["From"] = SENDER_EMAIL
    msg["To"] = useremail_id
    msg["Subject"] = "Fraud Alert: Suspicious Card Activity"
    body = f"""
Dear {username},

We detected suspicious activity on your card. 

-----------------------------------
Card Number : {card_number}
Status      : {card_status}
Alert Type  : {alert_type}
Count       : {count}
Alert Time  : {alert_time}
-----------------------------------

Please contact HDFC Support immediately if this was not you.

Regards,
HDFC Bank Fraud Detection Team
"""
    msg.set_content(body)
    #### Send email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()   # Secure connection
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
            print(f"Email sent to {useremail_id}")
    except Exception as e:
        print("Email failed:", e)

##### Kafka consumer
consumer = KafkaConsumer(
    "fraud_alerts",
    bootstrap_servers="localhost:29092",   # use kafka:9092 if inside docker
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="fraud-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Listening for fraud alerts...\n")

##### Consume messages
for msg in consumer:
    data = msg.value
    # Fix card_no: "('xxxx',)" -> "xxxx"
    raw_card = data.get("card_no", "")
    print("raw_card",raw_card) #raw_card ('4434-2421-2321-9994',)
    try:
        # Convert tuple string to real tuple
        if raw_card.startswith("("):
            card_no = ast.literal_eval(raw_card)[0]
        else:
            card_no = raw_card
    except Exception:
        card_no = raw_card

    alert_type = data.get("alert_type")
    count = data.get("invalid_count")
    alert_time = data.get("alert_time")

    print("FRAUD ALERT")
    print(f"Card No   : {card_no}")
    print(f"Type      : {alert_type}")
    print(f"Count     : {count}")
    print(f"Time      : {alert_time}")
    print("-" * 40)
    time.sleep(1)
    #fetch the card details from card_details table if record exist block the card
    card_details=fetch_card_details(card_no)
    if card_details==None:
        print("No card details found in card_details table for card_no",card_no)
    else:
        send_email(card_details,alert_type,count,alert_time)

