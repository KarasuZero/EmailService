import smtplib
from email.message import EmailMessage

from confluent_kafka import Consumer, KafkaError


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'email-consumer',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = KafkaConsumer('email', **conf)

sender = "citlalli.macgyver@ethereal.email"
password = "9nH1pz8mzaFs4u4UhZ"



print("Connecting to server..........")

# Listen for messages
for msg in consumer:
    # Decode the message value
    message = msg.value().decode('utf-8')

    # log the message to docker logs
    print(message)

    # Split the message into toUser, subject, and body
    toUser, subject, body = message.split(',')

    # Send the email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        message = f"Subject: {subject}\n\n{body}"
        server.sendmail(smtp_username, toUser, message)