import random
import time
import json

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime, timezone


fake = Faker()

def generate_sales_transactions():
  user = fake.simple_profile()


  return {
    "transaction_id": fake.uuid4(),
    "product_id": random.choice(['product_1', 'product_2', 'product_3', 'product_4', 'product_5', 'product_6']),
    "product_name": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
    "product_category": random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
    "product_price": round(random.uniform(10, 1000), 2),
    "product_quantity": random.randint(1, 10),
    "product_brand": random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
    'currency': random.choice(['USD', 'GBP']),
    "customer_id": user['username'],
    "transaction_date": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
    "payment_method": random.choice(['credit_card', 'debit_card', 'online_transfer'])
  }


def delivery_report(err, msg):
  if err is not None:
    print(f"Message Delivery Failed: {err}")
  else:
    # When data deliveryed to Kafka, we can check the message
    print(f"Message Deliveryed to {msg.topic} [{msg.partition()}]")
  


def main():
  topic = "financial_transactions"

  producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092'
  })

  curr_time = datetime.now()

  # keep generating transaction until over 2 minutes
  while(datetime.now() - curr_time).seconds < 120:
    try:
      transaction = generate_sales_transactions()
      transaction['total_amount'] = transaction['product_price'] * transaction['product_quantity']

      print(f"Generating Transaction: {transaction}")


      producer.produce(topic=topic,
                       key=transaction['transaction_id'],
                       value=json.dumps(transaction),
                       on_delivery=delivery_report)
      
      # Ensure data get deliveryed before next one get sent
      producer.poll()

      # Wait for 3 seconds before sending the next transaction
      time.sleep(3)
    
    except BufferError:
      print("Buffer Full! Please Wait...")
      time.sleep(1)

    except Exception as e:
      print(f"Generating Transaction Failed: {e}")


if __name__ == "__main__":
  main()