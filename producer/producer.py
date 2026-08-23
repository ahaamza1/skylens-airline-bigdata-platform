import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# إعداد الـ Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

airlines = ['EgyptAir', 'Emirates', 'Qatar Airways', 'Saudia', 'Royal Jordanian']
airports = ['CAI', 'DXB', 'DOH', 'RUH', 'AMM', 'LHR', 'JFK']
statuses = ['ON_TIME', 'DELAYED', 'CANCELLED']

print("🚀 Starting Flight Simulator... Press Ctrl+C to stop.")

try:
    while True:
        # توليد بيانات رحلة وهمية
        event_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        status = random.choices(statuses, weights=[70, 20, 10])[0] # 70% On Time
        delay_minutes = 0 if status != 'DELAYED' else random.randint(15, 120)
        
        origin = random.choice(airports)
        destination = random.choice([a for a in airports if a != origin])

        flight_event = {
            "flight_id": f"FL{random.randint(1000, 9999)}",
            "airline": random.choice(airlines),
            "origin": origin,
            "destination": destination,
            "status": status,
            "delay_minutes": delay_minutes,
            "event_time": event_time
        }

        # إرسال البيانات إلى Topic
        producer.send('flight-events', value=flight_event)
        
        print(f"✈️ Sent: {flight_event['flight_id']} | {flight_event['airline']} | {flight_event['origin']} -> {flight_event['destination']} | {status}")
        
        time.sleep(2) # إرسال رحلة كل ثانيتين
        
except KeyboardInterrupt:
    print("\n🛑 Simulator stopped by user.")
finally:
    producer.close()