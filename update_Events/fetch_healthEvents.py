from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
import json
from datetime import datetime


# Connecting to database and creating schema
load_dotenv()
DATABASE_URI = os.getenv('DATABASE_URI')
EXTERNAL_BROKER = "3.80.138.59:9092"
CONSUMER_TOPIC = "health_events"
CONSUMER_GROUP = "health_events_consumer_group2"

engine = create_engine(DATABASE_URI, echo=False) 
Base = declarative_base()

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    Location = Column(String(150), nullable=False)
    Severity = Column(String(150), nullable=False)
    EventType = Column(String(150), nullable=False)
    Details = Column(String(150), nullable=False)
    Timestamp = Column(DateTime, nullable=False)  # Use sqlalchemy.DateTime

def safe_deserializer(m):
    try:
        return json.loads(m.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"Deserialization error: {e}. Raw message: {m}")
        return None 
    

consumer = KafkaConsumer(
    CONSUMER_TOPIC,
    bootstrap_servers=[EXTERNAL_BROKER],
    value_deserializer=safe_deserializer,
    auto_offset_reset="earliest",  # Start from earliest if no committed offsets
    enable_auto_commit=True,       # Commit manually
    group_id=CONSUMER_GROUP    
)





def update_db():
    session = Session()
    count = 0  # Counter for number of entries processed
    
    try:
        while True:
            # Poll the Kafka topic with a timeout
            print("Polling Kafka for messages...")
            messages = consumer.poll(timeout_ms=5000)  # Polls every 5 seconds
            if messages.items():
                for topic_partition, msg_list in messages.items():
                    for message in msg_list:
                        try:
                            # Process the message
                            event = message.value
                            
                            # Skip invalid or None messages
                            if event is None:
                                print(f"Skipping invalid message at offset {message.offset}.")
                                continue
                            
                            # Validate required fields
                            required_fields = ["Location", "Severity", "EventType", "Timestamp", "Details"]
                            if not all(field in event for field in required_fields):
                                print(f"Missing required fields in message at offset {message.offset}: {event}")
                                continue
                            
                            # Parse the timestamp
                            try:
                                timestamp = datetime.strptime(event.get("Timestamp"), '%Y-%m-%d %H:%M:%S')
                            except (TypeError, ValueError) as e:
                                print(f"Invalid timestamp format for message at offset {message.offset}: {e}")
                                continue
                            
                            # Create and add a new Event object
                            new_event = Event(
                                Location=event.get("Location"),
                                Severity=event.get("Severity"),
                                EventType=event.get("EventType"),
                                Timestamp=timestamp,
                                Details=event.get("Details")
                            )
                            session.add(new_event)
                            count += 1
                            session.commit()

                        except Exception as msg_error:
                            print(f"Error processing message at offset {message.offset}: {msg_error}")
                            session.rollback()  # Rollback any partial transaction

            else:  # No more messages
                print(f"No new messages. Total committed entries: {count}")
                break

    except Exception as e:
        session.rollback()
        print(f"Error occurred during database update: {e}")
    finally:
        consumer.close()
        session.close()
        print(f"Closed consumer and session. Total committed entries: {count}")





def count_and_print_entries():
    session = Session()
    try:
        # Count the number of entries
        total_count = session.query(Event).count()
        print(f"Total number of entries in the Event table: {total_count}")

        # Query and print the first 20 entries
        events = session.query(Event).limit(20).all()
        print("\nDisplaying up to 20 entries:")
        for event in events:
            print(f"ID: {event.id}, Location: {event.Location}, Severity: {event.Severity}, "
                  f"EventType: {event.EventType}, Details: {event.Details}, Timestamp: {event.Timestamp}")
    except Exception as e:
        print(f"Error occurred while querying the database: {e}")
    finally:
        session.close()




if __name__ == '__main__':  
    #Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)  # Create tables with updated schema
    Session = sessionmaker(bind=engine)
    update_db()
    count_and_print_entries()


