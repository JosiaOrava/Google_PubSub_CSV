import json
import csv
import os
import re
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.protobuf.timestamp_pb2 import Timestamp as ProtoTimestamp
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from conf import *

# Conf
OUTPUT_DIR = "pubsub_daily_data"  
MAX_MESSAGES_TO_PULL = 10 
PULL_TIMEOUT_SECONDS = 30.0

curr_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(curr_dir, "private.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
print(f"GOOGLE_APPLICATION_CREDENTIALS set to: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")

# Common header for all rows in the daily CSV files
CSV_HEADER = [
    'Device ID',
    'Sensor Type',
    'Temperature', 
    'Humidity',    
    'Pressure',    
    'Volumetric Water Content', 
    'Electrical Conductivity' 
]

# Error check for credentials
if not os.path.exists(credentials_path):
    print(f"ERROR: Credentials file not found at location: {credentials_path}")
    print("Make sure you have downloaded service account keyfile, renamed to 'private.json' and placed it in current directory")
    exit(1)
else:
    print("Credentials file found")

def is_mac_address(string):
    return bool(re.match(r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$", string))

def process_messages():
    subscriber_client = pubsub_v1.SubscriberClient()
    subscription_path = subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    daily_file_handlers = {}
    headers_written_for_date = set()

    print(f"Listening for messages on {subscription_path}...")

    try:
        while True:
            response = subscriber_client.pull(
                request={"subscription": subscription_path, "max_messages": MAX_MESSAGES_TO_PULL},
                timeout=PULL_TIMEOUT_SECONDS,
            )

            if not response.received_messages:
                print("No messages received. Continuing to listen...")
                continue

            ack_ids = []
            # Fallback time if message's own timestamp fails
            current_processing_time_utc = datetime.now(timezone.utc)

            for received_message in response.received_messages:
                message_id = received_message.message.message_id
                
                raw_publish_time = received_message.message.publish_time
                msg_datetime_utc = None 

                if isinstance(raw_publish_time, ProtoTimestamp):
                    
                    if raw_publish_time.seconds == 0 and raw_publish_time.nanos == 0:
                        print(f"Warning: Message {message_id} has a default/unset Protobuf Timestamp for publish_time")
                        
                    else:
                        try:
                            msg_datetime_utc = ProtoTimestamp.ToDatetime(raw_publish_time)
                        except Exception as e:
                            print(f"ERROR: converting Protobuf Timestamp to datetime for message {message_id}: {e}")
                            
                elif isinstance(raw_publish_time, (datetime, DatetimeWithNanoseconds)):
                    
                    msg_datetime_utc = raw_publish_time
                
                if not msg_datetime_utc:
                    print(f"Warning: Message {message_id} using fallback time. Publish_time was '{raw_publish_time}' (type: {type(raw_publish_time)})")
                    msg_datetime_utc = current_processing_time_utc
                
                msg_date_str = msg_datetime_utc.strftime('%Y-%m-%d')
                publish_datetime_iso = msg_datetime_utc.isoformat() 
                
                try:
                    message_data_str = received_message.message.data.decode('utf-8')
                    data = json.loads(message_data_str)

                    if not data or not isinstance(data, dict) or len(data) != 1:
                        print(f"Skipping message {message_id}: Unexpected data in JSON structure")
                        ack_ids.append(received_message.ack_id)
                        continue

                    device_key_original = list(data.keys())[0]
                    device_data = data[device_key_original]
                    
                    if msg_date_str not in daily_file_handlers:
                        output_filename = os.path.join(OUTPUT_DIR, f"data_{msg_date_str}.csv")
                        file_exists = os.path.isfile(output_filename)
                        file_handle = open(output_filename, 'a', newline='')
                        writer = csv.writer(file_handle)
                        daily_file_handlers[msg_date_str] = {'writer': writer, 'file_handle': file_handle}
                        print(f"Opened/Using CSV for date {msg_date_str}: {output_filename}")
                        if not file_exists or msg_date_str not in headers_written_for_date:
                            writer.writerow(CSV_HEADER)
                            headers_written_for_date.add(msg_date_str)
                    
                    writer = daily_file_handlers[msg_date_str]['writer']

                    base_row_data = {
                        'Device ID': device_key_original,
                        'Sensor Type': '',
                        'Temperature': '',
                        'Humidity': '',
                        'Pressure': '',
                        'Volumetric Water Content': '',
                        'Electrical Conductivity': ''
                    }

                    if is_mac_address(device_key_original):
                        base_row_data['Sensor Type'] = "Ruuvitag"
                        temperatures = device_data.get('temperature', [])
                        humidities = device_data.get('humidity', [])
                        pressures = device_data.get('pressure', [])
                        num_readings = len(temperatures)
        
                        for i in range(num_readings):
                            row = base_row_data.copy()
                            row['Temperature'] = temperatures[i] if i < len(temperatures) else ''
                            row['Humidity'] = humidities[i] if i < len(humidities) else ''
                            row['Pressure'] = pressures[i] if i < len(pressures) else ''
                            writer.writerow([row[col] for col in CSV_HEADER])

                    elif device_key_original == "TEROS12":
                        base_row_data['Sensor Type'] = "TEROS 12"
                        vwc_values = device_data.get('volumetric_water_content', [])
                        temp_values = device_data.get('temperature', [])
                        ec_values = device_data.get('electrical_conductivity', [])
                        num_readings = len(vwc_values)
                        
                        for i in range(num_readings):
                            row = base_row_data.copy()
                            row['Temperature'] = temp_values[i] if i < len(temp_values) else ''
                            row['Volumetric Water Content'] = vwc_values[i] if i < len(vwc_values) else ''
                            row['Electrical Conductivity'] = ec_values[i] if i < len(ec_values) else ''
                            writer.writerow([row[col] for col in CSV_HEADER])

                    else:
                        print(f"Skipping message {message_id}: Unknown device key '{device_key_original}'.")

                    ack_ids.append(received_message.ack_id)

                except json.JSONDecodeError:
                    print(f"Skipping message {message_id}: Invalid JSON.")
                    ack_ids.append(received_message.ack_id)
                except Exception as e:
                    dev_key_info = device_key_original if 'device_key_original' in locals() else 'unknown device'
                    print(f"Error processing message data for {message_id} ({dev_key_info}): {e}")
                    ack_ids.append(received_message.ack_id)

            if ack_ids:
                subscriber_client.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": ack_ids}
                )
                print(f"Acknowledged {len(ack_ids)} messages.")

    except KeyboardInterrupt:
        print("Script interrupted. Shutting down...")
    except Exception as e:
        print(f"An critical unexpected error occurred in main loop: {e}") 
    finally:
        print("Closing daily CSV files...")
        for date_str in daily_file_handlers:
            daily_file_handlers[date_str]['file_handle'].close()
            print(f"Closed file for date {date_str}")
        if 'subscriber_client' in locals():
            subscriber_client.close()
            print("Subscriber client closed")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("ERROR: Set your GCP_PROJECT_ID in the conf file")
        exit(1)
    if not SUBSCRIPTION_ID:
        print("ERROR: Set your SUBSCRIPTION_ID in the conf file")
        exit(1)
    if not PRIVATE_KEY_FILE:
        print("ERROR: Set your PRIVATE_KEY_FILE in the conf file")
        exit(1)
    process_messages()