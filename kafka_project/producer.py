import yaml
from kafka import KafkaProducer
import os
import sys
import json
import datetime

CONFIG = 'conf'
CONFIG_FILENAME = 'config.yaml'
script_path = sys.argv[0]
version_dir = os.path.abspath(os.path.join(script_path, '../'))
config_dir = os.path.join(version_dir, CONFIG)
configfile_full_pathname = os.path.join(config_dir, CONFIG_FILENAME)


def read_config_yaml():
    config_dict = ''
    try:
        # print(configfile_full_pathname)
        print('Reading configuration file: %s' % configfile_full_pathname)
        with open(configfile_full_pathname, encoding="utf8") as config_file:
            config_dict = yaml.safe_load(config_file)
    except Exception as e:
        print('Error reading configuration file :' + configfile_full_pathname + "exiting..")
        sys.exit(-1)
    finally:
        return config_dict


def serializer(data):
    return json.dumps(data).encode('utf-8')


def send_data_to_consumer(**kwargs):
    cfg = kwargs['cfg']
    hostname = cfg['broker']['kafka']['hostname']
    print(hostname)
    port = cfg['broker']['kafka']['port']
    print(port)
    broker = f"{hostname}:{port}"
    print(broker)
    k_topic = cfg['broker']['kafka']['topic']
    print(k_topic)
    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=serializer)
    try:
        print(f"Sending messages to topic: {k_topic}...")
        for i in range(20):
            try:
                message = {
                    'id': f'sensor_{i}',
                    'Date': str(datetime.datetime.now().date()),
                    ''
                    'Time': str(datetime.datetime.now().time())
                }
                message_key = message['id'].encode('utf-8')
                message_value = message

                send_msg = producer.send(topic=k_topic, key=message_key, value=message_value)
            except Exception as e:
                print(e)
                continue

            try:
                metadata = send_msg.get(timeout=10)
                print(f"Message sent successfully: topic={metadata.topic}, "
                      f"partition={metadata.partition}, offset={metadata.offset}")
            except Exception as e:
                print(f"Failed to send message: {e}")

    except Exception as e:
        print(e)
    finally:
        if 'producer' in locals() and producer:
            producer.flush()  # Ensure all messages are sent before closing
            producer.close()
            print("Producer closed.")


if __name__ == '__main__':
    cfg = read_config_yaml()
    send_data_to_consumer(cfg=cfg)

