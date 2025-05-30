import json
import pandas as pd
from kafka import KafkaConsumer
import yaml
import os
import sys

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


def consume_data_from_producer(**kwargs):
    try:
        data = []
        cfg = kwargs['cfg']
        hostname = cfg['broker']['kafka']['hostname']
        print(hostname)
        port = cfg['broker']['kafka']['port']
        print(port)
        broker = f"{hostname}:{port}"
        print(broker)
        k_topic = cfg['broker']['kafka']['topic']
        print(k_topic)
        group_id = cfg['broker']['kafka']['group_id']
        print(group_id)
        consumer = KafkaConsumer(k_topic,bootstrap_servers=[broker],
                                 group_id=group_id,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=5000,
                                 enable_auto_commit=True,
                                 auto_commit_interval_ms=5000
                                 )
        print(f"Listening for JSON messages on topic: {broker}...")
        message_count = 0
        try:
            for message in consumer:
                if message:
                    try:
                        json_data = message.value
                        data.append(json_data)
                        message_count += 1
                        df = pd.DataFrame(data)
                        print("DataFrame created:")
                        df.to_csv('output.csv', index=False)
                    except Exception as e:
                        print(e)
                        continue
        except Exception as e:
            print(e)
        finally:
            consumer.close()
            print(data)
    except Exception as e:
        print(e)
    finally:
        if 'consumer' in locals() and consumer:
            consumer.close()
            print("Consumer closed.")


if __name__ == '__main__':
    cfg = read_config_yaml()
    consume_data_from_producer(cfg=cfg)
