import ast
import json

from kafka import KafkaConsumer

consumer = None
csv_files = {}


def init_csv_files():
    csv_files['naivebayes'] = "../dataset/naivebayes/train-processed.csv"
    csv_files['svm'] = "../dataset/svm/train-processed.csv"
    csv_files['decisiontree'] = "../dataset/decisiontree/train-processed.csv"
    csv_files['randomforest'] = "../dataset/randomforest/train-processed.csv"
    csv_files['xgboost'] = "../dataset/xgboost/train-processed.csv"


def connect_to_kafka():
    global consumer
    print('connect to kafka...')
    try:
        consumer = KafkaConsumer(
            'twitter',
            # bootstrap_servers=['137.117.169.11:9092'],
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group',
            # value_deserializer=lambda m: json.loads(m.decode('ascii', 'ignore')))
            value_deserializer=lambda m: ast.literal_eval(json.dumps(m.decode('ascii', 'ignore')))
        )
    except Exception as e:
        print('error while connecting to kafka.\n' + str(e))


def main():
    try:
        init_csv_files()
        connect_to_kafka()
    except Exception as e:
        print('main error:' + str(e))


if __name__ == '__main__':
    main()
