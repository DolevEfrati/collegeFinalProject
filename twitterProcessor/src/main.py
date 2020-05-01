from kafka import KafkaConsumer
import naivebayesnati, svmnati, decisiontreenati, randomforestnati, xgboostnati, preprocess
import json, ast
import re
import cPickle as pickle
import requests
from datetime import datetime

LOAD_FILES=True
CSV_FILE_NAIVEBAYES="c:/Users/User/Desktop/final-project/twitter-sentiment-analysis/dataset/naivebayes/train-processed.csv"
CSV_FILE_SVM="c:/Users/User/Desktop/final-project/twitter-sentiment-analysis/dataset/svm/train-processed.csv"
CSV_FILE_DECISIONTREE="c:/Users/User/Desktop/final-project/twitter-sentiment-analysis/dataset/decisiontree/train-processed.csv"
CSV_FILE_RANDOMFOREST="c:/Users/User/Desktop/final-project/twitter-sentiment-analysis/dataset/randomforest/train-processed.csv"
CSV_FILE_XGBOOST="c:/Users/User/Desktop/final-project/twitter-sentiment-analysis/dataset/xgboost/train-processed.csv"

print 'Connecting kafka consumer...'
consumer = KafkaConsumer(
    'twitter',
     #bootstrap_servers=['137.117.169.11:9092'],
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     #value_deserializer=lambda m: json.loads(m.decode('ascii', 'ignore'))) 
     value_deserializer=lambda m: ast.literal_eval(json.dumps(m.decode('ascii', 'ignore')))
)
print 'Kafka consumer is conected!'


def sendWinner(winner):
    requests.post('http://localhost:9200/index12/_doc', json={
        'date': createdAt,
        'winner': winner
    })
    
def saveClf(file, clf):
    print("Saving " + file + "...")
    with open(file, 'wb') as output:
        pickle.dump(clf, output, pickle.HIGHEST_PROTOCOL)
    
    print(file + " saved!")

def loadClf(file):
    data = 1
    print("Loading " + file + "...")
    try:
        with open('code/' + file, 'rb') as f:
            data = pickle.load(f)
    except EOFError as exc:
        print(exc)

    print(file + " loaded!")
    return data


if LOAD_FILES:
    naivebayesClf = loadClf('naivebayesClf.pkl')
    svmClf = loadClf('svmClf.pkl')
    decisiontreeClf = loadClf('decisiontreeClf.pkl')
    randomforestClf = loadClf('randomforestClf.pkl')
    xgboostClf = loadClf('xgboostClf.pkl')
else:
    naivebayesClf = naivebayesnati.clfPredicator(CSV_FILE_NAIVEBAYES,False)
    saveClf('naivebayesClf.pkl', naivebayesClf)

    svmClf = svmnati.clfPredicator(CSV_FILE_SVM,False)
    saveClf('svmClf.pkl', svmClf)

    decisiontreeClf = decisiontreenati.clfPredicator(CSV_FILE_DECISIONTREE,False)
    saveClf('decisiontreeClf.pkl', decisiontreeClf)

    randomforestClf = randomforestnati.clfPredicator(CSV_FILE_RANDOMFOREST,False)
    saveClf('randomforestClf.pkl', randomforestClf)

    xgboostClf = xgboostnati.clfPredicator(CSV_FILE_XGBOOST,False)
    saveClf('xgboostClf.pkl', xgboostClf)

print 'Start listening to kafka...'

for message in consumer:
    j = json.loads(message.value.decode('utf-8', 'ignore'))
    print('\n####################')
    print(j)
    print('\n')
    tweet = j['text'].encode('ascii','ignore')
    print('Tweet before preprocessing: ' + tweet)
    tweet = preprocess.preprocess_tweet(tweet)
    print('Tweet after preprocessing: ' + tweet)
    val_tweets = [['1', '1', naivebayesnati.get_feature_vector(tweet)]]
    for tweet, val_set_y in naivebayesnati.extract_features(val_tweets,1,False,'frequency'):
        value_naivebayes = int(naivebayesClf.predict(tweet))
        print(value_naivebayes)
    for tweet, val_set_y in svmnati.extract_features(val_tweets,1,False,'frequency'):
        value_svm = int(svmClf.predict(tweet))
        print(value_svm)
    for tweet, val_set_y in decisiontreenati.extract_features(val_tweets,1,False,'frequency'):
        value_decisiontree = int(decisiontreeClf.predict(tweet))
        print(value_decisiontree)
    for tweet, val_set_y in randomforestnati.extract_features(val_tweets,1,False,'presence'):
        value_randomforest = int(randomforestClf.predict(tweet))
        print(value_randomforest)
    for tweet, val_set_y in xgboostnati.extract_features(val_tweets,1,False,'frequency'):
        value_xgboost = int(xgboostClf.predict(tweet))
        print(value_xgboost)
    total_value=int(round((value_naivebayes + value_svm + value_decisiontree + value_randomforest + value_xgboost)/5.0))
    print(total_value)

    country = j['country']
    fullName = j['fullName']
    createdAt = j['createdAt']

    if(total_value == int(value_naivebayes)):
        sendWinner('naivebayes')
    if(total_value == int(value_svm)):
        sendWinner('svm')
    if(total_value == int(value_decisiontree)):
        sendWinner('decisiontree')
    if(total_value == int(value_randomforest)):
        sendWinner('randomforest')
    if(total_value == int(value_xgboost)):
        sendWinner('xgboost')

    res = requests.post('http://localhost:9200/index12/_doc', json={
        'value': 1-total_value,
        'country': country,
        'fullName': fullName,
        'date': createdAt,
        'naivebayes': total_value == int(value_naivebayes),
        'svm': total_value == int(value_svm),
        'decisiontree': total_value == int(value_decisiontree)
        # 'randomforest': total_value == int(value_randomforest),
        # 'xgboost': total_value == int(value_xgboost)
    })
    print('\n####################')
