from kafka import KafkaConsumer, KafkaProducer
import json
import time

from nupic.frameworks.opf.model_factory import ModelFactory

from model_params import MODEL_PARAMS


def getConsumer():
    try:
        return KafkaConsumer(
            'rootSpans',
            group_id='anomalous_spans',
            bootstrap_servers='kafka:9092'
        )
    except Exception as e:
        print 'error connecting to kafka, waiting before trying again', e
        time.sleep(5)
        return getConsumer()


def getProducer():
    try:
        return KafkaProducer(
            bootstrap_servers='kafka:9092'
        )
    except Exception as e:
        print 'error connecting to kafka, waiting before trying again', e
        time.sleep(5)
        return getProducer()

consumer = getConsumer()
producer = getProducer()

print 'hello'
model = ModelFactory.create(MODEL_PARAMS)
model.enableInference({'predictedField': 'duration'})
print 'after'

seen = 0
for msg in consumer:
    print 'starting anomaly detect'
    messageValue = json.loads(msg.value)
    print 'after load'
    # pull out app local roots, this should be a look up for a known tag later
    seen += len(messageValue['spans'])
    print 'starting parsing'
    for span in messageValue['spans']:
        duration = span['finish_time'] - span['start_time']
        result = model.run({
            'duration': duration
        })

        anomalyScore = result.inferences['anomalyScore']
        if anomalyScore > 0.9:
            print 'found an anomaly (%s): %d' % (span['trace_id'], duration)
            producer.send('interestingTraces', bytes(span['trace_id']))
    print 'done ', seen
