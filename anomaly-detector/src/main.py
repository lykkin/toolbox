from kafka import KafkaConsumer
import json
import time

from nupic.frameworks.opf.model_factory import ModelFactory

from model_params import MODEL_PARAMS


def getConsumer():
    try:
        return KafkaConsumer(
            'parentSpans',
            group_id='anomalous_spans',
            bootstrap_servers='kafka:9092'
        )
    except:
        time.sleep(5)
        return getConsumer()

consumer = getConsumer()

model = ModelFactory.create(MODEL_PARAMS)
model.enableInference({'predictedField': 'duration'})

for msg in consumer:
    messageValue = json.loads(msg.value)
    # pull out app local roots, this should be a look up for a known tag later
    for span in messageValue['spans']:
        result = model.run({
            'duration': span['finish_time'] - span['start_time']
        })

        anomalyScore = result.inferences['anomalyScore']
        if anomalyScore > 0.9:
            print 'found an anomaly in ', anomalyScore, span['span_id']
