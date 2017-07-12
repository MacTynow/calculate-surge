###############
# Compare all the pickup geohash and return number of each geohash per batch
# eg : dr5ruk : 50 
# then return surge factor for this location based on demande 
# eg : dr5ruk : 1.6
# geoh_counts_sorted_dstream = geoh_counts.transform((lambda foo:foo.sortBy(lambda x:( -x[1]))))
# geoh_counts_sorted_dstream.pprint()  

import json
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: calculate_surge_factor.py <broker_list> <topic>", file=sys.stderr)
    exit(-1)


  sc = SparkContext(appName="CalculateSurgeFactor")
  ssc = StreamingContext(sc, 30)


  brokers = sys.argv[1]
  topic = sys.argv[2]
  kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})


  pickups = kvs.map(lambda v: json.loads(v[1]))
  pickup_dstream = pickups.map(lambda pickup: pickup['pickup_geohash'])
  geoh_counts = pickup_dstream.countByValue().mapValues(lambda x: x[1])
  geoh_counts.pprint()


  ssc.start()
  ssc.awaitTermination()
