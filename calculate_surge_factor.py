###############
# Compare all the pickup geohash and return number of each geohash per batch
# eg : dr5ruk : 50 
# then return surge factor for this location based on demande 
# eg : dr5ruk : 1.6
# geoh_counts_sorted_dstream = geoh_counts.transform((lambda foo:foo.sortBy(lambda x:( -x[1]))))
# geoh_counts_sorted_dstream.pprint()  

%pyspark
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

ssc = StreamingContext(sc, 30)

brokers = "famous-gerbil-kf:9092"
topic = "pickups"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

pickups = kvs.map(lambda v: json.loads(v[1]))
pickup_dstream = pickups.map(lambda pickup: pickup['pickup_geohash'])
geoh_counts = pickup_dstream.countByValue().map(lambda x: (x[0], x[1], float(x[1]/10)/10+1))
geoh_counts.pprint()

ssc.start()
ssc.awaitTermination()
