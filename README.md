# General mapping of Drupal Queue API to Kafka
## Data mapping

* Drupal queue → Kafka topic
  * mapped as 1 instance of Producer and 1 of low-level Consumer
  * n web servers → 2 * n partitions in topic
  * topics are pre-allocated during deployment to avoid possibility of 
    random topic generation at runtime (needed because Kafka is mostly used at
    large scale in tightened security environments)
* Drupal site → Kafka ConsumerGroup
 
## API mapping

* Queue operations
  * createQueue("foo"):
    * if topic exists in Kafka, mark as available in Drupal
    * else throw
    * do nothing in Kafka itself
  * $q->deleteQueue():
    * mark as deleted in Drupal
    * do nothing in Kafka
  * $q->numberOfItems() → not available (not meaningful)
* Item operations
  * $q->createItem → produce items until one is not in deleted list
  * $q->claimItem → save current offset, Consumer::consume(), store new offset
  * $q->deleteItem → do nothing in Kafka, add item offset to deleted list  
  * $q->releaseItem → do nothing in Kafka, rollback to saved offset
* Extra services
  * Cron: purge deleted lists from items no longer in Kafka
    * fetch offset of first item since start
    * delete lower offsets from deleted list
  * Deleted list: not deleted on queue "deletion", since it will still be needed
    if the queue is recreated under the same name, matching the same topic

## Issues

* High-level Consumer doesn't seem to work, at least not with the current 
  version (0.9.1) of librdkafka, the extension, and Kafka (kafka_2.11-0.10.1.0).
* Topic creation: many use cases in Drupal assume queue creation/deletion is 
  dynamic, but librdkafka does not support topic creation/deletion, it needs to
  have automatic creation enabled
* Topic deletion: deleted topics are not (immediately) available for re-creation 
  under the same name
* Minor: Latency at low data load: 80 msec local
  * But high throughput at high load: 350 k items/sec local
