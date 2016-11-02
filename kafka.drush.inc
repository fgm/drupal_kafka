<?php

/**
 * @file
 * Drush plugin for Kafka module.
 */

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicConf;
use Symfony\Component\Yaml\Yaml;

/**
 * Implements hook_drush_command().
 */
function kafka_drush_command() {
  $file = preg_replace('/(inc|php)$/', 'yml', __FILE__);
  $config = Yaml::parse(file_get_contents($file));
  $items = $config['commands'];
  return $items;
}

/**
 * Drush callback for "kafka-high-level-consumer-demo".
 *
 * @throws \Exception
 */
function drush_kafka_high_level_consumer_demo() {
  $conf = new Conf();
  $t0 = time();
  // Set a rebalance callback to log partition assignemts (optional)
  $conf->setRebalanceCb(function (
    KafkaConsumer $kafka,
    $err,
    array $partitions = NULL
  ) {
    switch ($err) {
      case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        echo "Assign: ";
        var_dump($partitions);
        $kafka->assign($partitions);
        break;

      case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        echo "Revoke: ";
        var_dump($partitions);
        $kafka->assign(NULL);
        break;

      default:
        throw new \Exception($err);
    }
  });

  // Configure the group.id. All consumer with the same group.id will consume
  // different partitions.
  $conf->set('group.id', 'myConsumerGroup');

  /* Initial list of Kafka brokers. Not needed with the Drupal service.
  $conf->set('metadata.broker.list', '127.0.0.1');
   */
  $topicConf = new TopicConf();

  // Set where to start consuming messages when there is no initial offset in
  // offset store or the desired offset is out of range.
  // 'smallest': start from the beginning.
  $topicConf->set('auto.offset.reset', 'smallest');

  // Set the configuration to use for subscribed/assigned topics.
  $conf->setDefaultTopicConf($topicConf);

  /** @var \Drupal\kafka\ClientFactory $clientFactory */
  $clientFactory = \Drupal::service('kafka.client_factory');
  $consumer = $clientFactory->create('high', $conf);

  // Subscribe to topic 'drupal'.
  $consumer->subscribe(['drupal']);

  echo "Waiting for partition assignment... (make take some time when\n";
  echo "quickly re-joining the group after leaving it.)\n";

  echo ($t1 = time()) . "\n";
  $count = 0;
  $continue = TRUE;
  while ($continue) {
    $message = $consumer->consume(120 * 1000);
    switch ($message->err) {
      case RD_KAFKA_RESP_ERR_NO_ERROR:
        $count++;
        break;

      case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        echo "No more messages; will not wait for more\n";
        $continue = FALSE;
        break;

      case RD_KAFKA_RESP_ERR__TIMED_OUT:
        echo "Timed out\n";
        $continue = FALSE;
        break;

      default:
        throw new \Exception($message->errstr(), $message->err);
    }
  }

  echo "count: $count\n";
  echo ($t2 = time()) . "\n";
  echo "setup: " . ($t1 - $t0) . " sec, delay: " . ($t2 - $t1) . "\n";
}

/**
 * Drush callback for "kafka-low-level-consumer-demo".
 */
function drush_kafka_low_level_consumer_demo() {
  $rk = \Drupal::service('kafka.low_level_consumer');
  $rk->setLogLevel(LOG_WARNING);
  $debug = FALSE;

  $topic = $rk->newTopic('drupal');

  // The first argument is the partition to consume from.
  // The second argument is the offset at which to start consumption. Valid
  // values are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END,
  // RD_KAFKA_OFFSET_STORED.
  $topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

  echo ($t0 = time()) . "\n";
  $count = 0;
  while (TRUE) {
    $count++;
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume(0, 30);
    if ($msg->err) {
      echo $msg->errstr(), "\n";
      break;
    }
    elseif ($debug) {
      drush_print('Message: [' . $msg->payload . "]");
    }
  }
  echo "count: $count\n";
  echo ($t1 = time()) . "\n";
  echo "delay: " . ($t1 - $t0) . "\n";
}

/**
 * Drush callback for "kafka-producer-demo".
 */
function drush_kafka_producer_demo() {
  $rk = \Drupal::service('kafka.producer');
  $rk->setLogLevel(LOG_WARNING);
  $topic = $rk->newTopic('drupal');

  $max = 1E6;
  $t0 = microtime(TRUE);
  for ($i = 1; $i <= $max; $i++) {
    if ($i % ($max / 10) === 0) {
      echo "$i\t";
    }
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message $i (" . time() . ")");
  }
  $t1 = microtime(TRUE);
  $rate = round($max / ($t1 - $t0));
  echo "\n$max messages @ $rate msg/sec\n";
}

/**
 * Drush callback for "kafka-topics".
 */
function drush_kafka_topics() {
  /** @var \Drupal\kafka\ClientFactory $clientFactory */
  $clientFactory = \Drupal::service('kafka.client_factory');
  $topics = ['topics' => $clientFactory->getTopics()];
  $yamlTopics = Yaml::dump($topics);
  drush_print($yamlTopics);
}
