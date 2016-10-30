<?php

use Symfony\Component\Yaml\Yaml;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\Producer;
use RdKafka\TopicConf;

function kafka_drush_command() {
  $file = preg_replace('/(inc|php)$/', 'yml', __FILE__);
  $config = Yaml::parse(file_get_contents($file));
  $items = $config['commands'];
  return $items;
}

function drush_kafka_high_level_consumer_demo() {
  $conf = new RdKafka\Conf();
  $t0 = microtime(TRUE);
  // Set a rebalance callback to log partition assignemts (optional)
  $conf->setRebalanceCb(function (
    RdKafka\KafkaConsumer $kafka,
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

// Initial list of Kafka brokers
  $conf->set('metadata.broker.list', '127.0.0.1');

  $topicConf = new RdKafka\TopicConf();

  // Set where to start consuming messages when there is no initial offset in
  // offset store or the desired offset is out of range.
  // 'smallest': start from the beginning
  $topicConf->set('auto.offset.reset', 'smallest');

  // Set the configuration to use for subscribed/assigned topics
  $conf->setDefaultTopicConf($topicConf);

  $consumer = new RdKafka\KafkaConsumer($conf);

  // Subscribe to topic 'test'
  $consumer->subscribe(['drupal']);

  echo "Waiting for partition assignment... (make take some time when\n";
  echo "quickly re-joinging the group after leaving it.)\n";

  $continue = TRUE;
  while ($continue) {
    $message = $consumer->consume(120 * 1000);
    switch ($message->err) {
      case RD_KAFKA_RESP_ERR_NO_ERROR:
        //var_dump($message);
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
        break;
    }
  }

  $t1 = microtime(TRUE);
  echo ($t1 - $t0);
}

function drush_kafka_low_level_consumer_demo() {
  $rk = new Consumer();
  $rk->setLogLevel(LOG_WARNING);
  //$rk->addBrokers('10.176.178.39,127.0.0.1,::1');
  $rk->addBrokers('127.0.0.1');

  print_r($rk->metadata(TRUE, NULL, 100));
  $topic = $rk->newTopic('drupal');

  // The first argument is the partition to consume from.
  // The second argument is the offset at which to start consumption. Valid values
  // are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, RD_KAFKA_OFFSET_STORED.
  $topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);
  //$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
  //$topic->consumeStart(0, RD_KAFKA_OFFSET_END);

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
    else {
      // echo 'Message: [' . $msg->payload . "]\n";
    }
  }
  echo "count: $count\n";
  echo ($t1 = time()) . "\n";
  echo "delay: " . ($t1 - $t0) . "\n";
}

function drush_kafka_producer_demo() {
  $rk = new Producer();
  $rk->setLogLevel(LOG_DEBUG);
  $rk->addBrokers("127.0.0.1");
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
