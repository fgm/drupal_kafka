<?php

namespace Drupal\kafka;

use Drupal\Core\Site\Settings;
use RdKafka\Conf;

class ClientFactory {
  /**
   * The Kafka settings.
   *
   * @var array
   */
  protected $settings;

  public function __construct(Settings $settings) {
    $this->settings = $settings->get('kafka');
  }

  public function create($type, Conf $conf = null) {
    switch ($type) {
      case 'low':
        $class = 'RdKafka\Consumer';
        $section = 'consumer';
        break;

      case 'high':
        $class = 'RdKafka\KafkaConsumer';
        $section = 'consumer';
        if (!isset($conf)) {
          $conf = new Conf();
        }
        $conf->set('metadata.broker.list', implode(',', $this->settings[$section]['brokers']));
        break;

      case 'producer':
        $class = 'RdKafka\Producer';
        $section = 'producer';
        break;

      default:
        throw new \InvalidArgumentException("Invalid client type.");
    }

    $client = isset($conf) ? new $class($conf) : new $class();

    // KafkaConsumer uses $conf exclusively. See switch() above.
    if (method_exists($client, 'addBrokers')) {
      $client->addBrokers(implode(',', $this->settings[$section]['brokers']));
    }
    return $client;
  }

  /**
   * Return visible topics in the Kafka instance.
   *
   * @return string[]
   *   An array of topic names. May include system topics like
   *   __consumer_offsets.
   */
  public function getTopics() {
    $topics = [];

    $sources = [
      $this->create('producer'),
      $this->create('low'),
    ];

    foreach ($sources as $rdk) {
      $rdk->setLogLevel(LOG_INFO);

      /** @var \RdKafka\Metadata $meta */
      $meta = $rdk->getMetadata(TRUE, null, 100);

      /** @var \RdKafka\Metadata\Collection $topics */
      $rkTopics = $meta->getTopics();

      /** @var \RdKafka\Metadata\Topic $topic */
      foreach ($rkTopics as $topic) {
        $topics[$topic->getTopic()] = TRUE;
      }
    }
    $topics = array_keys($topics);
    sort($topics);
    return $topics;
  }

}
