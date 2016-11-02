<?php

namespace Drupal\kafka;

use Drupal\Core\Site\Settings;
use RdKafka\Conf;

/**
 * Class ClientFactory builds instances of producers and consumers.
 */
class ClientFactory {
  const METADATA_TIMEOUT = 1000;

  /**
   * The Kafka settings.
   *
   * @var array
   */
  protected $settings;

  /**
   * ClientFactory constructor.
   *
   * @param \Drupal\Core\Site\Settings $settings
   *   The core settings service.
   */
  public function __construct(Settings $settings) {
    $this->settings = $settings->get('kafka');
  }

  /**
   * The client factory method.
   *
   * @param string $type
   *   Strings "low" and "high" for consumers, "producer" for producers.
   * @param \RdKafka\Conf|null $conf
   *   Optional. A configuration object. For high-level consumers, this is the
   *   only way to assign broker IP from settings.
   *
   * @return \RdKafka\Consumer|\RdKafka\KafkaConsumer|\RdKafka\Producer
   *   A client instance.
   */
  public function create($type, Conf $conf = NULL) {
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
      $meta = $rdk->getMetadata(TRUE, NULL, self::METADATA_TIMEOUT);

      /** @var \RdKafka\Metadata\Collection $topics */
      $rkTopics = $meta->getTopics();

      /** @var \RdKafka\Metadata\Topic $topic */
      foreach ($rkTopics as $topic) {
        $topics[$topic->getTopic()] = NULL;
      }
    }
    $topics = array_keys($topics);
    sort($topics);
    return $topics;
  }

}
