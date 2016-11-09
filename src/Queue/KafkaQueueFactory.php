<?php

namespace Drupal\kafka\Queue;

use Drupal\Core\KeyValueStore\KeyValueFactoryInterface;
use Drupal\Core\KeyValueStore\KeyValueStoreInterface;
use Drupal\Core\Site\Settings;
use Drupal\kafka\ClientFactory;

/**
 * Class KafkaQueueFactory is the queue.kafka implementation.
 */
class KafkaQueueFactory {

  /**
   * The kafka.client_factory service.
   *
   * @var \Drupal\kafka\ClientFactory
   */
  protected $clientFactory;

  /**
   * The keyvalue service.
   *
   * @var \Drupal\Core\KeyValueStore\KeyValueStoreInterface
   */
  protected $kv;

  /**
   * The settings service.
   *
   * @var \Drupal\Core\Site\Settings
   */
  protected $settings;

  /**
   * KafkaQueue constructor.
   *
   * @param \Drupal\Core\KeyValueStore\KeyValueFactoryInterface $kv
   *   The keyvalue service.
   * @param \Drupal\kafka\ClientFactory $clientFactory
   *   The kafka.client_factory service.
   * @param \Drupal\Core\Site\Settings $settings
   *   The settings service.
   */
  public function __construct(
    ClientFactory $clientFactory,
    KeyValueFactoryInterface $kv,
    Settings $settings) {
      $this->clientFactory = $clientFactory;
      $this->kv = $kv;
      $this->settings = $settings;
    }


  /**
   * Return a queue from its name.
   *
   * @param string $name
   *   The queue name
   *
   * @return \Drupal\Core\Queue\QueueInterface
   *   The queue instance
   */
  public function get($name) {
    $queue = new KafkaQueue($name, $this->kv, $this->clientFactory, $this->settings);
    return $queue;
  }

}
