<?php

namespace Drupal\kafka\Queue;

use Drupal\Component\Uuid\UuidInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\Queue\QueueGarbageCollectionInterface;
use Drupal\Core\Queue\QueueInterface;
use Drupal\Core\Site\Settings;
use Drupal\Core\StringTranslation\TranslatableMarkup;
use Drupal\kafka\ClientFactory;
use RdKafka\Conf;
use RdKafka\TopicConf;

/**
 * Class KafkaQueue is a Drupal Queue backend.
 */
class KafkaQueue implements QueueInterface, QueueGarbageCollectionInterface {
  const TABLE = 'kafka_queue';

  /**
   * The kafka.client_factory service.
   *
   * @var \Drupal\kafka\ClientFactory
   */
  protected $clientFactory;

  /**
   * A configured, subscribed consumer instance, or null.
   *
   * @var \RdKafka\KafkaConsumer
   */
  protected $consumer;

  /**
   * The consumer topic for the queue.
   *
   * @var \RdKafka\ConsumerTopic
   */
  protected $consumerTopic;

  /**
   * Is the queue deleted ?
   *
   * @var bool
   */
  protected $isDeleted = NULL;

  /**
   * The database service.
   *
   * @var \Drupal\Core\Database\Connection
   */
  protected $database;

  /**
   * The queue name.
   *
   * @var string
   */
  protected $name;

  /**
   * The producer topic for the queue.
   *
   * @var \RdKafka\ProducerTopic
   */
  protected $producerTopic;

  /**
   * The available topics.
   *
   * @var string[]
   */
  protected $topics = [];

  /**
   * The uuid service.
   *
   * @var \Drupal\Component\Uuid\UuidInterface
   */
  protected $uuid;

  /**
   * KafkaQueue constructor.
   *
   * @param string $name
   *   The queue/topic name.
   * @param \Drupal\Core\Database\Connection $database
   *   The database service.
   * @param \Drupal\kafka\ClientFactory $clientFactory
   *   The kafka.client_factory service.
   * @param \Drupal\Core\Site\Settings $settings
   *   The settings service.
   * @param \Drupal\Component\Uuid\UuidInterface $uuid
   *   The uuid service.
   */
  public function __construct($name, Connection $database, ClientFactory $clientFactory, Settings $settings, UuidInterface $uuid) {
    $this->clientFactory = $clientFactory;
    $this->database = $database;
    $this->name = $name;
    $this->uuid = $uuid;
  }

  /**
   * Warn Kafka that we unsubscribe.
   */
  public function __destruct() {
    if (isset($this->consumer)) {
      $this->consumer->unsubscribe();
    }
  }

  /**
   * {@inheritdoc}
   */
  public function garbageCollection() {
    // Purge deleted queues.
    $this->database->delete(static::TABLE)
      ->condition('deleted', 1)
      ->execute();
  }

  /**
   * @return \RdKafka\KafkaConsumer
   */
  protected function consumer() {
    if (!isset($this->consumer)) {
      $this->consumer = $this->clientFactory->highLevelConsumer();
      $this->consumer->subscribe([$this->name]);
    }
    return $this->consumer;
  }

  /**
   * Lazy fetch the consumer topic for the queue.
   *
   * @return \RdKafka\ConsumerTopic
   *   The topic.
   */
  protected function consumerTopic() {
    if (!isset($this->consumerTopic)) {
      $this->consumerTopic = $this
        ->clientFactory
        ->highLevelConsumer()
        ->newTopic($this->name);
    }

    return $this->consumerTopic;
  }

  /**
   * Adds a queue item and store it directly to the queue.
   *
   * @param mixed $data
   *   Arbitrary data to be associated with the new task in the queue.
   *
   * @return false|string
   *   A unique ID if the item was successfully created and was (best effort)
   *   added to the queue, otherwise FALSE. We don't guarantee the item was
   *   committed to disk etc, but as far as we know, the item is now in the
   *   queue.
   */
  public function createItem($data) {
    $item = new KafkaItem([
      'created' => time(),
      'data' => $data,
      'item_id' => $uuid = $this->uuid->generate(),
    ]);
    $stringItem = json_encode($item);
    $this->producerTopic()->produce(RD_KAFKA_PARTITION_UA, 0, $stringItem);
    return $uuid;
  }

  /**
   * Retrieves the number of items in the queue.
   *
   * This is intended to provide a "best guess" count of the number of items in
   * the queue. Depending on the implementation and the setup, the accuracy of
   * the results of this function may vary.
   *
   * e.g. On a busy system with a large number of consumers and items, the
   * result might only be valid for a fraction of a second and not provide an
   * accurate representation.
   *
   * @return int
   *   An integer estimate of the number of items in the queue: 0.
   */
  public function numberOfItems() {
    return 0;
  }

  /**
   * {@inheritdoc}
   */
  public function claimItem($lease_time = 3600) {
    $consumer = $this->consumer();

    /** @var \RdKafka\Message $message */
    $message = $consumer->consume($this->clientFactory->consumerSettings('timeout', 100));

    switch ($message->err) {
      case RD_KAFKA_RESP_ERR_NO_ERROR:
        // TODO Do something with $message->offset for deleteItem / releaseItem.
        $item = json_decode($message->payload);
        $error = FALSE;
        break;

      case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        // No more messages; will not wait for more.
        $error = TRUE;
        break;

      case RD_KAFKA_RESP_ERR__TIMED_OUT:
        // Timed out.
        $error = TRUE;
        break;

      default:
        // We should really throw, but the Queue API says not to do it:
        // throw new \Exception($message->errstr(), $message->err);
        // so we just return an error.
        $error = TRUE;
    }

    $ret = $error ? FALSE : $item;
    return $ret;
  }

  /**
   * Deletes a finished item from the queue.
   *
   * @param array $item
   *   The item returned by \Drupal\Core\Queue\QueueInterface::claimItem().
   */
  public function deleteItem($item) {
    // TODO: Implement deleteItem() method.
  }

  /**
   * Lazy fetch the product topic for the queue.
   *
   * @return \RdKafka\ProducerTopic
   *   The topic.
   */
  protected function producerTopic() {
    if (!isset($this->producerTopic)) {
      $this->producerTopic = $this
        ->clientFactory
        ->producer()
        ->newTopic($this->name);
    }

    return $this->producerTopic;
  }

  /**
   * Releases an item that the worker could not process.
   *
   * Another worker can come in and process it before the timeout expires.
   *
   * @param object $item
   *   The item returned by \Drupal\Core\Queue\QueueInterface::claimItem().
   *
   * @return bool
   *   TRUE if the item has been released, FALSE otherwise.
   */
  public function releaseItem($item) {
    return FALSE;
  }

  /**
   * {@inheritdoc}
   *
   * Reject queues not matching a Kafka topic.
   *
   * Idempotent: it is not an error to create the queue repeatedly.
   */
  public function createQueue() {
    $name = $this->name;

    $topics = $this->clientFactory->getTopics()['topics'];
    if (!in_array($name, $topics)) {
      throw new \DomainException(new TranslatableMarkup('Failed creating queue @name, which does not match an existing topic.', ['@name' => $name]));
    }

    $this->database
      ->merge(static::TABLE)
      ->key('name', $name)
      ->fields([
        'deleted' => 0,
      ])
      ->execute();
  }

  /**
   * {@inheritdoc}
   *
   * Idempotent: it is not an error to delete the queue repeatedly, or delete
   * a non-existent queue: it will be created as deleted.
   *
   * One of the side effects of deleting a queue is its creation without
   * checking whether the topic exists in Kafka. This is not an issue, as it
   * will not possible to use it for items until createQueue() is invoked, which
   * will check whether the topic exists when it will actually be likely to be
   * used.
   */
  public function deleteQueue() {
    $this->database
      ->merge(static::TABLE)
      ->key('name', $this->name)
      ->fields([
        'deleted' => 1,
      ])
      ->execute();
  }

}
