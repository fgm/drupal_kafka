<?php

namespace Drupal\kafka\Queue;

use Drupal\Component\Uuid\UuidInterface;
use Drupal\Core\Database\Connection;
use Drupal\Core\Database\Query\Merge;
use Drupal\Core\Queue\QueueGarbageCollectionInterface;
use Drupal\Core\Queue\QueueInterface;
use Drupal\Core\Queue\SuspendQueueException;
use Drupal\Core\Site\Settings;
use Drupal\Core\StringTranslation\TranslatableMarkup;
use Drupal\kafka\ClientFactory;

/**
 * Class KafkaQueue is a Drupal Queue backend.
 *
 * Queues match Kafka topics and have a DB representation:
 * - queues can be marked deleted: this does not affect Kafka
 * - queues can only be used is they have been created and are not deleted.
 * - queues are removed from Kafka and the DB on garbage collection.
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
   * @var bool|null
   */
  protected $isDeleted = NULL;

  /**
   * Is the queue created in the DB ?
   *
   * @var bool|null
   */
  protected $isExisting = NULL;

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

    $this->checkQueueStatus();
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
  public function claimItem($lease_time = 3600) {
    // Same unspecified behavior as core DatabaseQueue::claimItem().
    if (!$this->isExisting || $this->isDeleted) {
      return FALSE;
    }

    $consumer = $this->consumer();

    /** @var \RdKafka\Message $message */
    $message = $consumer->consume($this->clientFactory->consumerSettings('timeout', 100));

    switch ($message->err) {
      case RD_KAFKA_RESP_ERR_NO_ERROR:
        // TODO Do something with $message->offset for deleteItem / releaseItem.
        $item = KafkaItem::fromString($message->payload);
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
   * Lazy fetch a configured, subscribed high-level consumer.
   *
   * @see KafkaQueue::__destruct()
   *
   * @return \RdKafka\KafkaConsumer
   *   The fully ready consumer instance.
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
   * Check the queue status in the database and cache it on the instance.
   *
   * @return array
   *   - 0: does queue exist ?
   *   - 1: is queue marked as deleted ? Nonexistent queues are not.
   */
  protected function checkQueueStatus() {
    $table = static::TABLE;
    $sql = <<<SQL
SELECT deleted 
FROM {$table} q
WHERE q.name = :name
SQL;
    $deleted = $this->database
      ->query($sql, [':name' => $this->name])
      ->fetchField();

    if ($deleted === FALSE) {
      $this->isExisting = FALSE;
      $this->deleted = FALSE;
    }
    else {
      $this->isExisting = TRUE;
      $this->isDeleted = (bool) $deleted;
    }

    return [$this->isExisting, $this->isDeleted];
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
    if (!$this->isExisting || $this->isDeleted) {
      throw new \RuntimeException(new TranslatableMarkup(
        "Queue @name is not usable at this time." , ['@name' => $this->name]
      ));
    }

    $item = new KafkaItem($data, $this->uuid->generate());
    $this->producerTopic()->produce(RD_KAFKA_PARTITION_UA, 0, $item->__toString());
    return $item->item_id;
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

    $this->isExisting = TRUE;
    $this->isDeleted = FALSE;
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

    $this->isExisting = TRUE;
    $this->isDeleted = TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function garbageCollection() {
    // Purge deleted queues.
    $this->database->delete(static::TABLE)
      ->condition('deleted', 1)
      ->execute();

    // The delete query may have changed the DB for our queue... or not.
    $this->checkQueueStatus();
  }

  /**
   * {@inheritdoc}
   */
  public function numberOfItems() {
    // Information is not provided by Kafka, and 0 is a valid value.
    return 0;
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

}
