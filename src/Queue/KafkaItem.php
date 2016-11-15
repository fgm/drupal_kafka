<?php

namespace Drupal\kafka\Queue;

/**
 * Class KafkaItem is a lightweight object for items exchanged with Kafka.
 */
class KafkaItem {

  /**
   * The same as what what passed into createItem().
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var mixed
   */
  public $data;

  // @codingStandardsIgnoreStart
  /**
   * The unique ID returned from createItem().
   *
   * The non-camelCase name is required by QueueInterface.
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var string
   */
  public $item_id;
  // @codingStandardsIgnoreEnd

  /**
   * Timestamp when the item was put into the queue.
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var int
   */
  public $created;

  /**
   * KafkaItem constructor.
   *
   * @param mixed $data
   *   The payload. Must be serializable.
   * @param null|string $uuid
   *   Optional, but recommended for efficiency: the unique id to the item.
   * @param int|null $time
   *   Optional: the item submission time.
   */
  public function __construct($data, $uuid = NULL, $time = NULL) {
    // Items can be created in a loop, so we cannot rely on REQUEST_TIME.
    $this->created = $time ?: time();

    $this->item_id = $uuid ?: \Drupal::service('uuid')->generate();
    $this->data = $data;
  }

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    // Order the keys for better readability of the serialized format, making
    // it usable without the class if needed.
    $array = [
      'item_id' => $this->item_id,
      'created' => $this->created,
      'data' => $this->data,
    ];

    return serialize($array);
  }

  /**
   * Factory method.
   *
   * @param string $serialized
   *   The serialized representation of the item.
   *
   * @return static
   */
  public static function fromString($serialized) {
    $array = unserialize($serialized);
    return new static($array['data'], $array['item_id'], $array['created']);
  }

}
