<?php

namespace Drupal\kafka\Queue;


class KafkaItem {

  /**
   * The same as what what passed into createItem().
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var mixed
   */
  public $data;

  /**
   * The unique ID returned from createItem().
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var string
   */
  public $item_id;

  /**
   * Timestamp when the item was put into the queue.
   *
   * @see \Drupal\Core\Queue\QueueInterface::claimItem()
   *
   * @var int
   */
  public $created;

  public function __construct() {
  }
}
