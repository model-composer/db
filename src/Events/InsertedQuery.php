<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class InsertedQuery extends AbstractEvent
{
	public function __construct(public string $table, public ?int $id)
	{
	}

	public function getData(): array
	{
		return [
			'table' => $this->table,
			'id' => $this->id,
		];
	}
}
