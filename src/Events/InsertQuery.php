<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class InsertQuery extends AbstractEvent
{
	public function __construct(public string $table, public array $data)
	{
	}

	public function getData(): array
	{
		return [
			'table' => $this->table,
			'data' => $this->data['data'] ?? null,
			'options' => $this->data['options'] ?? [],
		];
	}
}
