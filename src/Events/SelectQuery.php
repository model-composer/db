<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class SelectQuery extends AbstractEvent
{
	public function __construct(public string $table, public array $data)
	{
	}

	public function getData(): array
	{
		return [
			'table' => $this->table,
			'where' => $this->data['where'] ?? null,
			'options' => $this->data['options'] ?? [],
		];
	}
}
