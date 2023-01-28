<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class UpdateQuery extends AbstractEvent
{
	public function __construct(public string $table, public array $data)
	{
	}

	public function getData(): array
	{
		return [
			'table' => $this->table,
			'where' => $this->data['where'] ?? null,
			'data' => $this->data['data'] ?? null,
			'options' => $this->data['options'] ?? [],
		];
	}
}
