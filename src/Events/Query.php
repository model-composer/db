<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class Query extends AbstractEvent
{
	public function __construct(public string $query, public ?string $table)
	{
	}

	public function getData(): array
	{
		return [
			'query' => $this->query,
			'table' => $this->table,
		];
	}
}
