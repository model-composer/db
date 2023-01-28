<?php namespace Model\Db\Events;

use Model\Events\AbstractEvent;

class ChangedTable extends AbstractEvent
{
	public function __construct(public string $table)
	{
	}

	public function getData(): array
	{
		return [
			'table' => $this->table,
		];
	}
}
