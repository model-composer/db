<?php namespace Model\Db\Events;

class ChangedTable
{
	public function __construct(public string $table)
	{
	}
}
