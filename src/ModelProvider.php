<?php namespace Model\Db;

use Model\Core\AbstractModelProvider;

class ModelProvider extends AbstractModelProvider
{
	public static function realign(): void
	{
		Db::migrate();
	}
}
