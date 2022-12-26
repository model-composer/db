<?php namespace Model\Db\Providers;

use Model\Core\AbstractModelProvider;
use Model\Db\Db;

class ModelProvider extends AbstractModelProvider
{
	public static function realign(): void
	{
		Db::migrate();
	}
}
