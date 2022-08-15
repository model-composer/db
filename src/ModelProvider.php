<?php namespace Model\Db;

use Model\Core\ModelProviderInterface;

class ModelProvider implements ModelProviderInterface
{
	public static function realign(): void
	{
		Db::migrate();
	}

	public static function getDependencies(): array
	{
		return [];
	}
}
