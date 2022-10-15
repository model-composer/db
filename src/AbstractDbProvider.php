<?php namespace Model\Db;

abstract class AbstractDbProvider
{
	abstract public static function getMigrationsPaths(): array;

	public static function alterSelect(array $config, string $table, array $where, array $options): array
	{
		return [$where, $options];
	}
}
