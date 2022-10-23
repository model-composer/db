<?php namespace Model\Db;

use Model\DbParser\Table;
use Model\ProvidersFinder\AbstractProvider;

abstract class AbstractDbProvider extends AbstractProvider
{
	public static function getMigrationsPaths(): array
	{
		return [];
	}

	public static function alterSelect(DbConnection $db, string $table, array $where, array $options): array
	{
		return [$where, $options];
	}

	public static function alterSelectResult(DbConnection $db, string $table, array $row, array $options): array
	{
		return $row;
	}

	public static function alterTableModel(DbConnection $db, string $table, Table $tableModel): Table
	{
		return $tableModel;
	}
}
