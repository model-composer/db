<?php namespace Model\Db;

use Model\DbParser\Table;
use Model\ProvidersFinder\AbstractProvider;

abstract class AbstractDbProvider extends AbstractProvider
{
	public static function getMigrationsPaths(): array
	{
		return [];
	}

	public static function alterUpdate(DbConnection $db, array $queries): array
	{
		return $queries;
	}

	public static function alterDelete(DbConnection $db, string $table, array|int $where, array $options): array
	{
		return [$where, $options];
	}

	public static function alterSelect(DbConnection $db, string $table, array|int $where, array $options): array
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
