<?php namespace Model\Db;

use Model\DbParser\Table;

abstract class AbstractDbProvider
{
	public static function getMigrationsPaths(): array
	{
		return [];
	}

	public static function linkedTables(): array
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
