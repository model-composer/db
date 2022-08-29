<?php namespace Model\Db;

interface DbProviderInterface
{
	public static function getMigrationsPaths(): array;
}
