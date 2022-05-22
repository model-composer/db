<?php namespace Model\Db;

use Model\Config\Config;

class Db
{
	/** @var DbConnection[] */
	private static array $connections = [];

	/**
	 * @param string $name
	 * @return DbConnection|null
	 * @throws \Exception
	 */
	public static function getConnection(string $name = 'primary'): ?DbConnection
	{
		if (isset(self::$connections[$name]))
			return self::$connections[$name];

		$config = self::getConfig();

		if (!isset($config['databases'][$name]))
			throw new \Exception('Db "' . $name . '" not found in config');

		self::$connections[$name] = new DbConnection($config['databases'][$name]);
		return self::$connections[$name];
	}

	/**
	 * @return DbConnection[]
	 */
	public static function getConnections(): array
	{
		return self::$connections;
	}

	/**
	 * Config retriever
	 *
	 * @return array
	 * @throws \Exception
	 */
	private static function getConfig(): array
	{
		return Config::get('db', [
			[
				'version' => '0.1.0',
				'migration' => function (array $config, string $env) {
					if ($config) // Already existing
						return $config;

					return [
						'databases' => [
							'primary' => [
								'host' => 'localhost',
								'port' => 3306,
								'username' => 'root',
								'password' => '',
								'name' => 'database',
							],
						],
					];
				},
			],
		]);
	}
}
