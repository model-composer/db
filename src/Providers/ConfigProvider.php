<?php namespace Model\Db\Providers;

use Model\Config\AbstractConfigProvider;

class ConfigProvider extends AbstractConfigProvider
{
	public static function migrations(): array
	{
		return [
			[
				'version' => '0.1.0',
				'migration' => function (array $config, string $env) {
					if ($config) // Already existing
						return $config;

					if (defined('INCLUDE_PATH') and file_exists(INCLUDE_PATH . 'app/config/Db/config.php')) {
						// ModEl 3 migration
						require(INCLUDE_PATH . 'app/config/Db/config.php');

						foreach ($config['databases'] as &$databaseConfig) {
							$databaseConfig['port'] = 3306;
							$databaseConfig['name'] = $databaseConfig['database'];
							unset($databaseConfig['database']);
						}

						return $config;
					}

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
			[
				'version' => '0.3.0',
				'migration' => function (array $config, string $env) {
					foreach ($config['databases'] as &$database)
						$database['migrations_folder'] = 'migrations';

					return $config;
				},
			],
			[
				'version' => '0.4.0',
				'migration' => function (array $config, string $env) {
					foreach ($config['databases'] as &$database) {
						$database['migrations'] = [
							$database['migrations_folder'],
						];
						unset($database['migrations_folder']);
					}

					return $config;
				},
			],
			[
				'version' => '0.5.6',
				'migration' => function (array $config, string $env) {
					foreach ($config['databases'] as &$database) {
						if (isset($database['linked-tables'])) {
							$database['linked_tables'] = $database['linked-tables'];
							unset($database['linked-tables']);
						}
					}

					return $config;
				},
			],
			[
				'version' => '0.9.10',
				'migration' => function (array $config, string $env) {
					foreach ($config['databases'] as &$database)
						$database['charset'] = 'utf8';

					return $config;
				},
			],
		];
	}

	public static function templating(): array
	{
		return [
			'databases.*.host',
			'databases.*.port',
			'databases.*.username',
			'databases.*.password',
			'databases.*.name',
		];
	}
}
