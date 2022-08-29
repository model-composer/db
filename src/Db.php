<?php namespace Model\Db;

use Model\Config\Config;
use Model\ProvidersFinder\Providers;
use Phinx\Console\PhinxApplication;
use Phinx\Wrapper\TextWrapper;

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
	 * @param string $db
	 * @param array $dbConfig
	 * @return void
	 */
	public static function injectDatabase(string $db, array $dbConfig): void
	{
		$config = self::getConfig();
		if (isset($config['databases'][$db]))
			throw new \Exception('Db ' . $db . ' already existing');

		$config['databases'][$db] = $dbConfig;
		Config::set('db', $config);
	}

	/**
	 * @param string $db
	 * @return void
	 * @throws \Exception
	 */
	public static function removeDatabase(string $db): void
	{
		$config = self::getConfig();
		if (isset($config['databases'][$db]))
			unset($config['databases'][$db]);
		Config::set('db', $config);
	}

	/**
	 * Executes migrations
	 *
	 * @return void
	 */
	public static function migrate(): void
	{
		$packagesWithProvider = Providers::find('DbProvider');

		$config = self::getConfig();
		foreach ($config['databases'] as $databaseName => $database) {
			$paths = $database['migrations'] ?: [];
			foreach ($paths as &$path) {
				$path = self::getProjectRoot() . $path;
				if (!is_dir($path))
					mkdir($path);
			}
			unset($path);

			foreach ($packagesWithProvider as $package) {
				$packageMigrations = $package['provider']::getMigrationsPaths();
				foreach ($packageMigrations as $packageMigration) {
					if (
						(empty($packageMigration['dbs']) or in_array($databaseName, $packageMigration['dbs']))
						and (!$packageMigration['except'] or !in_array($databaseName, $packageMigration['except']))
					)
						$paths[] = $packageMigration['path'];
				}
			}

			if (count($paths) === 0)
				continue;

			$phinxConfig = [
				'paths' => [
					'migrations' => array_unique($paths),
				],
				'environments' => [
					'production' => [
						'adapter' => 'mysql',
						'host' => $database['host'],
						'port' => $database['port'],
						'name' => $database['name'],
						'user' => $database['username'],
						'pass' => $database['password'],
						'charset' => 'utf8',
					],
				],
			];

			$phinxConfigFile = tempnam(sys_get_temp_dir(), 'tmp_config_' . $databaseName . '.php');
			file_put_contents($phinxConfigFile, "<?php\nreturn " . var_export($phinxConfig, true) . ";\n");

			$app = new PhinxApplication();
			$wrap = new TextWrapper($app);
			$wrap->setOption('configuration', $phinxConfigFile);
			$err = $wrap->getMigrate();
			if ($wrap->getExitCode() !== 0)
				throw new \Exception('Errors while migrating: ' . $err);

			unlink($phinxConfigFile);
		}
	}

	/**
	 * Retrieves project root
	 *
	 * @return string
	 */
	private static function getProjectRoot(): string
	{
		return realpath(__DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . '..') . DIRECTORY_SEPARATOR;
	}

	/**
	 * Config retriever
	 *
	 * @return array
	 * @throws \Exception
	 */
	public static function getConfig(): array
	{
		return Config::get('db', [
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
		], [
			'databases.*.host',
			'databases.*.port',
			'databases.*.username',
			'databases.*.password',
			'databases.*.name',
		]);
	}
}
