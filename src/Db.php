<?php namespace Model\Db;

use Model\Config\Config;
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
	 * Executes migrations
	 *
	 * @return void
	 */
	public static function migrate(): void
	{
		$config = self::getConfig();
		foreach ($config['databases'] as $databaseName => $database) {
			if (!$database['migrations_folder'])
				continue;

			$appRoot = self::getProjectRoot();
			if (!is_dir($appRoot . $database['migrations_folder']))
				mkdir($appRoot . $database['migrations_folder']);

			$phinxConfig = [
				'paths' => [
					'migrations' => $appRoot . '/migrations',
				],
				'environments' => [
					"production" => [
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

			$phinxConfigFile = $appRoot . $database['migrations_folder'] . DIRECTORY_SEPARATOR . 'tmp_config_' . $databaseName . '.php';
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
			[
				'version' => '0.3.0',
				'migration' => function (array $config, string $env) {
					foreach ($config['databases'] as &$database)
						$database['migrations_folder'] = 'migrations';

					return $config;
				},
			],
		]);
	}
}
