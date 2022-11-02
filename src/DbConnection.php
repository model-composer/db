<?php namespace Model\Db;

use Model\Cache\Cache;
use Model\DbParser\Parser;
use Model\DbParser\Table;
use Model\ProvidersFinder\Providers;
use Model\QueryBuilder\QueryBuilder;

class DbConnection
{
	private array $config;
	private \PDO $db;
	private Parser $parser;
	private QueryBuilder $builder;

	private array $inMemoryCache = [];

	private int $c_transactions = 0;
	private array $query_counters = [
		'query' => [],
		'table' => [],
		'total' => 0,
	];

	/** @var Table[] */
	private array $tablesCache = [];

	protected array $deferedInserts = [];

	public function __construct(private readonly string $name, array $config)
	{
		$this->config = array_merge([
			'host' => 'localhost',
			'port' => 3306,
			'user' => 'root',
			'password' => '',
			'name' => 'database',
			'limits' => [
				'query' => 100,
				'table' => 10000,
				'total' => null,
			],
		], $config);

		$this->db = new \PDO('mysql:host=' . $this->config['host'] . ':' . $this->config['port'] . ';dbname=' . $this->config['name'] . ';charset=utf8', $this->config['username'], $this->config['password'], [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
			\PDO::ATTR_STRINGIFY_FETCHES => false,
		]);

		$this->parser = new Parser($this->db, $this->config['host'] . '.' . $this->config['name']);
		$this->builder = new QueryBuilder($this->parser);
	}

	/**
	 * @return string
	 */
	public function getName(): string
	{
		return $this->name;
	}

	/**
	 * @return array
	 */
	public function getConfig(): array
	{
		return $this->config;
	}

	/**
	 * @return Parser
	 */
	public function getParser(): Parser
	{
		return $this->parser;
	}

	/**
	 * @return QueryBuilder
	 */
	public function getBuilder(): QueryBuilder
	{
		return $this->builder;
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return int|null
	 */
	public function insert(string $table, array $data = [], array $options = []): ?int
	{
		$options = array_merge([
			'defer' => null,
		], $options);

		if ($options['defer'] !== null) {
			if ($options['defer'] === true)
				$options['defer'] = 0;
			if (!is_numeric($options['defer']))
				throw new \Exception('Invalid defer value');
			$options['defer'] = (int)$options['defer'];

			if (!isset($this->deferedInserts[$table])) {
				$this->deferedInserts[$table] = [
					'options' => $options,
					'rows' => [],
				];
			}

			if ($this->deferedInserts[$table]['options'] !== $options)
				throw new \Exception('Cannot defer inserts with different options on the same table');

			$this->deferedInserts[$table]['rows'][] = $data['data'];
			if ($options['defer'] > 0 and count($this->deferedInserts[$table]['rows']) >= $options['defer'])
				$this->bulkInsert($table);

			return null;
		}

		$qry = $this->builder->insert($table, $data, $options);

		if (!$this->inTransaction())
			$this->beginTransaction();

		$this->query($qry, $table, 'INSERT', $options);
		return $this->db->lastInsertId();
	}

	/**
	 * @param string $table
	 * @return void
	 */
	private function bulkInsert(string $table): void
	{
		if (!isset($this->deferedInserts[$table]))
			return;

		$options = $this->deferedInserts[$table]['options'];
		$options['bulk'] = true;

		$qry = $this->builder->insert($table, $this->deferedInserts[$table]['rows'], $options);
		if ($qry) {
			if (!$this->inTransaction())
				$this->beginTransaction();

			$this->query($qry, $table, 'INSERT', $options);
		}

		unset($this->deferedInserts[$table]);
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $data
	 * @param array $options
	 * @return \PDOStatement|null
	 */
	public function update(string $table, array|int $where = [], array $data = [], array $options = []): ?\PDOStatement
	{
		if (isset($this->deferedInserts[$table]))
			throw new \Exception('There are open bulk inserts on the table ' . $table . '; can\'t update');

		if (empty($where) and !($options['confirm'] ?? false))
			throw new \Exception('Tried to update full table without explicit confirm');

		$queries = [
			[
				'table' => $table,
				'where' => $where,
				'data' => $data,
				'options' => $options,
			],
		];

		$providers = Providers::find('DbProvider');
		foreach ($providers as $provider)
			$queries = $provider['provider']::alterUpdate($this, $queries);

		$response = null;
		foreach ($queries as $queryData) {
			$qry = $this->builder->update($queryData['table'], $queryData['where'], $queryData['data'], $queryData['options']);
			if ($qry === null)
				continue;

			if (!$this->inTransaction())
				$this->beginTransaction();

			$queryResponse = $this->query($qry, $table, 'UPDATE', $options);
			if ($response === null) // First one
				$response = $queryResponse;
		}

		return $response;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return \PDOStatement|null
	 */
	public function delete(string $table, array|int $where = [], array $options = []): ?\PDOStatement
	{
		if (isset($this->deferedInserts[$table]))
			throw new \Exception('There are open bulk inserts on the table ' . $table . '; can\'t delete');

		if (empty($where) and !($options['confirm'] ?? false))
			throw new \Exception('Tried to delete full table without explicit confirm');

		$providers = Providers::find('DbProvider');
		foreach ($providers as $provider)
			[$where, $options] = $provider['provider']::alterDelete($this, $table, $where, $options);

		$qry = $this->builder->delete($table, $where, $options);

		if (!$this->inTransaction())
			$this->beginTransaction();

		try {
			return $this->query($qry, $table, 'DELETE', $options);
		} catch (\Exception $e) {
			$message = $e->getMessage();
			if (stripos($message, 'a foreign key constraint fails') !== false) {
				preg_match_all('/`([^`]+?)`, CONSTRAINT `(.+?)` FOREIGN KEY \(`(.+?)`\) REFERENCES `(.+?)` \(`(.+?)`\)/i', $message, $matches, PREG_SET_ORDER);

				if (count($matches[0]) == 6) {
					$fk = $matches[0];
					$message = 'You\'re trying to delete a row from table "<b>' . $fk[4] . '</b>" that is referenced in the table "<b>' . $fk[1] . '</b>", in "<b>' . $fk[3] . '</b>" field';
					throw new \Exception($message);
				}
			}

			throw $e;
		}
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return array|null
	 */
	public function select(string $table, array|int $where = [], array $options = []): ?array
	{
		$options['limit'] = 1;
		$options['stream'] = false;
		$response = $this->selectAll($table, $where, $options);
		return $response ? $response[0] : null;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return iterable
	 */
	public function selectAll(string $table, array|int $where = [], array $options = []): iterable
	{
		if (isset($this->deferedInserts[$table]))
			throw new \Exception('There are open bulk inserts on the table ' . $table . '; can\'t read');

		$cacheable = $this->isSelectCacheable($table, $where, $options);
		if ($cacheable)
			return $this->selectFromCache($table, $where, $options);

		$providers = Providers::find('DbProvider');
		foreach ($providers as $provider)
			[$where, $options] = $provider['provider']::alterSelect($this, $table, $where, $options);

		$qry = $this->builder->select($table, $where, $options);

		$response = $this->query($qry, $table, 'SELECT', $options);

		$results = $this->streamResults($table, $response, $options, $providers);

		if ($options['stream'] ?? true) {
			return $results;
		} else {
			$resultsArr = [];
			foreach ($results as $r)
				$resultsArr[] = $r;

			return $resultsArr;
		}
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return bool
	 */
	private function isSelectCacheable(string $table, array|int $where = [], array $options = []): bool
	{
		// Only full selects or selects by id are cacheable
		if (is_array($where) and count($where) > 0)
			return false;

		// If I did request select not to be cached, I return false
		if (!($options['cache'] ?? true))
			return false;

		// Only simple queries can be cached
		if (!empty($options['joins']) or !empty($options['fields']) or !empty($options['group_by']))
			return false;

		// If order_by is used, only array form is cacheable
		if (($options['order_by'] ?? null) and !is_array($options['order_by']))
			return false;

		// If limit is used, only normal limits are accepted
		if (($options['limit'] ?? null) and !preg_match('/^[0-9]+(, ?[0-9+])?$/', $options['limit']))
			return false;

		// Only tables with no more than 200 rows are cached
		if ($this->count($table) > 200)
			return false;

		return true;
	}

	/**
	 * @param string $table
	 * @return string
	 */
	private function getCacheKeyFor(string $table): string
	{
		return 'model.db.cache.tables.' . $this->config['host'] . '.' . $this->config['name'] . '.' . $table;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return array
	 */
	private function selectFromCache(string $table, array|int $where = [], array $options = []): array
	{
		$cacheKey = $this->getCacheKeyFor($table) . '.rows';
		$rows = $this->getItemFromCache($table, $cacheKey, function (\Symfony\Contracts\Cache\ItemInterface $item) use ($table, $cacheKey) {
			$item->expiresAfter(3600 * 24);
			Cache::registerInvalidation('keys', [$cacheKey]);

			$tableModel = $this->parser->getTable($table);

			$response = [];
			$results = $this->selectAll($table, [], ['cache' => false]);
			foreach ($results as $row)
				$response[$row[$tableModel->primary[0]]] = $row;

			return $response;
		});

		// Select by id
		if (is_int($where))
			return isset($rows[$where]) ? [$rows[$where]] : [];

		if ($options['order_by'] ?? null) {
			usort($rows, function ($a, $b) use ($options) {
				foreach ($options['order_by'] as $sortingField) {
					if (is_string($sortingField))
						$sortingField = [$sortingField, 'ASC'];

					if ($a[$sortingField[0]] != $b[$sortingField[0]])
						return $a[$sortingField[0]] <=> $b[$sortingField[0]];
				}

				return 0;
			});
		} else {
			$rows = array_values($rows);
		}

		if ($options['limit'] ?? null) {
			if (is_numeric($options['limit'])) {
				$rows = array_slice($rows, 0, $options['limit']);
			} else {
				$limit = explode(',', $options['limit']);
				$rows = array_slice($rows, $limit[0], $limit[1]);
			}
		}

		return $rows;
	}

	/**
	 * Middleware method that stores in memory repeated cache queries
	 *
	 * @param string $table
	 * @param string $key
	 * @param callable $getter
	 * @return mixed
	 */
	private function getItemFromCache(string $table, string $key, callable $getter): mixed
	{
		if (!isset($this->inMemoryCache[$table], $this->inMemoryCache[$table][$key])) {
			$cache = Cache::getCacheAdapter();
			$this->inMemoryCache[$table][$key] = $cache->get($key, $getter);
		}

		return $this->inMemoryCache[$table][$key];
	}

	/**
	 * @param array $queries
	 * @param array $options
	 * @return iterable
	 */
	public function unionSelect(array $queries, array $options = []): iterable
	{
		$qry_str = [];
		foreach ($queries as $qryOptions) {
			$copiedQueryOptions = $options;
			if (isset($copiedQueryOptions['order_by']))
				unset($copiedQueryOptions['order_by']);
			if (isset($copiedQueryOptions['limit']))
				unset($copiedQueryOptions['limit']);

			$singleQueryOptions = $qryOptions['options'] ?? [];
			$copiedQueryOptions = $this->array_merge_recursive_distinct($singleQueryOptions, $copiedQueryOptions);
			$qry_str[] = $this->builder->select($qryOptions['table'], $qryOptions['where'] ?? [], $copiedQueryOptions);
		}

		if (empty($qry_str))
			return [];

		$qry = implode(' UNION ', $qry_str);

		if (($options['order_by'] ?? null) !== null) {
			if (!is_string($options['order_by']))
				throw new \Exception('Currently, only strings "order by" are supported in union selects');

			$qry .= ' ORDER BY ' . $options['order_by'];
		}

		if (($options['limit'] ?? null) !== null)
			$qry .= ' LIMIT ' . $options['limit'];

		return $this->query($qry, null, 'SELECT', $options);
	}

	/**
	 * Utility per il metodo precedente
	 *
	 * @param array $array1
	 * @param array $array2
	 * @return array
	 */
	private function array_merge_recursive_distinct(array &$array1, array &$array2): array
	{
		$merged = $array1;

		foreach ($array2 as $key => &$value) {
			if (is_numeric($key))
				$merged[] = $value;
			elseif (is_array($value) && isset ($merged [$key]) && is_array($merged [$key]))
				$merged[$key] = $this->array_merge_recursive_distinct($merged [$key], $value);
			else
				$merged[$key] = $value;
		}

		return $merged;
	}

	/**
	 * Streams the results via generator, normalizing values
	 *
	 * @param string $table
	 * @param iterable $results
	 * @param array $options
	 * @param array $providers
	 * @return \Generator
	 */
	private function streamResults(string $table, iterable $results, array $options, array $providers): \Generator
	{
		foreach ($results as $r) {
			foreach ($providers as $provider)
				$r = $provider['provider']::alterSelectResult($this, $table, $r, $options);

			yield $this->normalizeRowValues($table, $r);
		}
	}

	/**
	 * @param string $table
	 * @param array $row
	 * @return array
	 */
	private function normalizeRowValues(string $table, array $row): array
	{
		$tableModel = $this->getTable($table);

		$newRow = [];
		foreach ($row as $k => $v) {
			if ($v !== null and array_key_exists($k, $tableModel->columns)) {
				$c = $tableModel->columns[$k];
				if (in_array($c['type'], ['double', 'float', 'decimal']))
					$v = (float)$v;
				if (in_array($c['type'], ['tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'year']))
					$v = (int)$v;

				if ($c['type'] === 'point') {
					$v = array_map(function ($v) {
						return (float)$v;
					}, explode(' ', substr($v, 6, -1)));
					if (count($v) !== 2 or ($v[0] == 0 and $v[1] == 0))
						$v = null;
				}
			}

			$newRow[$k] = $v;
		}

		return $newRow;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $options
	 * @return int
	 */
	public function count(string $table, array $where = [], array $options = []): int
	{
		if (empty($where) and empty($options)) {
			// Simple counts are cached
			$cacheKey = $this->getCacheKeyFor($table) . '.count';
			return (int)$this->getItemFromCache($table, $cacheKey, function (\Symfony\Contracts\Cache\ItemInterface $item) use ($table, $cacheKey) {
				Cache::registerInvalidation('keys', [$cacheKey]);
				$item->expiresAfter(3600 * 24);
				return $this->count($table, [], ['cache' => false]);
			});
		}

		$options['fields'] = 'COUNT(*)';
		$qry = $this->builder->select($table, $where, $options);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		return $this->query($qry, $table, 'SELECT', $options)->fetchColumn();
	}

	/**
	 * @param string $query
	 * @param string|null $table
	 * @param string|null $type
	 * @param array $options
	 * @return \PDOStatement
	 * @throws \Exception
	 */
	public function query(string $query, string $table = null, string $type = null, array $options = []): \PDOStatement
	{
		$options = array_merge([
			'query_limit' => true,
			'debug' => false,
		], $options);

		if ($options['debug'] ?? false)
			echo "QUERY: " . $query . "\n";

		if ($options['query_limit']) {
			if ($this->config['limits']['query']) {
				if (!isset($this->query_counters['query'][$query]))
					$this->query_counters['query'][$query] = 0;
				$this->query_counters['query'][$query]++;

				if ($this->query_counters['query'][$query] > $this->config['limits']['query'])
					throw new \Exception('Query limit (per query) exceeded. - ' . $query);
			}

			if ($this->config['limits']['table'] and $table !== null) {
				if (!isset($this->query_counters['table'][$table]))
					$this->query_counters['table'][$table] = 0;
				$this->query_counters['table'][$table]++;

				if ($this->query_counters['table'][$table] > $this->config['limits']['table'])
					throw new \Exception('Query limit (per table "' . $table . '") exceeded. - ' . $query);
			}

			if ($this->config['limits']['total']) {
				$this->query_counters['total']++;

				if ($this->query_counters['total'] > $this->config['limits']['total'])
					throw new \Exception('Total query limit exceeded');
			}
		}

		// Cache invalidation for that specific table
		if ($table and $type !== 'SELECT')
			$this->changedTable($table);

		return $this->db->query($query);
	}

	/**
	 * @param string $type
	 * @param int|null $n
	 * @return void
	 */
	public function setQueryLimit(string $type, ?int $n): void
	{
		if (!array_key_exists($type, $this->config['limits']))
			throw new \Exception('Query limit ' . $type . ' not found');

		$this->config['limits'][$type] = $n;
	}

	/**
	 * Invalidates cache for a specific table
	 *
	 * @param string $table
	 * @return void
	 */
	public function changedTable(string $table): void
	{
		$cache = Cache::getCacheAdapter();
		$cache->deleteItems([
			$this->getCacheKeyFor($table) . '.rows',
			$this->getCacheKeyFor($table) . '.count',
		]);
		if (isset($this->inMemoryCache[$table]))
			unset($this->inMemoryCache[$table]);
	}

	/**
	 * @param string $column
	 * @param string|null $table
	 * @return string
	 */
	public function parseColumn(string $column, ?string $table = null): string
	{
		return $this->builder->parseColumn($column, $table);
	}

	/**
	 * @param mixed $v
	 * @param string|null $type
	 * @return string
	 * @throws \Exception
	 */
	public function parseValue(mixed $v, ?string $type = null): string
	{
		return $this->builder->parseValue($v, $type);
	}

	/**
	 * @param string $name
	 * @return Table
	 */
	public function getTable(string $name): Table
	{
		if (!isset($this->tablesCache[$name])) {
			$tableModel = clone $this->parser->getTable($name);

			$providers = Providers::find('DbProvider');
			foreach ($providers as $provider)
				$tableModel = $provider['provider']::alterTableModel($this, $name, clone $tableModel);

			$this->tablesCache[$name] = $tableModel;
		}

		return $this->tablesCache[$name];
	}

	/**
	 * @return bool
	 */
	public function inTransaction(): bool
	{
		return $this->c_transactions > 0;
	}

	/**
	 * @return bool
	 */
	public function beginTransaction(): bool
	{
		$res = ($this->c_transactions === 0) ? $this->db->beginTransaction() : true;
		if ($res)
			$this->c_transactions++;
		return $res;
	}

	/**
	 * @return bool
	 */
	public function commit(): bool
	{
		if ($this->c_transactions <= 0)
			return false;

		$this->c_transactions--;
		if ($this->c_transactions == 0)
			return $this->db->commit();
		else
			return true;
	}

	/**
	 * @return bool
	 */
	public function rollBack(): bool
	{
		if ($this->c_transactions > 0) {
			$this->c_transactions = 0;
			return $this->db->rollBack();
		}

		$this->c_transactions = 0;
		return false;
	}

	/**
	 * @return void
	 */
	public function terminate(): void
	{
		foreach ($this->deferedInserts as $table => $options) {
			if (count($options['rows']) > 0)
				$this->bulkInsert($table);
		}

		if ($this->inTransaction())
			$this->commit();
	}
}
