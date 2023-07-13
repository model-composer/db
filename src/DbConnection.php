<?php namespace Model\Db;

use Model\Cache\Cache;
use Model\Db\Events\ChangedTable;
use Model\Db\Events\DeleteQuery;
use Model\Db\Events\InsertedQuery;
use Model\Db\Events\InsertQuery;
use Model\Db\Events\Query;
use Model\Db\Events\SelectQuery;
use Model\Db\Events\UpdateQuery;
use Model\DbParser\Parser;
use Model\DbParser\Table;
use Model\Events\Events;
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
			'cache_tables' => [],
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
	 * @return \PDO
	 */
	public function getDb(): \PDO
	{
		return $this->db;
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

		$rows = ($this->isAssoc($data) or count($data) === 0) ? [$data] : $data;

		Events::dispatch(new InsertQuery($table, ['data' => $data, 'options' => $options]));

		$queries = [
			[
				'table' => $table,
				'rows' => $rows,
				'options' => $options,
			],
		];

		if ($options['alter'] ?? true) {
			$providers = Providers::find('DbProvider');
			foreach ($providers as $provider)
				$queries = $provider['provider']::alterInsert($this, $queries);
		}

		if ($options['defer'] !== null) {
			if (count($queries) > 1)
				throw new \Exception('Cannot defer an insert that needs more than one query');

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

			$this->deferedInserts[$table]['rows'] = array_merge($this->deferedInserts[$table]['rows'], $rows);
			if ($options['defer'] > 0 and count($this->deferedInserts[$table]['rows']) >= $options['defer'])
				$this->bulkInsert($table);

			return null;
		}

		$ids = [];
		foreach ($queries as $qryIdx => $query) {
			foreach (($query['options']['replace_ids'] ?? []) as $replace_id) {
				if (!isset($ids[$replace_id['from']]))
					throw new \Exception('Query idx ' . $replace_id['from'] . ' does not exist');

				foreach ($query['rows'] as &$row)
					$row[$replace_id['field']] = $ids[$replace_id['from']];
				unset($row);
			}

			$qry = $this->builder->insert($query['table'], $query['rows'], $query['options']);
			if (!$qry)
				continue;

			if (!$this->inTransaction())
				$this->beginTransaction();

			$this->query($qry, $query['table'], 'INSERT', $query['options']);
			$ids[$qryIdx] = $this->db->lastInsertId();
		}

		Events::dispatch(new InsertedQuery($table, $ids[0] ?? null));

		return $ids[0] ?? null;
	}

	/**
	 * Checks whether the given array is associative
	 *
	 * @param array $arr
	 * @return bool
	 */
	private function isAssoc(array $arr): bool
	{
		if ([] === $arr)
			return false;
		return array_keys($arr) !== range(0, count($arr) - 1);
	}

	/**
	 * @param string $table
	 * @return void
	 */
	public function bulkInsert(string $table): void
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

		Events::dispatch(new UpdateQuery($table, ['where' => $where, 'data' => $data, 'options' => $options]));

		$queries = [
			[
				'table' => $table,
				'where' => $where,
				'data' => $data,
				'options' => $options,
			],
		];

		if ($options['alter'] ?? true) {
			$providers = Providers::find('DbProvider');
			foreach ($providers as $provider)
				$queries = $provider['provider']::alterUpdate($this, $queries);
		}

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
	 * @param array $data
	 * @param array $options
	 * @return null|int
	 */
	public function updateOrInsert(string $table, array|int $where, array $data, array $options = []): ?int
	{
		$tableModel = $this->getTable($table);
		if (!is_array($where) and is_numeric($where))
			$where = [$tableModel->primary[0] => $where];

		$check = $this->select($table, $where);
		if ($check) {
			$this->update($table, $where, $data, $options);
			return $check[$tableModel->primary[0]];
		} else {
			return $this->insert($table, array_merge($where, $data), $options);
		}
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

		Events::dispatch(new DeleteQuery($table, ['where' => $where, 'options' => $options]));

		if ($options['alter'] ?? true) {
			$providers = Providers::find('DbProvider');
			foreach ($providers as $provider)
				[$where, $options] = $provider['provider']::alterDelete($this, $table, $where, $options);
		}

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

		if ($options['in_memory_cache'] ?? true) {
			$cacheKey = sha1(json_encode($where) . json_encode($options));
			if (isset($this->inMemoryCache[$table]) and array_key_exists($cacheKey, $this->inMemoryCache[$table]))
				return $this->inMemoryCache[$table][$cacheKey];
		}

		Events::dispatch(new SelectQuery($table, ['where' => $where, 'options' => $options]));

		$originalWhere = $where;
		if ($options['alter'] ?? true) {
			$providers = Providers::find('DbProvider');
			foreach ($providers as $provider)
				[$where, $options] = $provider['provider']::alterSelect($this, $table, $where, $options);
		}

		$convertibleOptions = ['order_by', 'group_by'];
		foreach ($convertibleOptions as $convertibleOption) {
			// Backward compatibility: ff one of these options is a string with just a field name on it, it can be written in the new array form
			if (!empty($options[$convertibleOption]) and is_string($options[$convertibleOption]) and preg_match('/^[a-z0-9_]+$/i', $options[$convertibleOption]))
				$options[$convertibleOption] = [$options[$convertibleOption]];
		}

		$cacheable = $this->isSelectCacheable($table, $where, $options);
		if ($cacheable)
			return $this->selectFromCache($table, $where, $options);

		$qry = $this->builder->select($table, $where, $options);

		$response = $this->query($qry, $table, 'SELECT', $options);

		$results = $this->streamResults($table, $response, $options, ($options['alter'] ?? true) ? $providers : []);

		if (($options['in_memory_cache'] ?? true) and $this->isWhereById($table, $originalWhere) or ($response->rowCount() > 0 and $response->rowCount() < 300)) {
			$this->inMemoryCache[$table][$cacheKey] = [];
			foreach ($results as $r)
				$this->inMemoryCache[$table][$cacheKey][] = $r;
			return $this->inMemoryCache[$table][$cacheKey];
		}

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
	 * @return bool
	 */
	private function isWhereById(string $table, array|int $where): bool
	{
		if (is_int($where))
			return true;

		$tableModel = $this->parser->getTable($table);
		return (count($where) === 1 and isset($where[$tableModel->primary[0]]));
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return bool
	 */
	private function isSelectCacheable(string $table, array|int $where = [], array $options = []): bool
	{
		// If I did request select not to be cached, I return false
		if (!($options['cache'] ?? true))
			return false;

		// Only full selects or selects by id are cacheable
		if (is_array($where) and count($where) > 0) {
			if (!$this->isWhereById($table, $where))
				return false;
		}

		// Only simple queries can be cached
		if (!empty($options['joins']) or !empty($options['group_by']))
			return false;

		// If order_by option is used, only array form is cacheable
		if (($options['order_by'] ?? null) and !is_array($options['order_by']))
			return false;

		// If fields option is used, only array form is cacheable
		if (($options['fields'] ?? null) and !is_array($options['fields']))
			return false;

		// If there is one unknown option, the query is not cacheable
		foreach ($options as $k => $v) {
			if (!in_array($k, ['cache', 'order_by', 'limit', 'offset', 'fields', 'stream']))
				return false;
		}

		// Only tables with no more than 200 rows are cached (along with the ones stated in config)
		if (!in_array($table, $this->config['cache_tables']) and $this->count($table) > 200)
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
		$rows = $this->getItemFromCache($table, $cacheKey, function (\Symfony\Contracts\Cache\ItemInterface $item) use ($table) {
			$item->expiresAfter(3600 * 24);

			$tableModel = $this->parser->getTable($table);

			$response = [];
			$results = $this->selectAll($table, [], ['cache' => false]);
			foreach ($results as $row)
				$response[$row[$tableModel->primary[0]]] = $row;

			return $response;
		});

		if (!empty($where)) {
			if (is_array($where)) {
				$tableModel = $this->parser->getTable($table);
				$where = $where[$tableModel->primary[0]];
			}

			return isset($rows[$where]) ? [$rows[$where]] : [];
		}

		if ($options['order_by'] ?? null) {
			usort($rows, function ($a, $b) use ($options) {
				foreach ($options['order_by'] as $sortingField) {
					if (is_string($sortingField))
						$sortingField = [$sortingField, 'ASC'];

					if ($a[$sortingField[0]] != $b[$sortingField[0]])
						return strtoupper($sortingField[1]) === 'ASC' ? $a[$sortingField[0]] <=> $b[$sortingField[0]] : $b[$sortingField[0]] <=> $a[$sortingField[0]];
				}

				return 0;
			});
		} else {
			$rows = array_values($rows);
		}

		if ($options['limit'] ?? null)
			$rows = array_slice($rows, $options['offset'] ?? 0, $options['limit']);

		if (!isset($options['fields'])) {
			return $rows;
		} else {
			$newRows = [];
			foreach ($rows as $r) {
				$newItem = [];
				foreach ($options['fields'] as $f)
					$newItem[$f] = $r[$f];
				$newRows[] = $newItem;
			}

			return $newRows;
		}
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
		$providers = Providers::find('DbProvider');
		foreach ($queries as &$qry) {
			foreach ($providers as $provider)
				[$qry['where'], $qry['options']] = $provider['provider']::alterSelect($this, $qry['table'], $qry['where'] ?? [], $qry['options'] ?? []);
		}

		$query = $this->builder->unionSelect($queries, $options);
		return $this->query($query, null, 'SELECT', $options);
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

			yield $this->normalizeRowValues($table, $r, $options);
		}
	}

	/**
	 * @param string $table
	 * @param array $row
	 * @param array $options
	 * @return array
	 */
	private function normalizeRowValues(string $table, array $row, array $options): array
	{
		$newRow = [];
		foreach ($row as $k => $v) {
			// TODO: support column aliases
			[$realTable, $realColumn, $parsedColumn, $isFromJoin] = $this->builder->parseInputColumn($k, $table, $options['joins'] ?? [], $options['alias'] ?? null);

			if ($v !== null and $realTable) {
				$tableModel = $this->getTable($realTable);

				if (array_key_exists($k, $tableModel->columns)) {
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
			return (int)$this->getItemFromCache($table, $cacheKey, function (\Symfony\Contracts\Cache\ItemInterface $item) use ($table) {
				$item->expiresAfter(3600 * 24);
				return $this->count($table, [], ['cache' => false]);
			});
		}

		if ($options['group_by'] ?? null) {
			if (is_array($options['group_by']) and count($options['group_by']) > 1)
				throw new \Exception('You cannot count rows grouped by more than one column');

			if (is_array($options['group_by']))
				$options['group_by'] = $options['group_by'][0];

			$options['count_distinct'] = [$options['group_by'] => 'MODEL_COUNT'];
			unset($options['group_by']);
		} else {
			$options['count'] = ['id' => 'MODEL_COUNT'];
		}

		$options['fields'] = [];
		$options['cache'] = false;
		$selectResponse = $this->select($table, $where, $options);
		return $selectResponse['MODEL_COUNT'];
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

		Events::dispatch(new Query($query, $table));

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

		Events::dispatch(new ChangedTable($table));
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
