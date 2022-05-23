<?php namespace Model\Db;

use Model\DbParser\Parser;
use Model\QueryBuilder\QueryBuilder;

class DbConnection
{
	private \PDO $db;
	private int $c_transactions = 0;
	private Parser $parser;
	private QueryBuilder $builder;

	public function __construct(public readonly array $config)
	{
		$this->db = new \PDO('mysql:host=' . $this->config['host'] . ':' . $this->config['port'] . ';dbname=' . $this->config['name'] . ';charset=utf8', $this->config['username'], $this->config['password'], [
			\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
			\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
			\PDO::ATTR_STRINGIFY_FETCHES => false,
		]);

		$this->parser = new Parser($this->db);
		$this->builder = new QueryBuilder($this->parser);
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return int
	 */
	public function insert(string $table, array $data = [], array $options = []): int
	{
		$qry = $this->builder->insert($table, $data, $options);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		if (!$this->inTransaction())
			$this->beginTransaction();

		$this->query($qry);
		return $this->db->lastInsertId();
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
		$qry = $this->builder->update($table, $where, $data);
		if ($qry === null)
			return null;

		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		if (!$this->inTransaction())
			$this->beginTransaction();

		return $this->query($qry);
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return \PDOStatement|null
	 */
	public function delete(string $table, array|int $where = [], array $options = []): ?\PDOStatement
	{
		$qry = $this->builder->delete($table, $where);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		if (!$this->inTransaction())
			$this->beginTransaction();

		return $this->query($qry);
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
		$qry = $this->builder->select($table, $where, $options);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		$result = $this->query($qry)->fetch();

		return $result ? $this->normalizeRowValues($table, $result) : null;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return iterable
	 */
	public function selectAll(string $table, array|int $where = [], array $options = []): iterable
	{
		$qry = $this->builder->select($table, $where, $options);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		$response = $this->query($qry);

		$results = $this->streamResults($table, $response);

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

		if (($options['order_by'] ?? null) !== null)
			$qry .= ' ORDER BY ' . $options['order_by'];
		if (($options['limit'] ?? null) !== null)
			$qry .= ' LIMIT ' . $options['limit'];

		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		return $this->query($qry);
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
	 * @return \Generator
	 */
	private function streamResults(string $table, iterable $results): \Generator
	{
		foreach ($results as $r)
			yield $this->normalizeRowValues($table, $r);
	}

	/**
	 * @param string $table
	 * @param array $row
	 * @return array
	 */
	private function normalizeRowValues(string $table, array $row): array
	{
		$tableModel = $this->parser->getTable($table);

		$newRow = [];
		foreach ($row as $k => $v) {
//			if (strpos($k, 'zkaggr_') === 0) // Remove aggregates prefix // TODO
//				$k = substr($k, 7);

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
		$options['fields'] = 'COUNT(*)';
		$qry = $this->builder->select($table, $where, $options);
		if ($options['debug'] ?? false)
			echo "QUERY: " . $qry . "\n";

		return $this->query($qry)->fetchColumn();
	}

	/**
	 * @param string $query
	 * @return \PDOStatement
	 */
	public function query(string $query): \PDOStatement
	{
		return $this->db->query($query);
	}

	/**
	 * @param string $k
	 * @param array $options
	 * @return string
	 */
	public function parseColumn(string $k, array $options = []): string
	{
		return $this->builder->parseColumn($k, $options);
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
}
