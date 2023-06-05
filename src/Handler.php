<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\Select;

use Exception;
use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\Plugin\BaseHandler;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

final class Handler extends BaseHandler {
	const TABLES_FIELD_MAP = [
		'engine' => ['field', 'engine'],
		'table_type' => ['static', 'BASE TABLE'],
		'table_name' => ['table', ''],
	];

	const COLUMNS_FIELD_MAP = [
		'extra' => ['static', ''],
		'generation_expression' => ['static', ''],
		'column_name' => ['field', 'Field'],
		'data_type' => ['field', 'Type'],
	];

  /** @var HTTPClient $manticoreClient */
	protected HTTPClient $manticoreClient;

	/**
	 * Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

  /**
	 * Process the request
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(Runtime $runtime): Task {
		$this->manticoreClient->setPath($this->payload->path);
		$taskFn = static function (Payload $payload, HTTPClient $manticoreClient): TaskResult {
			if (preg_match('/COUNT\([^*\)]+\)/ius', $payload->originalQuery)) {
				return static::handleSelectCountOnField($manticoreClient, $payload);
			}

			// 0. Select that has database
			if (stripos($payload->originalQuery, '`Manticore`.') > 0
				|| stripos($payload->originalQuery, 'Manticore.') > 0
			) {
				return static::handleSelectDatabasePrefixed($manticoreClient, $payload);
			}

			// 1. Handle empty table case first
			if (!$payload->table) {
				return static::handleMethods($manticoreClient, $payload);
			}

			// 2. Other cases with normal select * from [table]
			if (stripos(
				'information_schema.files|information_schema.triggers|information_schema.column_statistics',
				$payload->table
			) !== false
			) {
				return $payload->getTaskResult();
			}

			// 3. Select from columns
			if ($payload->table === 'information_schema.columns') {
				return static::handleSelectFromColumns($manticoreClient, $payload);
			}

			// 4. Handle select fields or count(*) from information_schema.tables
			if ($payload->table === 'information_schema.tables') {
				return static::handleSelectFromTables($manticoreClient, $payload);
			}

			// 5. Select from existing table while pasing string as a numeric
			return static::handleSelectFromExistingTable($manticoreClient, $payload);
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$this->payload, $this->manticoreClient]
		)->run();
	}

	/**
	 * @return array<string>
	 */
	public function getProps(): array {
		return ['manticoreClient'];
	}

	/**
	 * Instantiating the http client to execute requests to Manticore server
	 *
	 * @param HTTPClient $client
	 * $return HTTPClient
	 */
	public function setManticoreClient(HTTPClient $client): HTTPClient {
		$this->manticoreClient = $client;
		return $this->manticoreClient;
	}

	/**
	 * Parse show create table response
	 * @param string $schema
	 * @return array<string,string>
	 */
	protected static function parseTableSchema(string $schema): array {
		preg_match("/\) engine='(.+?)'/", $schema, $matches);
		$row = [];
		if ($matches) {
			$row['engine'] = strtoupper($matches[1]);
		} else {
			$row['engine'] = 'ROWWISE';
		}

		return $row;
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleMethods(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		[$method] = $payload->fields;
		[$query, $field] = match (strtolower($method)) {
			'version()' => ["show status like 'mysql_version'", 'Value'],
			default => throw new Exception("Unsupported method called: $method"),
		};

		/** @var array{0:array{data:array{0:array{Databases:string,Value:string}}}} */
		$queryResult = $manticoreClient->sendRequest($query)->getResult();
		return $payload->getTaskResult()->row(['Value' => $queryResult[0]['data'][0][$field]]);
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleFieldCount(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		$table = $payload->where['table_name']['value'];
		$query = "DESC {$table}";
		/** @var array{0:array{data:array<mixed>}} */
		$descResult = $manticoreClient->sendRequest($query)->getResult();
		$count = sizeof($descResult[0]['data']);
		return TaskResult::withRow(['COUNT(*)' => $count])
			->column('COUNT(*)', Column::String);
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleSelectFromTables(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		if (sizeof($payload->fields) === 1 && stripos($payload->fields[0], 'count(*)') === 0) {
			return static::handleFieldCount($manticoreClient, $payload);
		}

		$table = $payload->where['table_name']['value'] ?? null;
		$data = [];
		if ($table) {
			$query = "SHOW CREATE TABLE {$table}";
			/** @var array<array{data:array<array<string,string>>}> */
			$schemaResult = $manticoreClient->sendRequest($query)->getResult();
			$createTable = $schemaResult[0]['data'][0]['Create Table'] ?? '';

			if ($createTable) {
				$createTables = [$createTable];
				$i = 0;
				foreach ($createTables as $createTable) {
					$row = static::parseTableSchema($createTable);
					$data[$i] = [];
					foreach ($payload->fields as $field) {
						[$type, $value] = static::TABLES_FIELD_MAP[$field] ?? ['field', $field];
						$data[$i][$field] = match ($type) {
							'field' => $row[$value],
							'table' => $table,
							'static' => $value,
							// default => $row[$field] ?? null,
						};
					}
					++$i;
				}
			}
		} else {
			$data = static::processSelectOtherFromFromTables($manticoreClient, $payload);
		}

		$result = $payload->getTaskResult();
		return $result->data($data);
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return array<array<string,string>>
	 */
	protected static function processSelectOtherFromFromTables(HTTPClient $manticoreClient, Payload $payload): array {
		$data = [];
		// grafana: SELECT DISTINCT TABLE_SCHEMA from information_schema.TABLES
		// where TABLE_TYPE != 'SYSTEM VIEW' ORDER BY TABLE_SCHEMA
		if (sizeof($payload->fields) === 1
			&& stripos($payload->fields[0], 'table_schema') !== false
		) {
			$data[] = [
				'TABLE_SCHEMA' => 'Manticore',
			];
		} elseif (sizeof($payload->fields) === 1 && stripos($payload->fields[0], 'table_name') !== false) {
			$query = 'SHOW TABLES';
			/** @var array<array{data:array<array<string,string>>}> */
			$tablesResult = $manticoreClient->sendRequest($query)->getResult();
			foreach ($tablesResult[0]['data'] as $row) {
				$data[] = [
					'TABLE_NAME' => $row['Index'],
				];
			}
		}

		return $data;
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleSelectFromColumns(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		$table = $payload->where['table_name']['value'];

		$query = "DESC {$table}";
		/** @var array<array{data:array<array<string,string>>}> */
		$descResult = $manticoreClient->sendRequest($query)->getResult();

		$data = [];
		$i = 0;
		foreach ($descResult[0]['data'] as $row) {
			$data[$i] = [];
			foreach ($payload->fields as $field) {
				[$type, $value] = static::COLUMNS_FIELD_MAP[$field] ?? ['field', $field];
				$data[$i][$field] = match ($type) {
					'field' => $row[$value],
					'static' => $value,
					// default => $row[$field] ?? null,
				};
			}
			++$i;
		}
		$result = $payload->getTaskResult();
		return $result->data($data);
	}


	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleSelectFromExistingTable(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		$table = str_ireplace(
			['`Manticore`.', 'Manticore.'],
			'',
			$payload->table
		);
		$selectQuery = str_ireplace(
			['`Manticore`.', 'Manticore.'],
			'',
			$payload->originalQuery
		);
		$selectQuery = preg_replace_callback(
			'/COALESCE\(([a-z@][a-z0-9_@]*),\s*\'\'\)\s*(<>|=[^>])\s*\'\'|'
				. 'CONTAINS\(([a-z@][a-z0-9_@]*), \'NEAR\(\((\w+), (\w+)\), (\d+)\)\'\)/ius',
			function ($matches) {
				if (isset($matches[1])) {
					return $matches[1] . ' ' . $matches[2] . ' \'\'';
				}

				return 'MATCH(\'' . $matches[4]
					. ' NEAR/' . $matches[6]
					. ' ' . $matches[5] . '\')';
			},
			$selectQuery
		);
		if (!$selectQuery) {
			throw new Exception('Failed to parse coalesce or contains from the query');
		}

		$query = "DESC {$table}";
		/** @var array<array{data:array<array<string,string>>}> */
		$descResult = $manticoreClient->sendRequest($query)->getResult();

		$isLikeOp = false;
		foreach ($descResult[0]['data'] as $row) {
			// Skip missing where statements
			if (!isset($payload->where[$row['Field']])) {
				continue;
			}

			$field = $row['Field'];
			$operator = $payload->where[$field]['operator'];
			$value = $payload->where[$field]['value'];
			$isInOp = str_contains($operator, 'IN');
			$isLikeOp = str_contains($operator, 'LIKE');

			$regexFn = static function ($field, $value) {
				return "REGEX($field, '^" . str_replace(
					'%',
					'.*',
					$value
				) . "$')";
			};

			$isNot = str_starts_with($operator, 'NOT');
			$selectQuery = match ($row['Type']) {
				'bigint', 'int', 'uint' => str_replace(
					match (true) {
						$isInOp => "{$field} {$operator} ('{$value}')",
						default => "{$field} {$operator} '{$value}'",
					},
					match (true) {
						$isInOp => "{$field} {$operator} '{$value}'",
					default => "{$field} {$operator} {$value}",
					},
					$selectQuery
				),
				'json', 'string' => str_replace(
					"{$field} {$operator} '{$value}'",
					match (true) {
						$isLikeOp => "{$field}__regex = " . ($isNot ? '0' : '1'),
					default => "{$field} {$operator} '{$value}'",
					},

					$isLikeOp ? str_ireplace(
						'select ',
						'select ' . $regexFn($field, $value) . ' AS ' . $field . '__regex,',
						$selectQuery
					) : $selectQuery
				),
				default => $selectQuery,
			};
		}

		/** @var array{0:array{columns:array<array<string,mixed>>,data:array<array<string,string>>}} */
		$result = $manticoreClient->sendRequest($selectQuery)->getResult();
		if ($isLikeOp) {
			$result = static::filterRegexFieldsFromResult($result);
		}
		return TaskResult::raw($result);
	}


	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleSelectCountOnField(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		$selectQuery = str_ireplace(
			['`Manticore`.', 'Manticore.'],
			'',
			$payload->originalQuery
		);

		$pattern = '/COUNT\((?! *\* *\))(\w+)\)/ius';
		$replacement = 'COUNT(*)';
		$query = preg_replace($pattern, $replacement, $selectQuery);
		if (!$query) {
			throw new Exception('Failed to fix query');
		}
		/** @var array<array{data:array<array<string,string>>}> */
		$selectResult = $manticoreClient->sendRequest($query)->getResult();
		return TaskResult::raw($selectResult);
	}

	/**
	 * @param HTTPClient $manticoreClient
	 * @param Payload $payload
	 * @return TaskResult
	 */
	protected static function handleSelectDatabasePrefixed(HTTPClient $manticoreClient, Payload $payload): TaskResult {
		$query = str_ireplace(
			['`Manticore`.', 'Manticore.'],
			'',
			$payload->originalQuery
		);

		/** @var array{error?:string} $queryResult */
		$queryResult = $manticoreClient->sendRequest($query)->getResult();
		if (isset($queryResult['error'])) {
			$errors = [
				"unsupported filter type 'string' on attribute",
				"unsupported filter type 'stringlist' on attribute",
				'unexpected LIKE',
				"unexpected '(' near '(",
			];

			foreach ($errors as $error) {
				if (str_contains($queryResult['error'], $error)) {
					return static::handleSelectFromExistingTable($manticoreClient, $payload);
				}
			}
		}

		return TaskResult::raw($queryResult);
	}

	/**
	 * @param array{0:array{columns:array<array<string,mixed>>,data:array<array<string,string>>}} $result
	 * @return array{0:array{columns:array<array<string,mixed>>,data:array<array<string,string>>}}
	 */
	protected static function filterRegexFieldsFromResult(array $result): array {
		$result[0]['columns'] = array_filter(
			$result[0]['columns'],
			fn($v) => !str_ends_with(array_key_first($v) ?? '', '__regex')
		);
		$result[0]['data'] = array_map(
			function ($row) {
				foreach (array_keys($row) as $key) {
					if (!str_ends_with($key, '__regex')) {
						continue;
					}

					unset($row[$key]);
				}

				return $row;
			}, $result[0]['data']
		);

		return $result;
	}
}
