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
			// 0. Select that has database
			if (stripos($payload->originalQuery, '`Manticore`.') > 0) {
				$query = str_replace('`Manticore`.', '', $payload->originalQuery);
				$queryResult = $manticoreClient->sendRequest($query)->getResult();
				return TaskResult::raw($queryResult);
			}

			// 1. Handle empty table case first
			if (!$payload->table) {
				return static::handleMethods($manticoreClient, $payload);
			}

			// 2. Other cases with normal select * from [table]
			if ($payload->table === 'information_schema.files'
				|| $payload->table === 'information_schema.triggers'
				|| $payload->table === 'information_schema.column_statistics'
			) {
				return $payload->getTaskResult();
			}

			// 3. Handle select count(*) from information_schema.tables
			if (sizeof($payload->fields) === 1 && stripos($payload->fields[0], 'count(*)') === 0) {
				return static::handleFieldCount($manticoreClient, $payload);
			}

			// 4. Select from columns
			if ($payload->table === 'information_schema.columns') {
				return static::handleSelectFromColumns($manticoreClient, $payload);
			}

			// 5. Select from tables
			return static::handleSelectFromTables($manticoreClient, $payload);
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
		$table = $payload->where['table_name']['value'];

		$query = "SHOW CREATE TABLE {$table}";
		/** @var array<array{data:array<array<string,string>>}> */
		$schemaResult = $manticoreClient->sendRequest($query)->getResult();
		$createTable = $schemaResult[0]['data'][0]['Create Table'] ?? '';

		$result = $payload->getTaskResult();
		$data = [];
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

		return $result->data($data);
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
}
