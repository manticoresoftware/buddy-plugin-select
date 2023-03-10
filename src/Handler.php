<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\Select;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\Plugin\BaseHandler;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;
use parallel\Runtime;

final class Handler extends BaseHandler {
	const FIELD_MAP = [
		'engine' => ['field', 'engine'],
		'table_type' => ['static', 'BASE TABLE'],
		'table_name' => ['table', ''],
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
			if ($payload->table === 'information_schema.files' || $payload->table === 'information_schema.triggers') {
				$columns = $payload->getColumns();
				return new TaskResult(
					[[
						'total' => 0,
						'warning' => '',
						'error' => '',
						'columns' => $columns,
					],
					]
				);
			}

			// TODO: maybe later we will implement multiple tables handle
			$table = $payload->where['table_name']['value'];
			$query = "SHOW CREATE TABLE {$table}";
			/** @var array<array{data:array<array<string,string>>}> */
			$schemaResult = $manticoreClient->sendRequest($query)->getResult();
			$createTable = $schemaResult[0]['data'][0]['Create Table'] ?? '';

			$columns = $payload->getColumns();
			$data = [];
			if ($createTable) {
				$createTables = [$createTable];
				$i = 0;
				foreach ($createTables as $createTable) {
					$row = static::parseTableSchema($createTable);
					$data[$i] = [];
					foreach ($payload->fields as $field) {
						[$type, $value] = static::FIELD_MAP[$field] ?? ['field', $field];
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

			return new TaskResult(
				[[
					'total' => sizeof($data),
					'error' => '',
					'warning' => '',
					'columns' => $columns,
					'data' => $data,
				],
				]
			);
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
}
