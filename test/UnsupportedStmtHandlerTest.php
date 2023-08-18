<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/

use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\ManticoreSearch\Endpoint as ManticoreEndpoint;
use Manticoresearch\Buddy\Core\ManticoreSearch\RequestFormat;
use Manticoresearch\Buddy\Core\ManticoreSearch\Response;
use Manticoresearch\Buddy\Core\Network\Request as NetRequest;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\CoreTest\Trait\TestHTTPServerTrait;
use Manticoresearch\Buddy\CoreTest\Trait\TestInEnvironmentTrait;
use Manticoresearch\Buddy\Plugin\Select\Handler;
use Manticoresearch\Buddy\Plugin\Select\Payload;
use PHPUnit\Framework\TestCase;
use parallel\Runtime;

class UnsupportedStmtHandlerTest extends TestCase {

	use TestHTTPServerTrait;
	use TestInEnvironmentTrait;

	/**
	 * @var HTTPClient $manticoreClient
	 */
	public static $manticoreClient;

	/**
	 * @var Runtime $runtime
	 */
	public static $runtime;

	public static function setUpBeforeClass(): void {
		self::setTaskRuntime();
		$serverUrl = self::setUpMockManticoreServer(false);
		self::setBuddyVersion();
		self::$manticoreClient = new HTTPClient(new Response(), $serverUrl);
		self::$runtime = Task::createRuntime();
	}

	public static function tearDownAfterClass(): void {
		self::finishMockManticoreServer();
	}

	public function testColumnSelectFromInformationSchemaExecutionOk():void {
		echo "\nTesting the execution of SELECT FROM information_schema.*\n";
		$columns = [
			[
				'TEST' => ['type' => 'string'],
			],
		];
		$testingSet = [
			'SELECT DEFAULT_COLLATION_NAME as TEST FROM information_schema.schemata',
			'SELECT DEFAULT_COLLATION_NAME as TEST FROM `information_schema`.`schemata`',
			'SELECT `DEFAULT_COLLATION_NAME` as `TEST` FROM information_schema.schemata',
			'SELECT DEFAULT_COLLATION_NAME as `TEST` FROM INFORMATION_SCHEMA.SCHEMATA',
			"SELECT TEST FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='Manticore' "
			. "AND REFERENCED_TABLE_NAME IS NULL AND TABLE_NAME='test'",
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, []);
		}
	}

	public function testSelectFromInformationSchemaExecutionOk():void {
		echo "\nTesting the execution of SELECT FROM information_schema.*\n";
		$columns = [
			[
				'*' => ['type' => 'string'],
			],
		];
		$testingSet = [
			'SELECT * FROM information_schema.events',
			'SELECT * FROM information_schema.routines',
			'SELECT * FROM information_schema.partitions',
			'SELECT * FROM information_schema.statistics',
			"SELECT * FROM information_schema.EVENTS WHERE EVENT_SCHEMA='Manticore'",
			"SELECT * FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='Manticore'"
			. "AND ROUTINE_TYPE IN ('PROCEDURE','FUNCTION')",
			"SELECT * FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA='Manticore' AND TABLE_NAME='test'",
			"SELECT * FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='Manticore' AND TABLE_NAME='test'"
			. 'ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, []);
		}
	}

	public function testSelectFromMySqlUserOk():void {
		echo "\nTesting the execution of SELECT FROM mysql.user.*\n";
		$columns = [
			[
				'*' => ['type' => 'string'],
			],
		];
		$testingSet = [
			'SELECT * FROM mysql.user',
			'SELECT * FROM mysql.user ORDER BY user',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, []);
		}
	}

	public function testSelectFromInformationSchemaExecutionFail():void {
		echo "\nTesting the execution of a select from an information_schema's table is not handled by Buddy\n";
		$testingSet = [
			'SELECT * FROM information_schema.role_table_grants',
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, [], []);
		}
	}

	/**
	 * @param string $query
	 * @param array<mixed> $columns
	 * @param array<mixed> $data
	 * @return void
	*/
	protected function checkExecutionResult(
		string $query,
		array $columns,
		array $data,
	): void {
		$request = NetRequest::fromArray(
			[
				'error' => 'P01: syntax error, unexpected WHERE, expecting $end near \'where charset = \'utf8mb4\'\'',
				'payload' => $query,
				'version' => 1,
				'format' => RequestFormat::SQL,
				'endpointBundle' => ManticoreEndpoint::Sql,
				'path' => 'sql?mode=raw',
			]
		);
		try {
			$payload = Payload::fromRequest($request);
			$handler = new Handler($payload);
			$handler->setManticoreClient(self::$manticoreClient);

			$task = $handler->run(self::$runtime);
			$task->wait();

			$this->assertEquals(true, $task->isSucceed());
			$result = $task->getResult()->getStruct();
			if (!isset($result[0]) || !is_array($result[0]) || !isset($result[0]['columns'])) {
				$this->fail();
			}
			$this->assertEquals($result[0]['columns'], $columns);
			if (isset($result[0]['data'])) {
				$this->assertEquals($result[0]['data'], $data);
			}
		} catch (QueryParseError $e) {
			$this->assertEquals('Failed to handle your select query', $e->getResponseError());
		}
	}
}
