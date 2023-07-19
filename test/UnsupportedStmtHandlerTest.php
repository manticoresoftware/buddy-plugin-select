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

class UnsupportedStmtHandlerTest extends TestCase {

	use TestHTTPServerTrait;
	use TestInEnvironmentTrait;

	/**
	 * @var HTTPClient $manticoreClient
	 */
	public static $manticoreClient;

	public static function setUpBeforeClass(): void {
		self::setTaskRuntime();
		$serverUrl = self::setUpMockManticoreServer(false);
		self::setBuddyVersion();
		self::$manticoreClient = new HTTPClient(new Response(), $serverUrl);
	}

	public static function tearDownAfterClass(): void {
		self::finishMockManticoreServer();
	}

	public function testSelectFromInformationSchemaExecutionOk():void {
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
		];

		foreach ($testingSet as $query) {
			$this->checkExecutionResult($query, $columns, []);
		}
	}

	public function testSelectFromInformationSchemaExecutionFail():void {
		echo "\nTesting the execution of a select from an information_schema's table is not handled by Buddy\n";
		$testingSet = [
			'SELECT * FROM information_schema.routines',
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

			$task = $handler->run(Task::createRuntime());
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
