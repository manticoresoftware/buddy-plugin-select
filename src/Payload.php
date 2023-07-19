<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\Select;

use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\Network\Request;
use Manticoresearch\Buddy\Core\Plugin\BasePayload;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\TaskResult;

final class Payload extends BasePayload {
	const HANDLED_TABLES = [
		'information_schema.files',
		'information_schema.tables',
		'information_schema.triggers',
		'information_schema.column_statistics',
		'information_schema.columns',
		'information_schema.events',
		'information_schema.schemata',
	];

	/** @var string */
	public string $originalQuery;

	/** @var string */
	public string $path;

	/** @var string */
	public string $table = '';

	/** @var array<string> */
	public array $fields = [];

	/** @var array<string,array{operator:string,value:int|string|bool}> */
	public array $where = [];

	public function __construct() {
	}

  /**
	 * @param Request $request
	 * @return static
	 * @throws QueryParseError
	 */
	public static function fromRequest(Request $request): static {
		$self = new static();
		$self->path = $request->path;
		$self->originalQuery = str_replace("\n", ' ', $request->payload);

		// Match fields
		preg_match(
			'/^SELECT\s+(?:(.*?)\s+FROM\s+(`?[a-z][a-z\_\-0-9]*`?(\.`?[a-z][a-z\_\-0-9]*`?)?)|(version\(\)))/is',
			$self->originalQuery,
			$matches
		);

		// At this point we have two cases: when we have table and when we direct select some function like
		// select version()
		// we put this function in fields and table will be empty
		// otherwise it's normal select with fields and table required
		if ($matches[2] ?? null) {
			$self->table = str_replace('`', '', strtolower(ltrim($matches[2], '.')));
			echo 'TEST ' . $self->table . PHP_EOL;
			$pattern = '/(?:[^,(]+|(\((?>[^()]+|(?1))*\)))+/';
			preg_match_all($pattern, $matches[1], $matches);
			$self->fields = array_map('trim', $matches[0]);

			// Match WHERE statements
			$matches = [];
			$pattern = '/([@a-zA-Z0-9_]+)\s*(=|<|>|!=|<>|'
				. 'NOT LIKE|LIKE|NOT IN|IN)'
				. "\s*(?:\('([^']+)'\)|'([^']+)'|([0-9]+))/";
			preg_match_all($pattern, $request->payload, $matches);
			foreach ($matches[1] as $i => $column) {
				$operator = $matches[2][$i];
				$value = $matches[3][$i] !== '' ? $matches[3][$i] : $matches[4][$i];
				$self->where[(string)$column] = [
					'operator' => (string)$operator,
					'value' => (string)$value,
				];
			}
			// Check that we hit tables that we support otherwise return standard error
			// To proxy original one
			if (!str_contains($request->error, "unsupported filter type 'string' on attribute")
				&& !in_array($self->table, static::HANDLED_TABLES)
				&& !str_starts_with($self->table, 'manticore')
			) {
				throw QueryParseError::create('Failed to handle your select query', true);
			}
		} else {
			$self->fields[] = $matches[4];
		}

		return $self;
	}

	/**
	 * Return initial TaskResult that we can extend later
	 * based on the fields we have
	 *
	 * @return TaskResult
	 */
	public function getTaskResult(): TaskResult {
		$result = TaskResult::withTotal(0);
		foreach ($this->fields as $field) {
			if (stripos($field, ' AS ') !== false) {
				[, $field] = (array)preg_split('/ AS /i', $field);
			}
			$result->column(trim((string)$field, '`'), Column::String);
		}

		return $result;
	}

	/**
	 * @param Request $request
	 * @return bool
	 */
	public static function hasMatch(Request $request): bool {
		$isSelect = stripos($request->payload, 'select') === 0;
		if ($isSelect) {
			foreach (static::HANDLED_TABLES as $table) {
				[$db, $dbTable] = explode('.', $table);
				if (preg_match("/`?$db`?\.`?$dbTable`?/i", $request->payload)) {
					return true;
				}
			}

			if (preg_match('/(`?Manticore`?|^select\s+version\(\))/ius', $request->payload)) {
				return true;
			}

			if (static::matchError($request)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param Request $request
	 * @return bool
	 */
	public static function matchError(Request $request): bool {
		if (str_contains($request->error, "unsupported filter type 'string' on attribute")) {
			return true;
		}

		if (str_contains($request->error, "syntax error, unexpected identifier, expecting DISTINCT or '*' near")) {
			return true;
		}

		if (str_contains($request->error, "unsupported filter type 'stringlist' on attribute")) {
			return true;
		}

		if (str_contains($request->error, 'unexpected LIKE')) {
			return true;
		}

		if (str_contains($request->error, "unexpected '(' near '(")
			&& stripos($request->payload, 'coalesce') !== false
		) {
			return true;
		}

		if (str_contains($request->error, "unexpected '(' near '(")
			&& stripos($request->payload, 'contains') !== false
		) {
			return true;
		}

		if (str_contains($request->error, "unexpected identifier, expecting ',' or ')' near")
			&& (stripos($request->payload, 'date(') !== false
			|| stripos($request->payload, 'quarter') !== false)) {
			return true;
		}

		return false;
	}
}
