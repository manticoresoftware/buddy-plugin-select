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
use Manticoresearch\Buddy\Core\Network\Request as NetworkRequest;
use Manticoresearch\Buddy\Core\Plugin\Request as BaseRequest;

final class Request extends BaseRequest {
	const HANDLED_TABLES = [
		'information_schema.files',
		'information_schema.tables',
		'information_schema.triggers',
	];

	public string $path;

	/** @var string */
	public string $table;

	/** @var array<string> */
	public array $fields = [];

	/** @var array<string,array{operator:string,value:int|string|bool}> */
	public array $where = [];

	public function __construct() {
	}

  /**
	 * @param NetworkRequest $request
	 * @return static
	 * @throws QueryParseError
	 */
	public static function fromNetworkRequest(NetworkRequest $request): static {
		$self = new static();
		$self->path = $request->path;

		// Match fields
		preg_match(
			'/^SELECT\s+(.*?)\s+FROM\s+([a-z][a-z\_\-0-9]*(\.[a-z][a-z\_\-0-9]*)?)/i',
			$request->payload,
			$matches
		);
		$self->table = strtolower($matches[2]);
		preg_match_all('/(\w+)/i', $matches[1], $matches);
		$self->fields = $matches[1];

		// Match WHERE statements
		$matches = [];
		preg_match_all("/([a-zA-Z0-9_]+)\s*(=|<|>|LIKE)\s*(?:'([^']+)'|([0-9]+))/", $request->payload, $matches);
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
		if (!in_array($self->table, static::HANDLED_TABLES)) {
			throw QueryParseError::create('Failed to handle your select query', true);
		}
		return $self;
	}

	/**
	 * Return columns for response created from parsed fields
	 *
	 * @return array<array<string,array{type:string}>>
	 */
	public function getColumns(): array {
		$columns = [];
		foreach ($this->fields as $field) {
			$columns[] = [
				$field => [
					'type' => 'string',
				],
			];
		}

		return $columns;
	}

	/**
	 * @param NetworkRequest $request
	 * @return bool
	 */
	public static function hasMatch(NetworkRequest $request): bool {
		return stripos($request->payload, 'select') === 0;
	}
}
