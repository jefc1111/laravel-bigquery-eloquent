<?php

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NomanSheikh\LaravelBigqueryEloquent\Schema\BigQuerySchemaBuilder;
use NomanSheikh\LaravelBigqueryEloquent\Tests\Support\BigQuerySpy;

it('exposes a BigQuery schema builder', function () {
    expect(Schema::connection('bigquery'))->toBeInstanceOf(BigQuerySchemaBuilder::class);
});

it('reports a table as existing when INFORMATION_SCHEMA finds it', function () {
    $spy = BigQuerySpy::attach(DB::connection('bigquery'), [['count' => 1]]);

    expect(Schema::connection('bigquery')->hasTable('users'))->toBeTrue()
        ->and($spy->sql)->toContain('`test-project.default_dataset.INFORMATION_SCHEMA.TABLES`')
        ->and($spy->parameters)->toBe(['users']);
});

it('reports a table as missing when INFORMATION_SCHEMA returns a zero count', function () {
    BigQuerySpy::attach(DB::connection('bigquery'), [['count' => 0]]);

    expect(Schema::connection('bigquery')->hasTable('users'))->toBeFalse();
});

it('lists columns in ordinal order from INFORMATION_SCHEMA', function () {
    $spy = BigQuerySpy::attach(DB::connection('bigquery'), [['column_name' => 'id'], ['column_name' => 'name']]);

    expect(Schema::connection('bigquery')->getColumnListing('users'))->toBe(['id', 'name'])
        ->and($spy->sql)->toContain('INFORMATION_SCHEMA.COLUMNS')
        ->and($spy->parameters)->toBe(['users']);
});

it('explains that create is not implemented', function () {
    Schema::connection('bigquery')->create('users', fn () => null);
})->throws(LogicException::class, 'Schema::create() is not implemented for the BigQuery driver');
