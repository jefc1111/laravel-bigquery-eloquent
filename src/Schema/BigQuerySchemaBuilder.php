<?php

namespace NomanSheikh\LaravelBigqueryEloquent\Schema;

use Closure;
use Illuminate\Database\Schema\Builder;
use LogicException;
use Override;

class BigQuerySchemaBuilder extends Builder
{
    /**
     * Fork addition: upstream throws here, but INFORMATION_SCHEMA makes this answerable.
     *
     * @param  string  $table
     * @return list<string>
     */
    #[Override]
    public function getColumnListing($table)
    {
        $projectId = $this->connection->getProjectId();
        $dataset = $this->connection->getDatabaseName();

        $rows = $this->connection->select(
            "SELECT column_name FROM `{$projectId}.{$dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = ? ORDER BY ordinal_position",
            [$table],
        );

        return array_column($rows, 'column_name');
    }

    /**
     * Fork addition: upstream throws here, but INFORMATION_SCHEMA makes this answerable.
     *
     * @param  string  $table
     * @return bool
     */
    #[Override]
    public function hasTable($table)
    {
        $projectId = $this->connection->getProjectId();
        $dataset = $this->connection->getDatabaseName();

        $rows = $this->connection->select(
            "SELECT COUNT(*) AS count FROM `{$projectId}.{$dataset}.INFORMATION_SCHEMA.TABLES` WHERE table_name = ?",
            [$this->connection->getTablePrefix().$table],
        );

        return ! empty($rows) && $rows[0]->count > 0;
    }

    #[Override]
    public function hasColumn($table, $column): never
    {
        $this->unsupported('hasColumn');
    }

    #[Override]
    public function hasColumns($table, $columns): never
    {
        $this->unsupported('hasColumns');
    }

    #[Override]
    public function getTables($schema = null): never
    {
        $this->unsupported('getTables');
    }

    #[Override]
    public function getColumns($table): never
    {
        $this->unsupported('getColumns');
    }

    #[Override]
    public function create($table, Closure $callback): never
    {
        $this->unsupported('create');
    }

    #[Override]
    public function table($table, Closure $callback): never
    {
        $this->unsupported('table');
    }

    #[Override]
    public function drop($table): never
    {
        $this->unsupported('drop');
    }

    #[Override]
    public function dropIfExists($table): never
    {
        $this->unsupported('dropIfExists');
    }

    #[Override]
    public function rename($from, $to): never
    {
        $this->unsupported('rename');
    }

    protected function unsupported(string $method): never
    {
        throw new LogicException("Schema::{$method}() is not implemented for the BigQuery driver. Run the statement yourself with DB::connection()->statement(), or use the BigQuery client via DB::connection()->getClient().");
    }
}
