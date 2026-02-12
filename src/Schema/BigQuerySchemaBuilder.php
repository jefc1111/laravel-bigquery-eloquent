<?php

namespace NomanSheikh\LaravelBigqueryEloquent\Schema;

use Illuminate\Database\Schema\Builder;
use Override;

class BigQuerySchemaBuilder extends Builder
{
    /**
     * Get the column listing for a given table.
     *
     * @param  string  $table
     * @return list<string>
     */
    #[Override]
    public function getColumnListing($table)
    {
        $projectId = $this->connection->getProjectId();
        $dbName = $this->connection->getDatabaseName();

        $query = "
            SELECT 
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM `$projectId.$dbName.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '$table'
            ORDER BY ordinal_position
        ";

        return array_column($this->connection->select($query), 'column_name');
    }

    /**
     * Determine if the given table exists.
     *
     * @param  string  $table
     * @return bool
     */
    #[Override]
    public function hasTable($table)
    {
        $projectId = $this->connection->getProjectId();
        $dbName = $this->connection->getDatabaseName();

        // Add table prefix if configured
        $table = $this->connection->getTablePrefix() . $table;

        $query = "
            SELECT COUNT(*) as count
            FROM `$projectId.$dbName.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '$table'
        ";

        $result = $this->connection->select($query);

        return ! empty($result) && $result[0]->count > 0;
    }
}

