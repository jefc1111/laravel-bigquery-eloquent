<?php

namespace NomanSheikh\LaravelBigqueryEloquent;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryResults;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Processors\Processor;
use NomanSheikh\LaravelBigqueryEloquent\Query\BigQueryGrammar;
use NomanSheikh\LaravelBigqueryEloquent\Query\BigQueryProcessor;
use NomanSheikh\LaravelBigqueryEloquent\Query\BigQueryQueryBuilder;
use NomanSheikh\LaravelBigqueryEloquent\Schema\BigQuerySchemaBuilder;
use NomanSheikh\LaravelBigqueryEloquent\Schema\BigQuerySchemaGrammar;
use Override;

class BigQueryConnection extends Connection
{
    protected BigQueryClient $client;

    protected QueryResults $result;

    protected string $projectId;

    protected string $dataset;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->projectId = (string) ($config['project_id'] ?? '');
        $this->dataset = (string) ($config['dataset'] ?? '');

        $this->client = new BigQueryClient([
            'projectId' => $this->projectId,
            'keyFilePath' => (string) ($config['key_file'] ?? ''),
        ]);

        $this->database = $this->dataset;

        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }

    public function getDefaultDataset(): string
    {
        return $this->dataset;
    }

    public function getProjectId(): string
    {
        return $this->projectId;
    }

    public function getDatabaseName(): string
    {
        return $this->dataset;
    }

    public function getClient(): BigQueryClient
    {
        return $this->client;
    }

    #[Override]
    public function table($table, $as = null): BigQueryQueryBuilder
    {
        $query = new BigQueryQueryBuilder($this, $this->getQueryGrammar(), $this->getPostProcessor());

        return $query->from($table);
    }

    #[Override]
    protected function getDefaultQueryGrammar(): BigQueryGrammar
    {
        return new BigQueryGrammar($this);
    }

    #[Override]
    public function getSchemaBuilder(): BigQuerySchemaBuilder
    {
        return new BigQuerySchemaBuilder($this);
    }

    #[Override]
    protected function getDefaultPostProcessor(): Processor
    {
        return new BigQueryProcessor;
    }

    #[Override]
    protected function getDefaultSchemaGrammar(): BigQuerySchemaGrammar
    {
        return new BigQuerySchemaGrammar($this);
    }

    public function getDriverName(): string
    {
        return 'bigquery';
    }

    public function getDriverTitle(): string
    {
        return 'BigQuery';
    }

    public function getPdo(): null
    {
        return null;
    }

    public function getReadPdo(): null
    {
        return null;
    }

    /**
     * Execute a select query using the BigQuery client and return rows as stdClass objects.
     * Compatible signature with Connection::select($query, $bindings = [], $useReadPdo = true)
     *
     * NOTE: This method was copied across from https://github.com/docteuri/laravel-bigquery-eloquent/commit/daca7dbe276ca12e4d07d95982050d302a3d7853
     * taken from the fork by user docteuri.
     *
     * @param string $query
     * @param array $bindings
     * @param bool $useReadPdo
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        $job = $this->client->query($query);

        if (! empty($bindings)) {
            $job = $job->parameters($bindings);
        }

        $result = $this->client->runQuery($job);

        $out = [];
        foreach ($result as $row) {
            $out[] = (object) ((array) $row);
        }

        $this->logQuery($query, $bindings, 0);

        return $out;
    }
}
