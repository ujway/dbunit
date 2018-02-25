<?php
/*
 * This file is part of DbUnit.
 *
 * (c) Sebastian Bergmann <sebastian@phpunit.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace PHPUnit\DbUnit\Operation;

use PHPUnit\DbUnit\Database\Connection;
use PHPUnit\DbUnit\DataSet\IDataSet;
use PHPUnit\DbUnit\DataSet\ITable;
use PHPUnit\DbUnit\DataSet\ITableMetadata;

/**
 * This class provides functionality for inserting rows from a dataset into a database.
 */
class BulkInsert extends RowBased
{
    protected $operationName = 'BULKINSERT';
    private $chunkSize;

    protected function buildOperationQuery(ITableMetadata $databaseTableMetaData, ITable $table, Connection $connection)
    {
        $columnCount = count($table->getTableMetaData()->getColumns());

        if ($columnCount > 0) {
            $placeHolders = implode(', ', array_fill(0, $columnCount, '?'));

            $columns = '';
            foreach ($table->getTableMetaData()->getColumns() as $column) {
                $columns .= $connection->quoteSchemaObject($column) . ', ';
            }

            $columns = substr($columns, 0, -2);

            $query = "
                INSERT INTO {$connection->quoteSchemaObject($table->getTableMetaData()->getTableName())}
                ({$columns})
                VALUES
                ({$placeHolders})";

            $bulk = '';
            for ($i = 1; $i < $this->chunkSize; $i++) {
                $bulk .= "
                ,({$placeHolders})";
            }
            $query .= $bulk;

            return $query;
        } else {
            return false;
        }
    }

    protected function buildOperationArguments(ITableMetadata $databaseTableMetaData, ITable $table, $row)
    {
        $args = [];
        foreach ($table->getTableMetaData()->getColumns() as $columnName) {
            $args[] = $table->getValue($row, $columnName);
        }

        return $args;
    }

    protected function disablePrimaryKeys(ITableMetadata $databaseTableMetaData, ITable $table, Connection $connection)
    {
        if (count($databaseTableMetaData->getPrimaryKeys())) {
            return true;
        }

        return false;
    }

    public function execute(Connection $connection, IDataSet $dataSet)
    {
        $databaseDataSet = $connection->createDataSet();

        $dsIterator = $this->iteratorDirection == self::ITERATOR_TYPE_REVERSE ? $dataSet->getReverseIterator() : $dataSet->getIterator();

        // 1度にInsertする際の上限値
        $defaultChunkSize = 100;
        foreach ($dsIterator as $table) {
            $rowCount = $table->getRowCount();
            if ($rowCount == 0) {
                continue;
            }

            /* @var $table ITable */
            $databaseTableMetaData = $databaseDataSet->getTableMetaData($table->getTableMetaData()->getTableName());
            $disablePrimaryKeys    = $this->disablePrimaryKeys($databaseTableMetaData, $table, $connection);
            if ($disablePrimaryKeys) {
                $connection->disablePrimaryKeys($databaseTableMetaData->getTableName());
            }
            $bulkCount = 0;
            while ($bulkCount < $rowCount) {
                $this->chunkSize = 0;
                $argsList = [];
                for ($chunkCount = 0; $chunkCount < $defaultChunkSize; $chunkCount++) {
                    $rowNumber = $bulkCount + $chunkCount;
                    if ($rowCount <= $rowNumber) {
                        break;
                    }
                    foreach ($this->buildOperationArguments($databaseTableMetaData, $table, $rowNumber) as $argsKey => $argsValue) {
                        $argsList[] = $argsValue;
                    }
                    $this->chunkSize++;
                }
                $query = $this->buildOperationQuery($databaseTableMetaData, $table, $connection);
                if ($query === FALSE) {
                    if ($table->getRowCount() > 0) {
                        throw new Exception($this->operationName, '', [], $table, 'Rows requested for insert, but no columns provided!');
                    }
                    continue;
                }
                try {
                    $statement = $connection->getConnection()->prepare($query);
                    $statement->execute($argsList);
                }
                catch (Exception $e) {
                    throw new Exception(
                        $this->operationName, $query, $argsList, $table, $e->getMessage()
                    );
                }
                $bulkCount += $chunkCount;
            }
            if ($disablePrimaryKeys) {
                $connection->enablePrimaryKeys($databaseTableMetaData->getTableName());
            }
        }
    }
}
