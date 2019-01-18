<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\Search\ElasticSearch;

use Doctrine\Search\Exception\UnknownFieldException;
use Doctrine\Search\SearchClientInterface;
use Doctrine\Search\Mapping\ClassMetadata;
use Doctrine\Search\Exception\NoResultException;

use Elasticsearch\Client as ESClient;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;

use Exception;

/**
 * SearchManager for ElasticSearch-Backend
 *
 * @author  Mike Lohmann <mike.h.lohmann@googlemail.com>
 * @author  Markus Bachmann <markus.bachmann@bachi.biz>
 */
class Client implements SearchClientInterface
{
    const ELASTIC_SEARCH_MAX_RETRY = 5;

    /**
     * @var ESClient
     */
    private $client;

    private $bulkData    = array();

    /**
     * Client constructor.
     *
     * @param string $hosts
     * @param int    $defaultPort
     */
    public function __construct(string $hosts, int $defaultPort)
    {
        $this->client = ClientBuilder::create()
            ->allowBadJSONSerialization()
            ->setHosts(
                array_map(
                   function($host) use ($defaultPort) {
                       list($host, $port) = array_pad(explode(':', $host, 2), 2, null);
                       if (!$port)
                           $port = $defaultPort;

                       return "$host:$port";
                   },
                    explode(',', $hosts)
                )
            )
            ->build()
        ;
    }

    /**
     * @return ESClient
     */
    public function getClient(): ESClient
    {
        return $this->client;
    }

    /**
     * {@inheritDoc}
     *
     * @throws Missing404Exception
     * @throws UnknownFieldException
     * @return Client
     */
    public function addDocuments(ClassMetadata $class, array $documents): Client
    {
        foreach ($documents as $id => $document) {
            $this->checkParameters($class, $document);

            $this->addUpsertBulkData(
                $class->index,
                $class->type,
                $id,
                $document
            );
        }

        return $this->sendBulkData();
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function removeDocuments(ClassMetadata $class, array $documents): Client
    {
        foreach (array_keys($documents) as $id)
            $this->addDeleteBulkData(
                $class->index,
                $class->type,
                $id
            );

        return $this->sendBulkData();
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function removeAll(ClassMetadata $class, array $query = null): Client
    {
        $this->deleteByQuery($class->index, $class->type, $query);

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NoResultException
     */
    public function find(ClassMetadata $class, $id, $options = []): array
    {
        try {
            return $this->getClient()->get(
                [
                    'index' => $class->index,
                    'id'    => $id,
                    'type'  => $class->type
                ]
            );
        } catch (Missing404Exception $ex) {
            throw new NoResultException;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws NoResultException
     * @throws UnknownFieldException
     * @throws Exception
     *
     * @return array
     */
    public function findOneBy(ClassMetadata $class, array $fields): array
    {
        $this->checkParameters($class, $fields);

        $must = [];

        foreach ($fields as $field => $value)
            $must[] = $this->equalsQuery($field, $value);

        $result = $this->search($class, $this->buildQuery($this->andQuery($must)));

        return $this->getSingleResult($result);
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     *
     * @return array
     */
    public function findAll(ClassMetadata $class): array
    {
        $result = $this->search($class, $this->buildQuery($this->matchAllQuery()));

        return $this->getArrayResult($result);
    }

    /**
     * {@inheritDoc}
     */
    public function search(ClassMetadata $class, array $query): array
    {
        return $this->getClient()->search(
            $this->buildBody($class, $query)
        );
    }

    /**
     * {@inheritDoc}
     *
     * @return array
     */
    public function createIndex($name, array $config = array()): array
    {
        return $this
            ->getClient()
            ->indices()
            ->create(
                [
                    'index' => $name,
                    'body'  => $config
                ]
            )
        ;
    }

    /**
     * {@inheritDoc}
     *
     * @return bool
     */
    public function indexExists($name): bool
    {
        return $this
            ->getClient()
            ->indices()
            ->exists(
                [
                    'index' => $name
                ]
            )
        ;
    }

    /**
     * {@inheritDoc}
     *
     * @return array
     */
    public function deleteIndex($index): array
    {
        return $this
            ->getClient()
            ->indices()
            ->delete(
                [
                    'index' => $index
                ]
            )
        ;
    }

    /**
     * {@inheritDoc}
     *
     * @return array
     */
    public function refreshIndex($index): array
    {
        return $this
            ->getClient()
            ->indices()
            ->refresh(
                [
                    'index' => $index
                ]
            )
        ;
    }

    /**
     * {@inheritDoc}
     *
     * @return array
     */
    public function createType(ClassMetadata $metadata): array
    {
        $mapping = $this->getRootMapping($metadata->rootMappings);

        if (!isset($mapping['mapping']))
            $mapping['mapping'] = $this->getMapping($metadata->fieldMappings);

        if (isset($metadata->boost))
            $mapping['_boost'] = [
                'name' => '_boost',
                'null_value' => $metadata->boost
            ];

        if (isset($metadata->parent))
            $mapping['_prent'] = $metadata->parent;

        return $this
            ->getClient()
            ->indices()
            ->putMapping(
                $this->buildBody($metadata, $mapping)
            )
        ;
    }

    /**
     * Generates property mapping from entity annotations
     *
     * @param array $mappings
     *
     * @return array
     */
    protected function getMapping($mappings): array
    {
        $properties = array();

        foreach ($mappings as $propertyName => $fieldMapping) {
            if (isset($fieldMapping['fieldName'])) {
                $propertyName = $fieldMapping['fieldName'];
            }

            if (isset($fieldMapping['type'])) {
                $properties[$propertyName]['type'] = $fieldMapping['type'];
 
                if ($fieldMapping['type'] == 'attachment' && isset($fieldMapping['fields'])) {
                    $callback = function ($field) {
                        unset($field['type']);
                        return $field;
                    };
                    $properties[$propertyName]['fields'] = array_map($callback, $this->getMapping($fieldMapping['fields']));
                }

                if ($fieldMapping['type'] == 'multi_field' && isset($fieldMapping['fields'])) {
                    $properties[$propertyName]['fields'] = $this->getMapping($fieldMapping['fields']);
                }
  
                if (in_array($fieldMapping['type'], array('nested', 'object')) && isset($fieldMapping['properties'])) {
                    $properties[$propertyName]['properties'] = $this->getMapping($fieldMapping['properties']);
                }
            }

            if (isset($fieldMapping['path'])) {
                $properties[$propertyName]['path'] = $fieldMapping['path'];
            }

            if (isset($fieldMapping['includeInAll'])) {
                $properties[$propertyName]['include_in_all'] = $fieldMapping['includeInAll'];
            }

            if (isset($fieldMapping['nullValue'])) {
                $properties[$propertyName]['null_value'] = $fieldMapping['nullValue'];
            }

            if (isset($fieldMapping['store'])) {
                $properties[$propertyName]['store'] = $fieldMapping['store'];
            }

            if (isset($fieldMapping['index'])) {
                $properties[$propertyName]['index'] = $fieldMapping['index'];
            }

            if (isset($fieldMapping['boost'])) {
                $properties[$propertyName]['boost'] = $fieldMapping['boost'];
            }

            if (isset($fieldMapping['analyzer'])) {
                $properties[$propertyName]['analyzer'] = $fieldMapping['analyzer'];
            }

            if (isset($fieldMapping['indexName'])) {
                $properties[$propertyName]['index_name'] = $fieldMapping['indexName'];
            }

            if (isset($fieldMapping['geohash'])) {
                $properties[$propertyName]['geohash'] = $fieldMapping['geohash'];
            }

            if (isset($fieldMapping['geohash_precision'])) {
                $properties[$propertyName]['geohash_precision'] = $fieldMapping['geohash_precision'];
            }

            if (isset($fieldMapping['geohash_prefix'])) {
                $properties[$propertyName]['geohash_prefix'] = $fieldMapping['geohash_prefix'];
            }
        }

        return $properties;
    }

    /**
     * @param ClassMetadata $class
     * @param array         $fields
     *
     * @throws UnknownFieldException
     */
    private function checkParameters(ClassMetadata $class, array $fields)
    {
        $parameters = $this->getParameters($class->parameters);
        foreach ($fields as $field => $value)
            if (!in_array($field, array_keys($parameters)))
                throw new UnknownFieldException($class->index, $class->type, $field);
    }

    /**
     * Generates parameter mapping from entity annotations
     *
     * @param array $paramMapping
     *
     * @return array
     */
    protected function getParameters(array $paramMapping): array
    {
        $parameters = [];

        foreach ($paramMapping as $propertyName => $mapping) {
            $paramName = isset($mapping['fieldName']) ? $mapping['fieldName'] : $propertyName;
            $parameters[$paramName] = $propertyName;
        }

        return $parameters;
    }

    /**
     * Generates root mapping from entity annotations
     *
     * @param array $mappings
     *
     * @return array
     */
    protected function getRootMapping($mappings): array
    {
        $properties = array();

        foreach ($mappings as $rootMapping) {
            $propertyName = $rootMapping['fieldName'];
            $mapping = array();

            if (isset($rootMapping['value'])) {
                $mapping = $rootMapping['value'];
            }

            if (isset($rootMapping['match'])) {
                $mapping['match'] = $rootMapping['match'];
            }

            if (isset($rootMapping['pathMatch'])) {
                $mapping['path_match'] = $rootMapping['pathMatch'];
            }

            if (isset($rootMapping['unmatch'])) {
                $mapping['unmatch'] = $rootMapping['unmatch'];
            }

            if (isset($rootMapping['pathUnmatch'])) {
                $mapping['path_unmatch'] = $rootMapping['pathUnmatch'];
            }

            if (isset($rootMapping['matchPattern'])) {
                $mapping['match_pattern'] = $rootMapping['matchPattern'];
            }

            if (isset($rootMapping['matchMappingType'])) {
                $mapping['match_mapping_type'] = $rootMapping['matchMappingType'];
            }

            if (isset($rootMapping['mapping'])) {
                $mapping['mapping'] = current($this->getMapping(array($rootMapping['mapping'])));
            }

            if (isset($rootMapping['id'])) {
                $properties[$propertyName][][$rootMapping['id']] = $mapping;
            } else {
                $properties[$propertyName] = $mapping;
            }
        }

        return $properties;
    }

    /**
     * Craft body from class matadata and query
     *
     * @param ClassMetadata $class
     * @param array         $query
     *
     * @return array
     */
    private function buildBody(ClassMetadata $class, array $query): array
    {
        return [
            'index' => $class->index,
            'type'  => $class->type,
            'body'  => $query
        ];
    }

    /**
     * Craft query from class matadata and query
     *
     * @param array $query
     *
     * @throws Exception
     *
     * @return array
     */
    private function buildQuery(array $query): array
    {
        return [
            'query' => $query
        ];
    }

    /**
     * Compute common data
     *
     * @param string $index
     * @param string $id
     * @param string $type
     * @param string $prefix
     *
     * @return array
     */
    private function prepareData(string $index, string $type, string $id, string $prefix = ''): array
    {
        return [
            $prefix . 'index' => $index,
            $prefix . 'type'  => $type,
            $prefix . 'id'    => $id
        ];
    }

    /**
     * Add _retry_on_conflict security to common data
     *
     * @param string $index
     * @param string $type
     * @param string $id
     * @param string $prefix
     *
     * @return array
     */
    private function prepareInsertData(string $index, string $type, string $id, string $prefix = ''): array
    {
        $data = $this->prepareData($index, $type, $id, $prefix);
        $data[$prefix . 'retry_on_conflict'] = self::ELASTIC_SEARCH_MAX_RETRY;

        return $data;
    }

    /**
     * Add upsert data to send for bulk purpose
     *
     * @param string $index
     * @param string $type
     * @param string $id
     * @param array  $data
     *
     * @return Client
     */
    public function addUpsertBulkData(string $index, string $type, string $id, array $data = []): Client
    {
        $index = [
            'update' => $this->prepareInsertData($index, $type, $id, '_')
        ];

        if (!empty($data)) {
            if (!isset($this->bulkData['body']))
                $this->bulkData['body'] = [];

            $this->bulkData['body'][] = $index;
            $this->bulkData['body'][] = [
                'doc' => $this->formatData($data),
                'doc_as_upsert' => true
            ];
        }

        return $this;
    }

    /**
     * Add delete data to send for bulk purpose
     *
     * @param string $index
     * @param string $type
     * @param string $id
     *
     * @return Client
     */
    public function addDeleteBulkData(string $index, string $type, string $id): Client
    {
        $this->bulkData['body'][] = [
            'delete' => $this->prepareData($index, $type, $id, '_')
        ];

        return $this;
    }

    // FIXME: Exceptions!!
    /**
     * Send bulk data to ES
     *
     * @return Client
     */
    public function sendBulkData(): Client
    {
        if (empty($this->bulkData))
            return;

        try {
            $response = $this->bulk($this->bulkData);

            if (isset($response['errors']) && $response['errors'])
                throw new ElasticSearchRequestErrorException('Elastic search request error, response: ' . json_encode($response));
        } catch (ElasticsearchException $e) {
            if ($e instanceof Missing404Exception && $this->allowNotFound($action))
                return;

            $canRetryExceptions = [
                MaxRetriesException::class,
                NoNodesAvailableException::class,
                NoShardAvailableException::class,
                RequestTimeout408Exception::class
            ];

            if (in_array(get_class($e), $canRetryExceptions))
                throw new ElasticSearchReschedulableException('An elastic search reschedulable exception occurs: ' . get_class($e), 0, $e);

            throw $e;
        }

        $this->bulkData = [];

        return $this;
    }

    /**
     * Format xxxByQuery data
     *
     * @param string $index
     * @param string $type
     * @param array  $query
     * @param array  $data
     *
     * @return array
     */
    public function formatBatchByQueryData(string $index, string $type, array $query, array $data = [])
    {
        $source = '';
        $body   = [
            'query' => $query
        ];

        if ($data) {
            foreach ($data as $property => $datum)
                $source .= "ctx._source['$property'] = params.$property;";

            $body['script'] = [
                'inline' => $source,
                'params' => $data
            ];
        }

        return [
            'index'     => $index,
            'type'      => $type,
            'conflicts' => 'proceed',
            'body'      => $body
        ];
    }

    /**
     * Update multiple documents corresponding to $data
     *
     * @param string $index
     * @param string $type
     * @param array  $query
     * @param array  $data
     *
     * @return array
     */
    public function updateByQuery(string $index, string $type, array $query, array $data): array
    {
        return $this->getClient()->updateByQuery(
            $this->formatBatchByQueryData(
                $index,
                $type,
                $query,
                $data
            )
        );
    }

    /**
     * Delete multiple documents corresponding to $data
     *
     * @param string $index
     * @param string $type
     * @param array  $query
     *
     * @return array
     */
    public function deleteByQuery(string $index, string $type, array $query): array
    {
        return $this->getClient()->deleteByQuery(
            $this->formatBatchByQueryData(
                $index,
                $type,
                $query
            )
        );
    }

    /**
     * Parse ES array of results
     *
     * @param array $result
     *
     * @return array
     */
    public function getArrayResult(array $result): array
    {
        if (empty($result['hits']['hits']))
            return [];

        return array_column($result['hits']['hits'], '_source');
    }

    /**
     * Parse ES single result
     *
     * @param array $result
     *
     * @throws NoResultException
     * @return array
     */
    public function getSingleResult(array $result): array
    {
        $results = $this->getArrayResult($result);

        if (empty($results))
            throw new NoResultException();

        return $results[0];
    }

    /**
     * @return array
     */
    public function matchAllQuery(): array
    {
        return [
            'match_all' => (object)[]
        ];
    }

    /**
     * Ensure there are arguments given to function
     *
     * @param array  $args
     * @param string $functionName
     *
     * @throws Exception
     */
    public function checkEmptyArguments(array $args, string $functionName)
    {
        if (empty($args))
            throw new Exception("Now arguments given to $functionName");
    }

    /**
     * Format an "and" statement for ES
     *
     * @return array
     * @throws Exception
     */
    public function andQuery(): array
    {
        $args = func_get_args();

        $this->checkEmptyArguments($args, 'andQuery');

        return [
            'bool' => [
                'must' => $args
            ]
        ];
    }

    /**
     * Format an "equals" statement for ES
     *
     * @param string     $field
     * @param            $value
     * @param float|null $boost
     *
     * @return array
     */
    public function equalsQuery(string $field, $value, float $boost = null): array
    {
        if (!is_null($boost))
            $value = [
                'query' => $value,
                'boost' => $boost
            ];

        return [
            'match' => [
                $field => $value
            ]
        ];
    }
}
