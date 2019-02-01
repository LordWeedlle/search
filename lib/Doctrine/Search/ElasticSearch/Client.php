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

use Doctrine\Search\Exception\DoctrineSearchException;
use Doctrine\Search\SearchClientInterface;
use Doctrine\Search\Mapping\ClassMetadata;

use Elastica\Client as ElasticaClient;
use Elastica\Query\AbstractQuery;
use Elastica\Query\BoolQuery;
use Elastica\Query\Term;
use Elastica\ResultSet;
use Elastica\Scroll;
use Elastica\Type;
use Elastica\Type\Mapping;
use Elastica\Document;
use Elastica\Index;
use Elastica\Query\MatchAll;
use Elastica\Exception\NotFoundException;
use Elastica\Search;

use Exception;
use Iterator;

/**
 * SearchManager for ElasticSearch-Backend
 *
 * @author  Mike Lohmann <mike.h.lohmann@googlemail.com>
 * @author  Markus Bachmann <markus.bachmann@bachi.biz>
 */
class Client implements SearchClientInterface
{
    /**
     * @var ElasticaClient
     */
    private $client;

    /**
     * Client constructor.
     *
     * @param string $hosts
     * @param int    $defaultPort
     *
     * @throws DoctrineSearchException
     */
    public function __construct(string $hosts, int $defaultPort)
    {
        $connections = [];
        $hosts       = explode(',', $hosts);

        if (!$hosts)
            throw new DoctrineSearchException('Cannot initialize client with empty hosts');

        foreach ($hosts as $host) {
            list($host, $port) = array_pad(explode(':', $host, 2), 2, null);

            if (!$port)
                $port = $defaultPort;

            $connections[] = [
                'host' => $host,
                'port' => $port
            ];
        }

        $this->client = new ElasticaClient(['connections' => $connections]);
    }

    /**
     * @return ElasticaClient
     */
    public function getClient(): ElasticaClient
    {
        return $this->client;
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function addDocuments(ClassMetadata $class, array $documents): Client
    {
        $bulk  = [];
        $index = $this->getIndex($class->index);
        $type  = $index->getType($class->type);

        foreach ($documents as $id => $document) {
            $parameters = $this->getParameters($class->parameters);
            $doc        = new Document($id);

            foreach ($parameters as $name => $value) {
                if (isset($document[$value])) {
                    if (method_exists($doc, "set{$name}")) {
                        $doc->{"set{$name}"}($document[$value]);
                    } else {
                        $doc->setParam($name, $document[$value]);
                    }

                    unset($document[$value]);
                }
            }

            $bulk[] = $doc->setData($document);
        }

        if (count($bulk) > 1) {
            $type->addDocuments($bulk);
        } else {
            $type->addDocument($bulk[0]);
        }

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function removeDocuments(ClassMetadata $class, array $documents): Client
    {
        $this
            ->getIndex($class->index)
            ->getType($class->type)
            ->deleteIds(array_keys($documents))
        ;

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function removeAll(ClassMetadata $class, array $query = null): Client
    {
        $this
            ->getIndex($class->index)
            ->getType($class->type)
            ->deleteByQuery($query ?: new MatchAll)
        ;

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @return Document|null
     */
    public function find(ClassMetadata $class, $id, $options = [])
    {
        try {
            return $this
                ->getIndex($class->index)
                ->getType($class->type)
                ->getDocument($id, $options)
            ;
        } catch (NotFoundException $ex) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     *
     * @return array|null
     */
    public function findOneBy(ClassMetadata $class, array $fields)
    {
        $must = new BoolQuery;

        foreach ($fields as $field => $value)
            $must->addMust(new Term([$field => $value]));

        $results = $this->search($class, $must);

        if (empty($results))
            return null;

        return $results[0];
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     *
     * @return array
     */
    public function findBy(ClassMetadata $class, array $fields): array
    {
        $must = new BoolQuery;

        foreach ($fields as $field => $value)
            $must->addMust(new Term([$field => $value]));

        $results = $this->search($class, $must);

        return $results[0];
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     *
     * @return ResultSet
     */
    public function findAll(ClassMetadata $class): ResultSet
    {
        return $this
            ->buildQuery($class)
            ->search()
        ;
    }

    /**
     * @param ClassMetadata $class
     *
     * @return Search
     */
    protected function buildQuery(ClassMetadata $class): Search
    {
        $searchQuery = new Search($this->getClient());
        $searchQuery->setOption(Search::OPTION_VERSION, true);

        if ($class->index) {
            $index = $this->getIndex($class->index);
            $searchQuery->addIndex($index);

            if ($class->type)
                $searchQuery->addType($index->getType($class->type));
        }

        return $searchQuery;
    }

    /**
     * @param ClassMetadata $class
     *
     * @return Scroll
     */
    protected function buildScrollQuery(ClassMetadata $class): Scroll
    {
        $searchQuery = new Search($this->getClient());
        $searchQuery->setOption(Search::OPTION_VERSION, true);

        if ($class->index) {
            $index = $this->getIndex($class->index);
            $searchQuery->addIndex($index);

            if ($class->type)
                $searchQuery->addType($index->getType($class->type));
        }

        return new Scroll($searchQuery);
    }

    /**
     * {@inheritDoc}
     *
     * @return ResultSet
     */
    public function search(ClassMetadata $class, AbstractQuery $query): ResultSet
    {
        return $this
            ->buildQuery($class)
            ->search($query)
        ;
    }

    /**
     * {@inheritDoc}
     */
    public function scrollSearch(ClassMetadata $class): Iterator
    {
        return $this->buildScrollQuery($class);
    }

    /**
     * {@inheritDoc}
     *
     * @return Index
     */
    public function createIndex($name, array $config = array()): Index
    {
        $index = $this->getIndex($name);
        $index->create($config, true);

        return $index;
    }

    /**
     * {@inheritDoc}
     *
     * @return Index
     */
    public function getIndex(string $name): Index
    {
        return $this
            ->getClient()
            ->getIndex($name)
        ;
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function deleteIndex($index): Client
    {
        $this
            ->getIndex($index)
            ->delete()
        ;

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @return Client
     */
    public function refreshIndex($index): Client
    {
        $this
            ->getIndex($index)
            ->refresh()
        ;

        return $this;
    }

    /**
     * {@inheritDoc}
     *
     * @return Type
     */
    public function createType(ClassMetadata $metadata): Type
    {
        $type           = $this->getIndex($metadata->index)->getType($metadata->type);
        $properties     = $this->getMapping($metadata->fieldMappings);
        $rootProperties = $this->getRootMapping($metadata->rootMappings);
        $mapping        = new Mapping($type, $properties);
        $mapping->disableSource($metadata->source);
        if (isset($metadata->boost))
            $mapping->setParam('_boost', ['name' => '_boost', 'null_value' => $metadata->boost]);

        if (isset($metadata->parent))
            $mapping->setParent($metadata->parent);

        foreach ($rootProperties as $key => $value)
            $mapping->setParam($key, $value);

        $mapping->send();

        return $type;
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
}
