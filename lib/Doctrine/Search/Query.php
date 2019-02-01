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

namespace Doctrine\Search;

use Doctrine\Search\Exception\DoctrineSearchException;
use Doctrine\Search\Exception\NoResultException;
use Doctrine\Search\Mapping\ClassMetadata;

use Elastica\Query\AbstractQuery;
use Elastica\Result;
use Elastica\ResultSet;

class Query
{
    const HYDRATE_BYPASS = -1;

    const HYDRATE_INTERNAL = -2;

    const HYDRATION_PARAMETER = 'ids';

    /**
     * @var SearchManager
     */
    protected $sm;

    /**
     * @var AbstractQuery
     */
    protected $query;

    /**
     * @var object
     */
    protected $hydrationQuery;

    /**
     * @var string
     */
    protected $hydrationParameter = self::HYDRATION_PARAMETER;

    /**
     * @var ClassMetadata
     */
    protected $entityClass;

    /**
     * @var integer
     */
    protected $hydrationMode;

    /**
     * @var boolean
     */
    protected $useResultCache;

    /**
     * @var integer
     */
    protected $cacheLifetime;

    /**
     * @var integer
     */
    protected $count;

    /**
     * @var integer
     */
    protected $total;

    public function __construct(SearchManager $sm)
    {
        $this->sm = $sm;
    }

    /**
     * Magic method to pass query building to the underlying query
     * object, saving the need to abstract.
     *
     * @param string $method
     * @param array $arguments
     *
     * @throws DoctrineSearchException
     *
     * @return Query
     */
    public function __call($method, $arguments): Query
    {
        if (!$this->query) {
            throw new DoctrineSearchException('No client query has been provided using Query#searchWith().');
        }

        call_user_func_array(array($this->query, $method), $arguments);
        return $this;
    }

    /**
     * Specifies the searchable entity class to search against.
     *
     * @param string $entityClass
     *
     * @return Query
     */
    public function from(string $entityClass): Query
    {
        $this->entityClass = $entityClass;

        return $this;
    }

    /**
     * Set the query object to be executed on the search engine
     *
     * @param mixed $query
     *
     * @return Query
     */
    public function searchWith($query): Query
    {
        $this->query = $query;

        return $this;
    }

    /**
     * @return SearchManager
     */
    protected function getSearchManager(): SearchManager
    {
        return $this->sm;
    }

    /**
     * Set the hydration mode from the underlying query modes
     * or bypass and return search result directly from the client
     *
     * @param integer $mode
     *
     * @return Query
     */
    public function setHydrationMode($mode): Query
    {
        $this->hydrationMode = $mode;

        return $this;
    }

    /**
     * If hydrating with Doctrine then you can use the result cache
     * on the default or provided query
     *
     * @param boolean $useCache
     * @param integer $cacheLifetime
     *
     * @return Query
     */
    public function useResultCache($useCache, $cacheLifetime = null): Query
    {
        $this->useResultCache = $useCache;
        $this->cacheLifetime = $cacheLifetime;

        return $this;
    }

    /**
     * Return the total hit count for the given query as provided by
     * the search engine.
     *
     * @return int
     */
    public function count(): int
    {
        return $this->count;
    }

    /**
     * Return the total hit count for the given query as provided by
     * the search engine.
     *
     * @return int
     */
    public function total(): int
    {
        return $this->total;
    }

    /**
     * Set a custom Doctrine Query to execute in order to hydrate the search
     * engine results into required entities. The assumption is made the the
     * search engine result id is correlated to the entity id. An optional
     * query parameter override can be specified.
     *
     * @param object $hydrationQuery
     * @param string $parameter
     *
     * @return Query
     */
    public function hydrateWith($hydrationQuery, $parameter = null): Query
    {
        $this->hydrationQuery = $hydrationQuery;

        if ($parameter)
            $this->hydrationParameter = $parameter;

        return $this;
    }

    /**
     * Return a provided hydration query
     *
     * @throws DoctrineSearchException
     *
     * @return object
     */
    protected function getHydrationQuery()
    {
        if (!$this->hydrationQuery)
            throw new DoctrineSearchException('A hydration query is required for hydrating results to entities.');

        return $this->hydrationQuery;
    }

    /**
     * Execute search for single result and hydrate results if required.
     *
     * @param integer $hydrationMode
     * @throws NoResultException
     * @throws DoctrineSearchException
     * @return mixed
     */
    public function getSingleResult($hydrationMode = null)
    {
        $this->query->setSize(1);

        $results = $this->getResult($hydrationMode);

        if (count($results) < 1)
            throw new NoResultException('No results found');

        return $results[0];
    }

    /**
     * Execute search and hydrate results if required.
     *
     * @param integer $hydrationMode
     * @throws DoctrineSearchException
     * @return mixed
     */
    public function getResult($hydrationMode = null)
    {
        if ($hydrationMode)
            $this->hydrationMode = $hydrationMode;

        $resultSet = $this->getSearchManager()->getClient()->search($this->entityClass, $this->query);

        // TODO: abstraction of support for different result sets
        if ($resultSet instanceof ResultSet) {
            $this->count = $resultSet->count();
            $this->total = $resultSet->getTotalHits();
            $results = $resultSet->getResults();
        } else {
            $resultClass = get_class($resultSet);
            throw new DoctrineSearchException("Unexpected result set class '$resultClass'");
        }

        // Return results depending on hydration mode
        if ($this->hydrationMode == self::HYDRATE_BYPASS) {
            return $resultSet;
        } elseif ($this->hydrationMode == self::HYDRATE_INTERNAL) {
            return $this->sm->getUnitOfWork()->hydrateCollection($this->entityClass, $resultSet);
        }

        // Document ids are used to lookup dbms results
        $ids = array_map(
            function (Result $result) {
                return (string) $result->getId();
            },
            $results
        );

        return $this->getHydrationQuery()
            ->setParameter($this->hydrationParameter, $ids ?: null)
            ->useResultCache($this->useResultCache, $this->cacheLifetime)
            ->getResult($this->hydrationMode)
        ;
    }
}
