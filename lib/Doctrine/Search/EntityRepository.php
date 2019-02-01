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

use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\Common\Persistence\ObjectRepository;
use Doctrine\Search\Exception\NoResultException;
use Doctrine\Search\Mapping\ClassMetadata;

use Elastica\Query as BaseQuery;
use Elastica\Query\AbstractQuery;
use Elastica\Query\BoolQuery;
use Elastica\Query\Term;

use Iterator;
use UnexpectedValueException;

class EntityRepository implements ObjectRepository
{
    /**
     * @var string
     */
    protected $_entityName;

    /**
     * @var ClassMetadata
     */
    private $_class;

    /**
     * @var SearchManager
     */
    private $_sm;

    public function __construct(SearchManager $sm, ClassMetadata $class)
    {
        $this->_sm         = $sm;
        $this->_entityName = $class->className;
        $this->_class      = $class;
    }

    /**
     * Finds an object by its primary key / identifier.
     *
     * @param string $id The identifier.
     * @return object The object.
     */
    public function find($id)
    {
        return $this->getSearchManager()->getUnitOfWork()->find($this->getClassMetadata(), $id);
    }

    /**
     * Return an Iterator to find all objects in the repository.
     *
     * @return Iterator
     */
    public function findAll()
    {
        return $this->getSearchManager()->getUnitOfWork()->findAll($this->getClassMetadata());
    }

    /**
     * Finds objects by a set of criteria.
     *
     * Optionally sorting and limiting details can be passed. An implementation may throw
     * an UnexpectedValueException if certain values of the sorting or limiting details are
     * not supported.
     *
     * @throws UnexpectedValueException
     *
     * @param array $criteria
     * @param array|null $orderBy
     * @param int|null $limit
     * @param int|null $offset
     *
     * @return array|ArrayCollection The objects.
     */
    public function findBy(array $criteria, array $orderBy = null, $limit = null, $offset = null)
    {
        $must = new BoolQuery();

        foreach ($criteria as $field => $value)
            $must->addMust(new Term([$field => $value]));

        $baseQuery = new BaseQuery($must);

        $query = new Query($this->getSearchManager());
        $query->searchWith($baseQuery);

        return $this->getSearchManager()->getUnitOfWork()->loadCollection($this->getClassMetadata(), $query);
    }

    /**
     * Finds a single object by a set of criteria.
     *
     * @param array $criteria
     *
     * @return object
     * @throws NoResultException
     */
    public function findOneBy(array $criteria)
    {
        return $this->getSearchManager()->getUnitOfWork()->load($this->getClassMetadata(), ['fields' => $criteria]);
    }

    /**
     * Execute a direct search query on the associated index and type
     *
     * @param AbstractQuery $query
     *
     * @return array|ArrayCollection
     */
    public function search(AbstractQuery $query)
    {
        return $this->getSearchManager()->getUnitOfWork()->loadCollection($this->getClassMetadata(), $query);
    }

    /**
     * Execute a direct delete by query on the associated index and type
     *
     * @param array $query
     */
    public function delete(array $query)
    {
        $this->getSearchManager()->getClient()->removeAll($this->getClassMetadata(), $query);
    }

    /**
     * Returns the class name of the object managed by the repository
     *
     * @return string
     */
    public function getClassName(): string
    {
        return $this->_entityName;
    }

    /**
     * Returns the class metadata managed by the repository
     *
     * @return ClassMetadata
     */
    public function getClassMetadata(): ClassMetadata
    {
        return $this->_class;
    }

    /**
     * Returns the search manager
     *
     * @return SearchManager
     */
    public function getSearchManager(): SearchManager
    {
        return $this->_sm;
    }
}
