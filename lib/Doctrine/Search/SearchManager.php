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

use Doctrine\Common\Persistence\Mapping\MappingException;
use Doctrine\Common\Persistence\ObjectManager;
use Doctrine\ORM\EntityManager;
use Doctrine\Search\Exception\UnexpectedTypeException;
use Doctrine\Common\EventManager;
use Doctrine\Search\Mapping\ClassMetadata;
use Doctrine\Search\Mapping\ClassMetadataFactory;

use ReflectionException;

/**
 * Interface for a Doctrine SearchManager class to implement.
 *
 * @author  Mike Lohmann <mike.h.lohmann@googlemail.com>
 */
class SearchManager implements ObjectManager
{
    /**
     * @var SearchClientInterface
     */
    private $client;

    /**
     * @var Configuration $configuration
     */
    private $configuration;

    /**
     * @var ClassMetadataFactory
     */
    private $metadataFactory;

    /**
     * @var SerializerInterface
     */
    private $serializer;

    /**
     * @var ObjectManager
     */
    private $entityManager;

    /**
     * The event manager that is the central point of the event system.
     *
     * @var EventManager
     */
    private $eventManager;

    /**
     * The EntityRepository instances.
     *
     * @var array
     */
    private $repositories = array();

    /**
     * The UnitOfWork used to coordinate object-level transactions.
     *
     * @var UnitOfWork
     */
    private $unitOfWork;

    /**
     * Constructor
     *
     * @param Configuration         $config
     * @param SearchClientInterface $client
     * @param EventManager          $eventManager
     */
    public function __construct(Configuration $config, SearchClientInterface $client, EventManager $eventManager)
    {
        $this->configuration = $config;
        $this->client = $client;
        $this->eventManager = $eventManager;

        $this->metadataFactory = $this->configuration->getClassMetadataFactory();
        $this->metadataFactory->setSearchManager($this);
        $this->metadataFactory->setConfiguration($this->configuration);
        $this->metadataFactory->setCacheDriver($this->configuration->getMetadataCacheImpl());

        $this->serializer = $this->configuration->getEntitySerializer();
        $this->entityManager = $this->configuration->getEntityManager();

        $this->unitOfWork = new UnitOfWork($this);
    }

    /**
     * Inject a Doctrine 2 object manager
     *
     * @param ObjectManager $om
     */
    public function setEntityManager(ObjectManager $om)
    {
        $this->entityManager = $om;
    }

    /**
     * @return ObjectManager|EntityManager
     */
    public function getEntityManager(): ObjectManager
    {
        return $this->entityManager;
    }

    /**
     * @return Configuration
     */
    public function getConfiguration(): Configuration
    {
        return $this->configuration;
    }

    /**
     * Gets the UnitOfWork used by the SearchManager to coordinate operations.
     *
     * @return UnitOfWork
     */
    public function getUnitOfWork(): UnitOfWork
    {
        return $this->unitOfWork;
    }

    /**
     * Gets the EventManager used by the SearchManager.
     *
     * @return EventManager
     */
    public function getEventManager(): EventManager
    {
        return $this->eventManager;
    }

    /**
     * Loads class metadata for the given class
     *
     * @param string $className
     *
     * @throws MappingException
     * @throws ReflectionException
     *
     * @return ClassMetadata
     */
    public function getClassMetadata($className): ClassMetadata
    {
        return $this->metadataFactory->getMetadataFor($className);
    }

    /**
     * @return ClassMetadataFactory
     */
    public function getClassMetadataFactory(): ClassMetadataFactory
    {
        return $this->metadataFactory;
    }

    /**
     * @return SearchClientInterface
     */
    public function getClient(): SearchClientInterface
    {
        return $this->client;
    }

    /**
     * @return SerializerInterface
     */
    public function getSerializer(): SerializerInterface
    {
        return $this->serializer;
    }

    /**
     * @return ClassMetadataFactory
     */
    public function getMetadataFactory(): ClassMetadataFactory
    {
        return $this->metadataFactory;
    }

    /**
     * {@inheritDoc}
     *
     * @throws MappingException
     * @throws ReflectionException
     */
    public function find($entityName, $id)
    {
        if (is_array($id)) {
            if (!isset($id['id']))
                throw new \InvalidArgumentException('An "id" field is required');

            $options = $id;
        } else {
            $options = [$id];
        }

        $class = $this->getClassMetadata($entityName);

        return $this->unitOfWork->load($class, $options);
    }

    /**
     * Adds the object to the index
     *
     * @param array|object $objects
     *
     * @throws UnexpectedTypeException
     */
    public function persist($objects)
    {
        if (!is_array($objects) && !$objects instanceof \Traversable) {
            $objects = array($objects);
        }
        foreach ($objects as $object) {
            if (!is_object($object)) {
                throw new UnexpectedTypeException($object, 'object');
            }
            $this->unitOfWork->persist($object);
        }
    }

    /**
     * Remove the object from the index
     *
     * @param array|object $objects
     *
     * @throws UnexpectedTypeException
     */
    public function remove($objects)
    {
        if (!is_array($objects) && !$objects instanceof \Traversable) {
            $objects = array($objects);
        }
        foreach ($objects as $object) {
            if (!is_object($object)) {
                throw new UnexpectedTypeException($object, 'object');
            }
            $this->unitOfWork->remove($object);
        }
    }

    /**
     * Commit all changes
     */
    public function flush($object = null)
    {
        $this->unitOfWork->commit($object);
    }

    /**
     * Gets the repository for an entity class.
     *
     * @param string $entityName The name of the entity.
     * @return EntityRepository The repository class.
     *
     * @throws MappingException
     * @throws ReflectionException
     */
    public function getRepository($entityName)
    {
        if (isset($this->repositories[$entityName])) {
            return $this->repositories[$entityName];
        }

        $metadata = $this->getClassMetadata($entityName);
        $repository = new EntityRepository($this, $metadata);
        $this->repositories[$entityName] = $repository;

        return $repository;
    }

    /**
     * Gets a collection of entity repositories.
     *
     * @param array $entityNames The names of the entities.
     * @return EntityRepositoryCollection The repository class.
     *
     * @throws MappingException
     * @throws ReflectionException
     */
    public function getRepositories(array $entityNames)
    {
        $repositoryCollection = new EntityRepositoryCollection($this);

        foreach ($entityNames as $entityName)
            $repositoryCollection->addRepository($this->getRepository($entityName));

        return $repositoryCollection;
    }

    /**
     * Returns a search engine Query wrapper which can be executed
     * to retrieve results.
     *
     * @return Query
     */
    public function createQuery(): Query
    {
        return new Query($this);
    }

    public function initializeObject($obj)
    {
    }

    public function contains($object)
    {
    }

    public function merge($object)
    {
    }

    /**
     * Clears the SearchManager. All entities that are currently managed
     * by this EntityManager become detached.
     *
     * @param string $objectName if given, only entities of this type will get detached
     */
    public function clear($objectName = null)
    {
        $this->unitOfWork->clear($objectName);
    }

    public function detach($object)
    {
    }

    public function refresh($object)
    {
    }
}
