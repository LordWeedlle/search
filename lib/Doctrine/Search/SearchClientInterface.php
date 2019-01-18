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

use Doctrine\Search\Exception\NoResultException;
use Doctrine\Search\Mapping\ClassMetadata;

/**
 * Interface for a Doctrine SearchManager class to implement.
 *
 * @author  Mike Lohmann <mike.h.lohmann@googlemail.com>
 */
interface SearchClientInterface
{
    /**
     * Finds document by id.
     *
     * @param ClassMetadata $class
     * @param mixed $id
     * @param array $options
     * @throws NoResultException
     */
    public function find(ClassMetadata $class, $id, $options = array());

    /**
     * Finds document by specified fields and values.
     *
     * @param ClassMetadata $class
     * @param array $fields
     *
     * @throws NoResultException
     */
    public function findOneBy(ClassMetadata $class, array $fields);

    /**
     * Finds all documents
     *
     * @param ClassMetadata $class
     */
    public function findAll(ClassMetadata $class);

    /**
     * Finds documents by a specific query.
     *
     * @param ClassMetadata $class
     * @param array         $query
     */
    public function search(ClassMetadata $class, array $query);

    /**
     * Creates a document index
     *
     * @param string $name The name of the index.
     * @param array  $config The configuration of the index.
     */
    public function createIndex($name, array $config = array());

    /**
     * Check if an index exists
     *
     * @param string $name The name of the index.
     */
    public function indexExists($name);

    /**
     * Deletes an index and its types and documents
     *
     * @param string $index
     */
    public function deleteIndex($index);

    /**
     * Refresh the index to make documents available for search
     *
     * @param string $index
     */
    public function refreshIndex($index);

    /**
     * Create a document type mapping as defined in the
     * class annotations
     *
     * @param ClassMetadata $metadata
     */
    public function createType(ClassMetadata $metadata);

    /**
     * Adds documents of a given type to the specified index
     *
     * @param ClassMetadata $class
     * @param array $documents Indexed by document id
     */
    public function addDocuments(ClassMetadata $class, array $documents);

    /**
     * Remove documents of a given type from the specified index
     *
     * @param ClassMetadata $class
     * @param array $documents Indexed by document id
     */
    public function removeDocuments(ClassMetadata $class, array $documents);

    /**
     * Remove all documents of a given type from the specified index
     * without deleting the index itself
     *
     * @param ClassMetadata $class
     * @param array $query
     */
    public function removeAll(ClassMetadata $class, array $query = null);
}
