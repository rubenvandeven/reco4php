<?php

/**
 * This file is part of the GraphAware Reco4PHP package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Reco4PHP\Post;

use GraphAware\Common\Result\Record;
use GraphAware\Common\Result\Result;
use GraphAware\Common\Type\Node;
use GraphAware\Common\Type\NodeInterface;
use GraphAware\Reco4PHP\Result\Recommendation;
use GraphAware\Reco4PHP\Result\Recommendations;

abstract class RecommendationSetPostProcessor implements PostProcessor
{
    /**
     * @param \GraphAware\Common\Type\NodeInterface $input
     * @param \GraphAware\Reco4PHP\Result\Recommendations $recommendations
     *
     * @return \GraphAware\Common\Cypher\Statement
     */
    abstract public function buildQuery(NodeInterface $input, Recommendations $recommendations);

    abstract public function postProcess(Node $input, Recommendation $recommendation, Record $record);

    final public function handleResultSet(Node $input, Result $result, Recommendations $recommendations)
    {
        $recordsMap = [];
        foreach ($result->records() as $i => $record) {
            $recordsMap[$record->get($this->idResultName())] = $record;
        }

        foreach ($recommendations->getItems() as $recommendation) {
            if (array_key_exists($recommendation->item()->identity(), $recordsMap)) {
                $this->postProcess($input, $recommendation, $recordsMap[$recommendation->item()->identity()]);
            }
        }
    }

    public function idResultName()
    {
        return "id";
    }
}