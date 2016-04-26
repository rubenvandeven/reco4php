<?php

/**
 * This file is part of the GraphAware Reco4PHP package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Reco4PHP\Filter;

use GraphAware\Common\Result\Record;
use GraphAware\Common\Result\Result;
use GraphAware\Common\Type\Node;
use GraphAware\Common\Type\NodeInterface;
use GraphAware\Reco4PHP\Result\Recommendation;
use GraphAware\Reco4PHP\Result\Recommendations;

abstract class RecommendationSetFilter implements Filter
{
    /**
     * @param \GraphAware\Common\Type\NodeInterface $input
     * @param \GraphAware\Reco4PHP\Result\Recommendations $recommendations
     *
     * @return \GraphAware\Common\Cypher\Statement
     */
    abstract public function buildQuery(NodeInterface $input, Recommendations $recommendations);
    
    /**
     * Returns whether or not the recommended node should be included in the recommendation.
     *
     * @param \GraphAware\Common\Type\NodeInterface $input
     * @param \GraphAware\Common\Type\NodeInterface $item
     *
     * @return bool
     */
    abstract public function doInclude(NodeInterface $input, Recommendation $recommendation, Record $record = null);

    final public function handleResultSet(Node $input, Result $result = null, Recommendations $recommendations)
    {
        $recordsMap = [];
        if($result)
        {
            foreach ($result->records() as $i => $record) {
                $recordsMap[$record->get($this->idResultName())] = $record;
            }
        }


        $to_remove = [];
        foreach ($recommendations->getItems() as $recommendation) {
            $record = array_key_exists($recommendation->item()->identity(), $recordsMap) ? $recordsMap[$recommendation->item()->identity()] : null;
            if(!$this->doInclude($input, $recommendation, $record))
            {
                $to_remove[] = $recommendation->item()->identity();
            }
        }
        return $to_remove;
    }

    public function idResultName()
    {
        return "id";
    }
}