<?php

/**
 * This file is part of the GraphAware Reco4PHP package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace GraphAware\Reco4PHP\Executor;

use GraphAware\Common\Result\ResultCollection;
use GraphAware\Common\Type\Node;
use GraphAware\Common\Type\NodeInterface;
use GraphAware\Reco4PHP\Persistence\DatabaseService;
use GraphAware\Reco4PHP\Post\CypherAwarePostProcessor;
use GraphAware\Reco4PHP\Post\RecommendationSetPostProcessor;
use GraphAware\Reco4PHP\Result\Recommendations;
use GraphAware\Reco4PHP\Engine\RecommendationEngine;
use Symfony\Component\Stopwatch\Stopwatch;
use GraphAware\Reco4PHP\Filter\RecommendationSetFilter;

class RecommendationExecutor
{
    protected $discoveryExecutor;

    protected $postProcessExecutor;

    protected $stopwatch;

    public function __construct(DatabaseService $databaseService)
    {
        $this->discoveryExecutor = new DiscoveryPhaseExecutor($databaseService);
        $this->postProcessExecutor = new PostProcessPhaseExecutor($databaseService);
        $this->databaseService = $databaseService;
        $this->stopwatch = new Stopwatch();
    }

    public function processRecommendation(NodeInterface $input, RecommendationEngine $engine)
    {
        $recommendations = new Recommendations();
        $this->stopwatch->start('discovery');
        $discoveryResult = $this->discoveryExecutor->processDiscovery($input, $engine->getDiscoveryEngines(), $engine->getBlacklistBuilders());
        $blacklist = $this->buildBlacklistedNodes($discoveryResult, $engine);
        foreach ($engine->getDiscoveryEngines() as $discoveryEngine) {
            $recommendations->merge($discoveryEngine->produceRecommendations($input, $discoveryResult));
        }
        $discoveryTime = $this->stopwatch->stop('discovery');
        //echo $discoveryTime->getDuration() . PHP_EOL;

        $this->stopwatch->start('post_process');
        $postProcessResult = $this->postProcessExecutor->execute($input, $recommendations, $engine);
        foreach ($engine->getPostProcessors() as $postProcessor) {
            if ($postProcessor instanceof CypherAwarePostProcessor) {
                foreach ($recommendations->getItems() as $recommendation) {
                    $tag = sprintf('post_process_%s_%d', $postProcessor->name(), $recommendation->item()->identity());
                    $postProcessor->postProcess($input, $recommendation, $postProcessResult->get($tag));
                }
            }
            elseif($postProcessor instanceof RecommendationSetPostProcessor)
            {
                $tag = $postProcessor->name();
                $result = $postProcessResult->get($tag);
                $postProcessor->handleResultSet($input, $result, $recommendations);
            }
        }
        $pPTime = $this->stopwatch->stop('post_process');
        $recommendations->sort();

        // by removing after postprocessing, the results of the postprocessing
        // can influence the removals. (uses $recommendations->rm() instead of remove())
        $this->removeIrrelevant($input, $engine, $recommendations, $blacklist);

        return $recommendations;
    }

    public function removeIrrelevant(NodeInterface $input, RecommendationEngine $engine, Recommendations $recommendations, array $blacklist)
    {
        // recommendations in blacklist should not be fed to the filters
        foreach ($recommendations->getItems() as $recommendation) {
            if(array_key_exists($recommendation->item()->identity(), $blacklist)) {
                $recommendations->rm($recommendation);
            }
        }

        $stack = $this->databaseService->getDriver()->stack('filter_'.$engine->name());
        foreach ($engine->getFilters() as $filter) {
            if($filter instanceof RecommendationSetFilter) {
                $tag = sprintf('filter_%s', $filter->name());
                $statement = $filter->buildQuery($input, $recommendations);
                if($statement)
                    $stack->push($statement->text(), $statement->parameters(), $tag);
            }
        }

        try {
            $filterResults = $this->databaseService->getDriver()->runStack($stack);
        } catch (\Exception $e) {
            throw new \RuntimeException('Filter Query Exception - '.$e->getMessage());
        }

        $filtered = [];
        
        foreach ($engine->getFilters() as $filter) {

            if($filter instanceof RecommendationSetFilter) {
                $tag = sprintf('filter_%s', $filter->name());
                
                $results = $filterResults->contains($tag) ? $filterResults->get($tag) : null;

                $outcome = (array) $filter->handleResultSet($input, $results, $recommendations);
                $filtered = array_merge($outcome, $filtered);
            }
            else {
                foreach ($recommendations->getItems() as $recommendation) {
                    if (!$filter->doInclude($input, $recommendation->item())) {
                        $filtered[] = $recommendation->item()->identity();
                    }
                }
            }
        }

        $filtered = array_unique($filtered);
        foreach($recommendations->getItems() as $recommendation) {
            if(in_array($recommendation->item()->identity(), $filtered)) {
                $recommendations->rm($recommendation);
            }
        }
    }

    public function buildBlacklistedNodes(ResultCollection $result, RecommendationEngine $engine)
    {
        $set = [];
        foreach ($engine->getBlacklistBuilders() as $blacklist) {
            $res = $result->get($blacklist->name());
            foreach ($res->records() as $record) {
                if ($record->hasValue($blacklist->itemResultName())) {
                    $node = $record->get($blacklist->itemResultName());
                    if ($node instanceof Node) {
                        $set[$node->identity()] = $node;
                    }
                }
            }
        }

        return $set;
    }
}
