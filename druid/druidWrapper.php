<?php

require_once(dirname(__file__) . '/utils.php');


define('CONF_MAIN', 'main');
define('CONF_LOG_PATH', 'log_path');
define('CONF_DATASOURCES', 'datasources');
define('CONF_URL', 'url');
define('CONF_METRICS', 'metrics');
define('CONF_DIMS', 'dims');

define('DATASOURCE_NGINX_AGGR', 'nginx-aggr-module');
define('DATASOURCE_DRUID', 'druid');


class NginxAggrDruidWrapper
{
    const START_TIME = '2020-01-01T00:00:00.000Z';
    const END_TIME = '2020-01-01T01:00:00.000Z';

    protected $schema;
    protected $log;
    protected $maps;

    public function __construct($schema, $log)
    {
        $this->schema = $schema;
        $this->log = $log;
        $this->maps = array();
    }

    protected static function metadataColumnString()
    {
        return array(
            'type' => 'STRING',
            'hasMultipleValues' => false,
            'size' => 0,
            'cardinality' => 0,
            'minValue' => null,
            'maxValue' => null,
            'errorMessage' => null
        );
    }

    protected static function metadataColumnFloat()
    {
        return array(
            'type' => 'FLOAT',
            'hasMultipleValues' => false,
            'size' => 0,
            'cardinality' => null,
            'minValue' => null,
            'maxValue' => null,
            'errorMessage' => null
        );
    }

    protected static function metadataColumnLong()
    {
        return array(
            'type' => 'LONG',
            'hasMultipleValues' => false,
            'size' => 0,
            'cardinality' => null,
            'minValue' => null,
            'maxValue' => null,
            'errorMessage' => null
        );
    }

    protected static function metadataAggrDoubleSum($name)
    {
        return array(
            'type' => 'doubleSum',
            'name' => $name,
            'fieldName' => $name,
            'expression' => null
        );
    }

    protected static function metadataAggrDoubleMax($name)
    {
        return array(
            'type' => 'doubleMax',
            'name' => $name,
            'fieldName' => $name,
            'expression' => null
        );
    }

    protected function segmentMetadata()
    {
        $columns = array(
            '__time' => self::metadataColumnLong()
        );
        $aggregators = array();

        $dimMap = $this->schema[CONF_DIMS];
        foreach ($dimMap as $name => $props)
        {
            $columns[$name] = self::metadataColumnString();
        }

        $metricMap = $this->schema[CONF_METRICS];
        foreach ($metricMap as $name => $props)
        {
            $columns[$name] = self::metadataColumnFloat();

            $type = isset($props['type']) ? $props['type'] : 'sum';
            $druidType = 'self::metadataAggrDouble' . ucfirst($type);
            $aggregators[$name] = call_user_func($druidType, $name);
        }

        $response = array(array(
            'id' => 'merged',
            'intervals' => null,
            'columns' => $columns,
            'size' => 0,
            'numRows' => 1,
            'aggregators' => $aggregators,
            'timestampSpec' => null,
            'queryGranularity' => null,
            'rollup' => null
        ));

        return json_encode($response);
    }

    protected function timeBoundary()
    {
        $response = array(array(
            'timestamp' => self::START_TIME,
            'result' => array(
                'minTime' => self::START_TIME,
                'maxTime' => self::END_TIME,
            )
        ));

        return json_encode($response);
    }

    protected function getDim($dim)
    {
        $dimMap = $this->schema[CONF_DIMS];

        $dim = $dimMap[$dim];
        if (isset($dim['maps']))
        {
            $maps = $dim['maps'];
            unset($dim['maps']);

            $this->maps = array_merge($this->maps, $maps);
        }

        return $dim;
    }

    protected function translateFilter($filter)
    {
        switch ($filter->type)
        {
        case 'or':
        case 'and':
            $filters = array();
            foreach ($filter->fields as $curFilter)
            {
                $filters[] = $this->translateFilter($curFilter);
            }

            return array(
                'type' => $filter->type,
                'filters' => $filters
            );

        case 'not':
            return array(
                'type' => $filter->type,
                'filter' => $this->translateFilter($filter->field)
            );

        case 'selector':
            return array(
                'type' => 'in',
                'dim' => $this->getDim($filter->dimension)['input'],
                'values' => array(strval($filter->value))
            );

        case 'in':
            return array(
                'type' => 'in',
                'dim' => $this->getDim($filter->dimension)['input'],
                'values' => array_map('strval', $filter->values)
            );

        case 'search':
            switch ($filter->query->type)
            {
            case 'contains':
                return array(
                    'type' => 'contains',
                    'dim' => $this->getDim($filter->dimension)['input'],
                    'values' => array($filter->query->value),
                    'case_sensitive' => $filter->query->caseSensitive
                );

            default:
                $this->log->dieError('unknown search filter query type ' . $filter->query->type);
            }

        case 'regex':
            return array(
                'type' => 'regex',
                'dim' => $this->getDim($filter->dimension)['input'],
                'pattern' => $filter->pattern
            );

        default:
            $this->log->dieError('unknown filter type ' . $filter->type);
        }
    }

    protected function timeseries($query)
    {
        $metricMap = $this->schema[CONF_METRICS];
        $metrics = array();
        $empty = array();
        foreach ($query->aggregations as $aggr)
        {
            $metrics[$aggr->name] = $metricMap[$aggr->fieldName];
            $empty[$aggr->name] = 0;
        }

        $request = array('metrics' => $metrics);
        if (isset($query->filter))
        {
            $request['filter'] = $this->translateFilter($query->filter);
        }
        if ($this->maps)
        {
            $request['maps'] = $this->maps;
        }

        $url = $this->schema[CONF_URL];
        $response = postJson($url, $request, $this->log);
        if ($response == '[]')
        {
            $response = json_encode($empty);
        }
        else
        {
            $response = substr($response, 1, -1);        // strip the []
        }

        $intervals = explode('/', $query->intervals);
        return '[{"timestamp":"' . $intervals[0] . '","result":' . $response . '}]';
    }

    protected function topN($query)
    {
        $dimensionType = $query->dimension->type;
        if ($dimensionType != 'default')
        {
            $this->log->dieError("unknown dimension type $dimensionType");
        }

        $threshold = $query->threshold;
        $metric = $query->metric;
        if (!is_string($metric))
        {
            $metricType = $metric->type;
            switch ($metricType)
            {
            case 'numeric':
                break;

            case 'inverted':
                $threshold = -$threshold;
                break;

            default:
                $this->log->dieError("unknown metric type $metricType");
            }
            $metric = $metric->metric;
        }

        $metricMap = $this->schema[CONF_METRICS];
        $metrics = array();
        foreach ($query->aggregations as $aggr)
        {
            $spec = $metricMap[$aggr->fieldName];
            if ($aggr->name == $metric)
            {
                $spec['top'] = $threshold;
            }
            $metrics[$aggr->name] = $spec;
        }

        $dimension = $query->dimension->dimension;
        $dims = array(
            $dimension => $this->getDim($dimension)
        );

        $request = array('metrics' => $metrics, 'dims' => $dims);
        if (isset($query->filter))
        {
            $request['filter'] = $this->translateFilter($query->filter);
        }
        if ($this->maps)
        {
            $request['maps'] = $this->maps;
        }

        $url = $this->schema[CONF_URL];
        $response = postJson($url, $request, $this->log);

        $intervals = explode('/', $query->intervals);
        return '[{"timestamp":"' . $intervals[0] . '","result":' . $response . '}]';
    }

    public function query($query)
    {
        switch ($query->queryType)
        {
        case 'segmentMetadata':
            $response = $this->segmentMetadata();
            break;

        case 'timeBoundary':
            $response = $this->timeBoundary();
            break;

        case 'timeseries':
            $response = $this->timeseries($query);
            break;

        case 'topN':
            $response = $this->topN($query);
            break;

        default:
            $this->log->dieError("unknown query {$query->queryType}");
        }

        return $response;
    }
}

function doMain($conf, $log)
{
    $startTime = microtime(true);

    $requestUri = rtrim($_SERVER['REQUEST_URI'], '/');

    $log->info("uri [$requestUri]");

    switch ($_SERVER['REQUEST_METHOD'])
    {
    case 'POST':
        if ($requestUri != '/druid/v2')
        {
            $log->dieError("unknown POST uri $requestUri");
        }

        $data = file_get_contents('php://input');
        $log->info("original query $data");

        $query = json_decode($data);

        if (function_exists('editQuery'))
        {
            list($query, $context) = editQuery($query);
            $log->info("final query $data");
        }

        if (!isset($conf[CONF_DATASOURCES][$query->dataSource]))
        {
            $log->dieError("unknown data source {$query->dataSource}");
        }
        $params = $conf[CONF_DATASOURCES][$query->dataSource];

        switch ($params['type'])
        {
        case DATASOURCE_NGINX_AGGR:
            $wrapper = new NginxAggrDruidWrapper($params, $log);
            $response = $wrapper->query($query);
            break;

        case DATASOURCE_DRUID:
            $url = $params[CONF_URL];
            $response = postJson($url . $requestUri, $query, $log);
            break;
        }

        if (function_exists('editResponse'))
        {
            $log->info("original response $response");
            $response = editResponse($response, $context);
        }

        break;

    case 'GET':
        if ($requestUri == '/status')
        {
            $response = '{"version":"0.10.0"}';
            break;
        }

        if ($requestUri == '/druid/v2/datasources')
        {
            $response = json_encode(array_keys($conf[CONF_DATASOURCES]));
            break;
        }

        $log->dieError("unknown GET uri $requestUri");
        break;

    default:
        $log->dieError('unknown method ' . $_SERVER['REQUEST_METHOD']);
    }

    $endTime = microtime(true);

    $log->info("took " . ($endTime - $startTime) . ", final response $response");

    return $response;
}

function main()
{
    ob_start();

    $baseDir = dirname(__file__);
    $conf = file_get_contents("$baseDir/druidWrapper.json");
    $conf = json_decode($conf, true);

    $log = new Logger($conf[CONF_MAIN][CONF_LOG_PATH]);

    $response = doMain($conf, $log);

    ob_end_clean();

    header('Content-Type: application/json');
    echo $response;
}

if (!count(debug_backtrace()))
{
    main();
}

