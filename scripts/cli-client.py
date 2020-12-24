from optparse import OptionParser
try:
    import urllib2      # python 2.x
except ImportError:
    import urllib.request as urllib2      # python 3.x
import json
import sys


def parseFilter(filtersStr):
    if len(filtersStr) == 0:
        return None

    filters = []
    for curFilterStr in filtersStr.split(','):
        curFilterStr = curFilterStr.strip()
        if len(curFilterStr) < 3:
            print('Error: failed to parse filter %s' % curFilterStr)
            return False

        field = curFilterStr[0]
        if curFilterStr[1] == '!':
            negated = True
            operator = curFilterStr[2]
            value = curFilterStr[3:]
        else:
            negated = False
            operator = curFilterStr[1]
            value = curFilterStr[2:]

        try:
            if operator == '=':
                filter = {'type': 'in', 'dim': field, 'values': [value]}
            elif operator == '~':
                filter = {'type': 'contains', 'dim': field, 'values': [value]}
            elif operator == '>':
                filter = {'type': 'gt', 'metric': field, 'value': float(value)}
            elif operator == '<':
                filter = {'type': 'lt', 'metric': field, 'value': float(value)}
            else:
                print('Error: failed to parse operator %s' % curFilterStr)
                return False
        except ValueError:
            print('Error: failed to parse value %s' % curFilterStr)
            return False

        if negated:
            filter = {'type': 'not', 'filter': filter}

        filters.append(filter)

    if len(filters) > 1:
        filter = {'type': 'and', 'filters': filters}
    else:
        filter = filters[0]

    return filter


if __name__ == '__main__':
    parser = OptionParser()

    parser.description = ('Queries an nginx-aggr-module server, and formats the response in tabular format, ' +
                         'for additional processing by shell tools (sort, awk, etc.).  ' +
                         'Each dimension/metric is assumed to be a single char.')

    parser.add_option('-u', '--url', dest='url', default='http://127.0.0.1:8001/query',
                      help='the server URL to query [default: "%default"]', metavar='ADDR')
    parser.add_option('-g', '--group-by', dest='groupBy', default='',
                      help='fields to group the data by', metavar='FIELDS')
    parser.add_option('-s', '--select', dest='select', default='',
                      help='fields to select for each group', metavar='FIELDS')
    parser.add_option('-m', '--metrics', dest='metrics', default='cx',
                      help='numeric fields to sum up for each group [default: "%default"]', metavar='FIELDS')
    parser.add_option('-c', '--count', dest='count', default='c',
                      help='the count metric name [default: "%default"]', metavar='FIELDS')
    parser.add_option('-f', '--filter', dest='filter', default='',
                      help='filter definition - format is <f1><o1><v1>,<f2><o2><v2>,...  ' +
                      'Where f=field, o=operator and v=value.  ' +
                      'Supported operators are: = (equals), ~ (contains), < (less than), > (greater than).' +
                      'Optionally, the operator can be preceded by ! to reverse the condition', metavar='FILTER')

    (options, args) = parser.parse_args()

    filter = parseFilter(options.filter)
    if filter == False:
        sys.exit(1)

    columns = []
    metrics = {}
    dims = {}

    for metric in options.metrics:
        if metric == options.count:
            metrics[metric] = {'default': 1}
        else:
            metrics[metric] = {}
        columns.append(metric)

    for dim in options.groupBy:
        dims[dim] = {}
        columns.append(dim)

    for dim in options.select:
        dims[dim] = {'type': 'select'}
        columns.append(dim)

    query = {'metrics': metrics, 'dims': dims}
    if filter is not None:
        query['filter'] = filter

    req = urllib2.Request(options.url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(query).encode('utf-8'))

    rows = json.loads(response.read())
    for row in rows:
        row = [str(row[col]) for col in columns]
        print('\t'.join(row))
