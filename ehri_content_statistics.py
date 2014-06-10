#!/usr/bin/python
"""
    Usage:
        ehri_content_statistics.py [-o FILE] [--format=FORMAT] [--quiet]
        ehri_content_statistics.py -h | --help

    Obtain statistical information from the neo4j graph and write them to a file

    Options:
        -o FILE             Specify output file (default standard out, which can be redirected)
        --format=FORMAT     Set the output format, csv or json (the default is json)
        --quiet             Print no messages or progress (default when using standard out)
        -h --help           Show this screen.

    Example:
        ./ehri_content_statistics.py -o out.csv --quiet --format=csv

    Requires Python modules:
        docopt
        requests

"""

# __author__ = 'paulboon'

import requests
import json
import sys
import docopt
from time import sleep

# Note that we could use py2neo
# but we only try to get some statistics with cypher via REST
# and the cypher queries can be reused in other programs written in other languages etc. etc.

# some globals
url = 'http://localhost:7474/db/data/cypher' # cypher REST url
headers = {'content-type': 'application/json'}
quiet = False; # when true is means no output on stdout (console)
output = {} # the output file

OUTPUT_FORMATS = ['csv', 'json']

# The get_*_stats functions all produce an array of rows, each with the same keys for the columns
# The statistical results are stored in a row oriented structure (dictionary) instead of columns.
# this makes more sense when exporting to xml or json because it can be more hierarchical.
#
# A separate specification is used for the 'readable' labels
# because that is a presentation thing

def write_stats_to_CSV(stat_results_table, print_spec):
    # title
    print >>output, print_spec['title']

    # column headers
    labels = print_spec['column_labels']
    row_str_arr = []
    items = labels
    for item in items:
        row_str_arr.append(item[1]) # [1] is column label value
    print >>output, ', '.join(row_str_arr)

    # values
    for row in stat_results_table:
        row_str_arr = []
        for item in items:
            row_str_arr.append(str(row[item[0]]))  # [0] is column key
        print >>output, ', '.join(row_str_arr)

def write_stats_to_JSON(stat_results_table, print_spec):
    wrapped_results = {print_spec['title'] : stat_results_table}
    print >>output, json.dumps(wrapped_results)


def write_stats(format, stat_results_table, print_spec):
    if format=='csv':
        write_stats_to_CSV(stat_results_table, print_spec)
    elif format=='json':
        write_stats_to_JSON(stat_results_table, print_spec)
    else:
        pass # could default ?

# show progress indicator (never tested on Windows)
def show_progress(counter, max_counter):
    if quiet: return

    if not hasattr(show_progress, "call_cnt"):
        show_progress.call_cnt = 0  # it doesn't exist yet, so initialize it
    show_progress.call_cnt += 1
    show_progress.call_cnt = show_progress.call_cnt%4 # prevent overflow

    percentage = 100*counter/max_counter
    rotator="|/-\\"
    size = 40
    #rot_counter = counter
    # always rotate even if counter did not change
    rot_counter = show_progress.call_cnt%4

    # rotator will show change if counter changes, but percentage might not!
    sys.stdout.write('(' + rotator[rot_counter%4]  + ')')
    # show bar
    sys.stdout.write('[' + '#'*(percentage*size/100) + '.'*(size-(percentage*size/100)) + ']')
    # show percentage
    sys.stdout.write(" %3d %%" % percentage)
    sys.stdout.flush()
    sys.stdout.write("\r") # place insertion at start of line
    if counter == max_counter:
        sleep(0.3) # 100% will show
        # clear line
        sys.stdout.write('Done' + ' '*(50))
        sys.stdout.write("\n")

def get_vocabulary_stats():
    if not quiet: print 'Getting statistics for Vocabularies'

    stat_results_table = []

    # vocabularies
    query = {'query': 'START a = node:entities( __ISA__= "cvocVocabulary") RETURN a'}
    r = requests.post(url, data=json.dumps(query), headers=headers)
    vocs = r.json()

    num_vocs = len(vocs['data'])
    voc_cnt = 0

    # for each vocabulary
    for data in vocs['data']:
        voc_cnt += 1
        show_progress(voc_cnt, num_vocs)

        voc_id = data[0]['data']['__ID__'] # need [0] because its in a wrapping list

        # number of concepts
        query = {'query':
                 'START concept = node:entities( __ISA__= "cvocConcept")'
                 'MATCH (concept)-[:inAuthoritativeSet]->(vocabulary)'
                 'WHERE vocabulary.__ID__= "%s"'
                 'RETURN COUNT(concept)' % voc_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        concept = r.json()
        num_concepts = concept['data'][0][0] # wrapped in two lists

        # concepts that have no narrower concepts and can be seen as endpoint or leaves of the graph
        # note that the other ones, that have a narrower concept, are broader concepts
        query = {'query':
                 'START concept = node:entities( __ISA__= "cvocConcept") '
                 'MATCH (concept)-[:inAuthoritativeSet]->(vocabulary)'
                 'WHERE vocabulary.__ID__= "%s" '
                 'AND NOT (concept)-[:narrower]->() '
                 'RETURN COUNT(distinct(concept))' % voc_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        endpoint = r.json()
        num_endpoint = endpoint['data'][0][0] # wrapped in two lists

        # toplevel concept have no broader concept, or in other words, they are not the narrower of another concept
        query = {'query':
                 'START concept = node:entities( __ISA__= "cvocConcept") '
                 'MATCH (concept)-[:inAuthoritativeSet]->(vocabulary)'
                 'WHERE vocabulary.__ID__= "%s" '
                 'AND NOT (concept)<-[:narrower]-() '
                 'RETURN COUNT(distinct(concept))' % voc_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        toplevel = r.json()
        num_toplevel = toplevel['data'][0][0] # wrapped in two lists

        # number of descriptions (for concepts)
        query = {'query':
                 'START descr = node:entities( __ISA__= "cvocConceptDescription") '
                 'MATCH (descr)-[:describes]->(concept)-[:inAuthoritativeSet]->(vocabulary)'
                 'WHERE vocabulary.__ID__= "%s"'
                 'RETURN COUNT(descr)' % voc_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        descr = r.json()
        num_descr = descr['data'][0][0] # wrapped in two lists

        # distinct languages
        query = {'query':
                 'START descr = node:entities( __ISA__= "cvocConceptDescription") '
                 'MATCH (descr)-[:describes]->(concept)-[:inAuthoritativeSet]->(vocabulary)'
                 'WHERE vocabulary.__ID__= "%s"'
                 'RETURN COUNT(distinct(descr.languageCode))' % voc_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        distinct_descr_lang = r.json()
        num_distinct_descr_lang= distinct_descr_lang['data'][0][0] # wrapped in two lists

        stat_results_table.append({'voc_id': voc_id,
                              'num_concepts': num_concepts,
                              'num_toplevel':num_toplevel,
                              'num_endpoint': num_endpoint,
                              'num_descr':num_descr,
                              'num_distinct_descr_lang':num_distinct_descr_lang
                              })
    return stat_results_table

def get_authorities_stats():
    if not quiet: print 'Getting statistics for Authoritaties'

    stat_results_table = []

    # sets
    query = {'query': 'START a = node:entities( __ISA__= "authoritativeSet") RETURN a'}
    r = requests.post(url, data=json.dumps(query), headers=headers)
    aset = r.json()

    num_sets = len(aset['data'])
    set_cnt = 0

    # for each set
    for data in aset['data']:
        set_cnt += 1
        show_progress(set_cnt, num_sets)

        aset_id = data[0]['data']['__ID__'] # need [0] because its in a wrapping list

        # number of historicalAgents
        query = {'query':
                 'START hagent = node:entities( __ISA__= "historicalAgent") '
                 'MATCH (hagent)-[:inAuthoritativeSet]->(aset) '
                 'WHERE aset.__ID__= "%s" '
                 'RETURN COUNT(hagent)' % aset_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        hagent = r.json()
        num_hagents = hagent['data'][0][0] # wrapped in two lists

        # number of descriptions
        query = {'query':
                 'START hagent = node:entities( __ISA__= "historicalAgent") '
                 'MATCH (descr)-[:describes]->(hagent)-[:inAuthoritativeSet]->(aset) '
                 'WHERE aset.__ID__= "%s" '
                 'RETURN COUNT(descr)' % aset_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        descr = r.json()
        num_descr = descr['data'][0][0] # wrapped in two lists

        # distinct languages
        query = {'query':
                 'START hagent = node:entities( __ISA__= "historicalAgent") '
                 'MATCH (descr)-[:describes]->(hagent)-[:inAuthoritativeSet]->(aset) '
                 'WHERE aset.__ID__= "%s" '
                 'RETURN COUNT(distinct(descr.languageCode))' % aset_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        distinct_descr_lang = r.json()
        num_distinct_descr_lang= distinct_descr_lang['data'][0][0] # wrapped in two lists

        stat_results_table.append({'set_id': aset_id,
                              'num_historicalAgents': num_hagents,
                              'num_descr':num_descr,
                              'num_distinct_descr_lang':num_distinct_descr_lang
                              })

    return stat_results_table

def get_country_stats():
    if not quiet: print 'Getting statistics for Countries'

    stat_results_table = []

    # countries
    query = {'query': 'START c = node:entities( __ISA__= "country") RETURN c'}
    r = requests.post(url, data=json.dumps(query), headers=headers)
    countries = r.json()

    num_counties = len(countries['data'])
    country_cnt = 0

    # for each country
    for data in countries['data']:
        country_cnt += 1
        show_progress(country_cnt, num_counties)

        country_id = data[0]['data']['__ID__'] # need [0] because its in a wrapping list
        #child_count = data[0]['data'].get('_childCount', 0)

        # just count the number of repositories of this country
        query = {'query':
                 'START repos = node:entities( __ISA__= "repository")'
                 'MATCH (repos)-[:hasCountry]->(country)'
                 'WHERE country.__ID__= "%s"'
                 'RETURN COUNT(distinct(repos))' % country_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        repos = r.json()
        num_repos = repos['data'][0][0] # wrapped in two lists

        stat_results_table.append({'country_id': country_id,
        #              'child_count':child_count,
                      'num_repos':num_repos
                      })

    return stat_results_table

def get_repo_stats(repo_id):
    stat_results_table = []

    # top level docs
    query = {'query':
             'START repo = node:entities( __ID__= "%s")'
             'MATCH (doc)-[:heldBy]->(repo)'
             'RETURN COUNT(doc)' % repo_id
    }
    r = requests.post(url, data=json.dumps(query), headers=headers)
    topdocs = r.json()
    num_topdocs = topdocs['data'][0][0] # wrapped in two lists

    if (num_topdocs > 0):
        # all children
        query = {'query':
                 'START repo = node:entities( __ID__= "%s")'
                 'MATCH (doc)-[:childOf*]->(topdoc)-[:heldBy]->(repo)'
                 'RETURN COUNT(distinct(doc))' % repo_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        docs = r.json()
        num_child_docs = docs['data'][0][0] # wrapped in two lists

        # without children, endpoints
        query = {'query':
                 'START repo = node:entities( __ID__= "%s")'
                 'MATCH (doc)-[:childOf*]->(topdoc)-[:heldBy]->(repo)'
                 'WHERE NOT (doc)<-[:childOf]-()'
                 'RETURN COUNT(distinct(doc))' % repo_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        docs = r.json()
        num_endpoint_docs = docs['data'][0][0] # wrapped in two lists

        # number of document descriptions?
        query = {'query':
                 'START repo = node:entities( __ID__= "%s") '
                 'MATCH (descr)-[:describes]->(doc)-[:childOf*]->(topdoc)-[:heldBy]->(repo) '
                 'RETURN COUNT(distinct(descr))' % repo_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        child_descr = r.json()
        num_child_descr = child_descr['data'][0][0] # wrapped in two lists
        query = {'query':
                 'START repo = node:entities( __ID__= "%s") '
                 'MATCH (descr)-[:describes]->(doc)-[:heldBy]->(repo)  '
                 'RETURN COUNT(distinct(descr))' % repo_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        top_descr = r.json()
        num_top_descr = top_descr['data'][0][0] # wrapped in two lists
        num_descr = num_top_descr + num_child_descr

        stat_results_table.append({'repo_id': repo_id,
                      'num_topdocs': num_topdocs,
                      'num_child_docs': num_child_docs,
                      'num_endpoint_docs':num_endpoint_docs,
                      'num_descr':num_descr
                      })

    else:
        # empty
        stat_results_table.append({'repo_id': repo_id,
                      'num_topdocs': 0,
                      'num_child_docs': 0,
                      'num_endpoint_docs':0,
                      'num_descr':0
                      })

    return stat_results_table

# repositories but grouped by the country
def get_country_repo_stats():
    if not quiet: print 'Getting statistics for Repositories'

    stat_results_table = []

    # countries
    query = {'query': 'START c = node:entities( __ISA__= "country") RETURN c'}
    r = requests.post(url, data=json.dumps(query), headers=headers)
    countries = r.json()

    num_counties = len(countries['data'])
    country_cnt = 0

    # for each country
    for data in countries['data']:
        country_cnt += 1
        show_progress(country_cnt, num_counties)

        country_id = data[0]['data']['__ID__'] # need [0] because its in a wrapping list

        # repositories of this country
        query = {'query':
                 'START repos = node:entities( __ISA__= "repository")'
                 'MATCH (repos)-[:hasCountry]->(country)'
                 'WHERE country.__ID__= "%s"'
                 'RETURN repos' % country_id
        }
        r = requests.post(url, data=json.dumps(query), headers=headers)
        repos = r.json()

        # for each repo, copy the rows and add country id
        for data in repos['data']:
            show_progress(country_cnt, num_counties)
            repo_id = data[0]['data']['__ID__'] # need [0] because its in a wrapping list
            repo_table = get_repo_stats(repo_id)
            for row in repo_table:
                # leave out the empty ones!
                if row['num_topdocs'] > 0:
                    row['country_id'] = country_id
                    stat_results_table.append(row)

    return stat_results_table


if __name__ == '__main__':
    try:
        # Parse arguments, use file docstring as a parameter definition
        arguments = docopt.docopt(__doc__)

        if arguments["--quiet"]:
            quiet = True

        if arguments['-o']:
            filename = arguments['-o']
            output = open(filename, 'w+')
        else:
            output = sys.stdout
            # force quiet
            quiet = True

        format = 'json'
        if arguments["--format"]:
            format = arguments["--format"]
        assert format in OUTPUT_FORMATS

        # get stats and write them
        write_stats(format, get_vocabulary_stats(), {'title':'Vocabularies',
                      'column_labels': [
                        ('voc_id','vocabulary id'),
                        ('num_concepts','total number of concepts'),
                        ('num_toplevel','number of toplevel concepts'),
                        ('num_endpoint','number of endpoint concepts'),
                        ('num_descr','number of concept descriptions'),
                        ('num_distinct_descr_lang','number of distinct languages for descriptions')
        ]})
        write_stats(format, get_authorities_stats(), {'title':'Authorative Sets',
                      'column_labels': [
                        ('set_id','set id'),
                        ('num_historicalAgents','total number of historical agents'),
                        ('num_descr','number of agent descriptions'),
                        ('num_distinct_descr_lang','number of distinct languages for descriptions')
        ]})
        write_stats(format, get_country_stats(), {'title':'Countries',
                      'column_labels': [
                        ('country_id','country id'),
                        ('num_repos','number of repositories')
        ]})
        write_stats(format, get_country_repo_stats(), {'title':'Repositories',
                      'column_labels': [
                        ('country_id','country id'),
                        ('repo_id','repository id'),
                        ('num_topdocs','number of toplevel documents'),
                        ('num_child_docs','number of descendants of toplevel documents'),
                        ('num_endpoint_docs','number of endpoint documents'),
                        ('num_descr','number of document descriptions')
        ]})

    # invalid options
    except docopt.DocoptExit as e:
        print e.message
