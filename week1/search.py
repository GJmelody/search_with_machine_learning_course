#
# The main search hooks for the Search Flask application.
#
from flask import (
    Blueprint, redirect, render_template, request, url_for
)

from week1.opensearch import get_opensearch

bp = Blueprint('search', __name__, url_prefix='/search')


# Process the filters requested by the user and return a tuple that is appropriate for use in: the query, URLs displaying the filter and the display of the applied filters
# filters -- convert the URL GET structure into an OpenSearch filter query
# display_filters -- return an array of filters that are applied that is appropriate for display
# applied_filters -- return a String that is appropriate for inclusion in a URL as part of a query string.  This is basically the same as the input query string
def process_filters(filters_input):
    # Filters look like: &filter.name=regularPrice&regularPrice.key={{ agg.key }}&regularPrice.from={{ agg.from }}&regularPrice.to={{ agg.to }}
    filters = []
    display_filters = []  # Also create the text we will use to display the filters that are applied
    applied_filters = ""
    for filter in filters_input:
        type = request.args.get(filter + ".type") 
        display_name = request.args.get(filter + ".displayName", filter)
        applied_filters += "&filter.name={}&{}.type={}&{}.displayName={}".format(filter, filter, type, filter,
                                                                                 display_name)
        if type == "range":
            filter_range_from = request.args.get(f"{filter}.from") or None
            filter_range_to = request.args.get(f"{filter}.to") or None
            filters.append({
                "range": {filter: {"gte": filter_range_from, "lte": filter_range_to}}
            })
            display_filters.append(f"{filter}: ${filter_range_from} - ${filter_range_to}")
            applied_filters += f"&{filter}.from={filter_range_from}&{filter}.to={filter_range_to}"
        elif type == "terms":
            filter_key = request.args.get(filter + ".key")
            filters.append({
                "term": {f"{filter}.keyword": filter_key}
            })
            display_filters.append(f"{filter}: {filter_key}")
            applied_filters += f"&{filter}.key={filter_key}"
    print("Filters: {}".format(filters))

    return filters, display_filters, applied_filters


# Our main query route.  Accepts POST (via the Search box) and GETs via the clicks on aggregations/facets
@bp.route('/query', methods=['GET', 'POST'])
def query():
    opensearch = get_opensearch() # Load up our OpenSearch client from the opensearch.py file.
    # Put in your code to query opensearch.  Set error as appropriate.
    error = None
    user_query = None
    query_obj = None
    display_filters = None
    applied_filters = ""
    filters = None
    sort = "_score"
    sortDir = "desc"
    if request.method == 'POST':  # a query has been submitted
        user_query = request.form['query']
        if not user_query:
            user_query = "*"
        sort = request.form["sort"]
        if not sort:
            sort = "_score"
        sortDir = request.form["sortDir"]
        if not sortDir:
            sortDir = "desc"
        query_obj = create_query(user_query, [], sort, sortDir)
    elif request.method == 'GET':  # Handle the case where there is no query or just loading the page
        user_query = request.args.get("query", "*")
        filters_input = request.args.getlist("filter.name")
        sort = request.args.get("sort", sort)
        sortDir = request.args.get("sortDir", sortDir)
        if filters_input:
            (filters, display_filters, applied_filters) = process_filters(filters_input)

        query_obj = create_query(user_query, filters, sort, sortDir)
    else:
        query_obj = create_query("*", [], sort, sortDir)

    print("query obj: {}".format(query_obj))
    response = opensearch.search(query_obj)
    # Postprocess results here if you so desire

    print("query response: {}".format(response))
    if error is None:
        return render_template("search_results.jinja2", query=user_query, search_response=response,
                               display_filters=display_filters, applied_filters=applied_filters,
                               sort=sort, sortDir=sortDir)
    else:
        redirect(url_for("index"))


def create_query(user_query, filters, sort="_score", sortDir="desc"):
    print("Query: {} Filters: {} Sort: {}".format(user_query, filters, sort))
    query_obj = {
        'size': 10,
        "query": {
            "bool": {
                "must": {
                    "query_string": {
                        "fields": [ "name", "shortDescription", "longDescription"],
                        "query": user_query,
                        "phrase_slop": 3
                    }
                },
                "filter": filters,
            }
        },
        "sort": [{sort: sortDir}],
        "highlight": {
            "fields": {
                "name": {},
                "shortDescription": {},
                "longDescription": {}
            }
        },
        "aggs": {
            "regularPrice": {
                "range": {
                    "field": "regularPrice",
                    "ranges": [
                        {"key": "$", "to": 100.0},
                        {"key": "$$", "from": 100.0, "to": 200.0},
                        {"key": "$$$", "from": 200.0}
                    ]
                },
            },
            "department": {
                "terms": {
                    "field": "department.keyword"
                }
            },
            "missing_images": {
                "missing": {
                    "field": "image.keyword"
                }
            }
        }
    }
    return query_obj
