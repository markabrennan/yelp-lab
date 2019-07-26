import requests
import config
import time
import mysql.connector
import collections


def yelp_business_search(url_params, api_key):
    """ yelp_business_search(url_params, api_key):
    Invoke the Yelp business searcg API
    Params:
        url_params contains our search criteria
        api_key is our access key
    Returns:
        Response data.

    TO DO:  Add exceptiion handling
    """
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = requests.get(url, headers=headers, params=url_params)

    return response.json()


def get_all_yelp_search_results(url_params, api_key, test_lim=None):
    """ all_bus_search_results((url_params, api_key):
    Iteratively invoke the yelp_business_seearch API call
    Params:
        url_params contains our search criteria
        api_key is our access key
    Returns:
        Results list of response dictionaries

    TO DO:  Add exceptiion handling
    """

    # test_lim allows us to pass a limit to test
    # the API and constrain the loop
    print('test limit:  ', test_lim)
    if test_lim:
        num = test_lim
    else:
        num = yelp_business_search(url_params, api_key)['total']

    print('{} total matches found.'.format(num))
    cur = 0
    results = []
    while cur < num and cur < 1000:
        url_params['offset'] = cur
        results.extend(yelp_business_search(url_params, api_key)['businesses'])
        time.sleep(.1)  # Wait a second
        cur += 50

    return results


def get_bus_ids(results):
    """get_bus_ids(results):
    Pull out business IDs from Yelp results set
    and return a list of IDS.
    Use dict.get() to avoid key errors - None
    value is set if key is not found.
    Params:
        results (list of Yelp dicts)
    Returns:
        List of business IDs
    """
    return [item.get('id') for item in results]


def get_bus_names(results):
    """get_bus_names(results):
    Pull out business names from Yelp results set
    and return a list of names.
    Use dict.get() to avoid key errors - None
    value is set if key is not found.
    Params:
        results (list of Yelp dicts)
    Returns:
        List of business names
    """
    return [item.get('name') for item in results]


def get_bus_ratings(results):
    """get_bus_ratings(results):
    Pull out business ratings from Yelp results set
    and return a list of ratings.
    Use dict.get() to avoid key errors - None
    value is set if key is not found.
    Params:
        results (list of Yelp dicts)
    Returns:
        List of business ratings
    """
    return [item.get('rating') for item in results]


def get_bus_prices(results):
    """get_bus_prices(results):
    Pull out business price ranking from Yelp results set
    and return a list of price rankings.
    Use dict.get() to avoid key errors - None
    value is set if key is not found.
    Params:
        results (list of Yelp dicts)
    Returns:
        List of business price rankings
    """
    return [item.get('price') for item in results]


def get_bus_recs(results):
    """get_bus_recs(results):
    Extract specific restaurant fields from list
    of Yelp dictionaries.
    dict.get() handles missing keys
    Params:
        results (list of Yelp dicts)
    Returns:
        List of restaurant values we care about:
            id
            name
            rating
            price rank
    """
    rec_list = []
    for rec in results:
        id = rec.get('id')
        name = rec.get('name', None)
        rating = rec.get('rating', None)
        price = rec.get('price', None)
        rec_list.append(dict(id=id, name=name, rating=rating, price=price))

    return rec_list


def populate_db(records, table_name, config_params):
    """populate_db(records, table_name, config_params):
    Iterate over our restaurant records and insert values
    into the DB.
    Params:
        records: list of dicts of restaurant values
        table_name: target table for insert
        config_params: config for DB connect
    Returns:
        Nothing.  Closes conn on finally block.
    """
    INSERT_STR = f'INSERT INTO {table_name} (id, name, rating, price) '
    VALUE_STR = 'VALUES (%s, %s, %s, %s)'
    INSERT_QUERY = INSERT_STR + VALUE_STR

    conn = get_db_conn(config_params)
    if not conn:
        print('No DB connection!')
        return
    try:
        cursor = conn.cursor()
        for rec in records:
            values = tuple(rec.values())
            cursor.execute(INSERT_QUERY, values)

        conn.commit()

    finally:
        conn.close()


def get_db_conn(config_params):
    """get_db_conn(config_params):
    Open a MySQL DB connection using
    config parameters
    Params:
        config module: config_params
    Returns:
        MySQL DB connection or None if exception
    """
    try:
        conn = mysql.connector.connect(
            host=config_params.host,
            user=config_params.user,
            passwd=config_params.password,
            database=config_params.db)

    except mysql.connector.Error as err:
        print('Connection error:  ', err.errno)
        return None

    return conn


def find_and_remove_dupes(results):
    bus_ids = get_bus_ids(results)
    dupes = [i for i, cnt in collections.Counter(bus_ids).items() if cnt > 1]
    print('dupes:  ', dupes)

    for i in dupes:
        for j in range(len(bus_ids)):
            if bus_ids[j] == i:
                print('dupe value:  ', i)
                print('deleting index: ', j)
                del results[j]

    return results


def main():

    # set up our business search parameters:
    term = 'burgers'
    location = 'Manhattan'
    url_params = {'term': term.replace(' ', '+'),
                  'location': location.replace(' ', '+'),
                  'limit': 50}

    # make our main API call to get all results
    # pass limit of 1 for testing
    results = get_all_yelp_search_results(url_params, config.key, test_lim=500)

    print('results:  ', len(results))

    # remove dupes!
    clean_results = find_and_remove_dupes(results)

    print('clean results:  ', len(clean_results))

    # pull out the attributes we care about: id, name, rating, price rank
    records = get_bus_recs(clean_results)

    print('records:  ', len(records))

    # populate the DB with our records
    populate_db(records, 'businesses_bak', config)


if __name__ == '__main__':
    # test our script with constrained API call and
    # with test table for inserts
    main()
