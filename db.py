from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


user = 'postgres'
pw = '12345'
address = '192.168.0.47'
db = 'postgres'
port = '5432'
schema = 'public'

pool = SimpleConnectionPool(
    1, 5,
    user=user,
    password=pw,
    host=address,
    port=port,
    database=db
)


def _get_dict_cursor(conn):
    return conn.cursor(
        cursor_factory=RealDictCursor
    )


def _get_connection():
    return pool.getconn()


def _put_connection(conn):
    pool.putconn(conn)


def get_markets():
    conn = _get_connection()
    cur = _get_dict_cursor(conn)
    query = sql.SQL(
        "SELECT * FROM {}.markets;"
    ).format(
        *map(sql.Identifier, (schema,))
    )
    cur.execute(query)
    records = cur.fetchall()
    _put_connection(conn)
    return records


def clear_markets():
    conn = _get_connection()
    cur = _get_dict_cursor(conn)
    query = sql.SQL(
        "SELECT * FROM {}.markets;"
    ).format(
        *map(sql.Identifier, (schema,))
    )
    cur.execute(query)
    records = cur.fetchall()
    _put_connection(conn)
    return records


def add_market(designation, description):
    conn = self._get_connection()
    cur = conn.cursor()
    query = f'INSERT INTO {self.schema}.projects (designation, description) ' \
            'VALUES (%s, %s) ' \
            'RETURNING project_id;'
    params = (designation, description)
    cur.execute(query, params)
    record = cur.fetchone()
    conn.commit()
    self._put_connection(conn)
    return next(iter(record), None)


def add_markets(market_list):
    conn = _get_connection()
    cur = conn.cursor()

    args_str = ','.join(
        cur.mogrify(
            "(%s)",
            (market_name,)
        ).decode("utf-8")
        for market_name in market_list)
    cur.execute(
        f'INSERT INTO {schema}.markets '
        '(market_name) '
        f'VALUES {args_str} '
        'RETURNING market_id;'
    )
    records = [next(iter(record))
               for record in cur.fetchall() if len(record) > 0]
    conn.commit()
    _put_connection(conn)
    return records


def get_companies():
    conn = _get_connection()
    cur = _get_dict_cursor(conn)
    query = sql.SQL(
        "SELECT * FROM {}.companies;"
    ).format(
        *map(sql.Identifier, (schema,))
    )
    cur.execute(query)
    records = cur.fetchall()
    _put_connection(conn)
    return records


def add_companies(company_list):
    conn = _get_connection()
    cur = conn.cursor()

    args_str = ','.join(
        cur.mogrify(
            "(%s)",
            (company_name,)
        ).decode("utf-8")
        for company_name in company_list)
    cur.execute(
        f'INSERT INTO {schema}.companies '
        '(company_name) '
        f'VALUES {args_str} '
        'RETURNING company_id, company_name;'
    )
    records = cur.fetchall()
    conn.commit()
    _put_connection(conn)
    return records


def add_figures(figure_list):
    conn = _get_connection()
    cur = conn.cursor()

    args_str = ','.join(
        cur.mogrify(
            "(%s)",
            (market_name,)
        ).decode("utf-8")
        for market_name in figure_list)
    cur.execute(
        f'INSERT INTO {schema}.markets '
        '(market_name) '
        f'VALUES {args_str} '
        'RETURNING market_id;'
    )
    records = [next(iter(record))
               for record in cur.fetchall() if len(record) > 0]
    conn.commit()
    _put_connection(conn)
    return records
