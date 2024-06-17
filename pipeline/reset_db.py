"""Reset the special_requests and rating tables."""
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor
from pipeline import get_connection, get_cursor
from extract import get_config


def clear_table(connector: connection, cursor: cursor) -> None:
    """Clears the data in the two tables."""
    clear_special_req_query = """TRUNCATE special_requests RESTART IDENTITY;"""
    clear_rating_query = """TRUNCATE rating RESTART IDENTITY;"""
    cursor.execute(clear_special_req_query)
    cursor.execute(clear_rating_query)
    connector.commit()
    cursor.close()
    connector.close()


def main():
    """Run main function from here"""
    config = get_config()
    conn = get_connection(config)
    cur = get_cursor(conn)

    clear_table(conn, cur)


if __name__ == "__main__":
    main()
