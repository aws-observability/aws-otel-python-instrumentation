import psycopg2


def database_operation() -> None:
    dbname = 'testdb'
    user = 'user'
    password = 'password'
    host = 'localhost'

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

    cur = conn.cursor()

    cur.execute("""
    CREATE TEMPORARY TABLE test_table (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    cur.execute("INSERT INTO test_table (name) VALUES (%s)", ("Alice",))
    cur.execute("INSERT INTO test_table (name) VALUES (%s)", ("Bob",))

    conn.commit()

    cur.execute("SELECT id, name FROM test_table")
    rows = cur.fetchall()
    for row in rows:
        print(f"ID: {row[0]}, Name: {row[1]}")

    cur.close()
    conn.close()


def main() -> None:
    database_operation()


if __name__ == "__main__":
    main()
