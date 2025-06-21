import psycopg2

conn = psycopg2.connect(
    host="localhost", dbname="postgres", user="postgres", password="iosamir1", port=6969
)
cur = conn.cursor()

cur.execute(
    """--sql
    CREATE TABLE IF NOT EXISTS person(
        id INT PRIMARY KEY,
        name VARCHAR(255),
        age INT,
        gender CHAR
    )
    """
)

cur.execute(
    """--sql
            INSERT INTO person (id, name, age, gender) VALUES
            (1, 'Ludo', 22, 'm'),
            (2, 'Elia', 15, 'm'),
            (3, 'Marta', 65, 'f'),
            (4, 'Lorenzo', 92, 'm');
            """
)
conn.commit()
cur.close()
conn.close()
