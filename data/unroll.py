# find_nested_etfs.py
import sys
from collections import defaultdict
from utilities.database import open_db


def find_nested(conn):
    sql = """
    SELECT
        v.etf_isin      AS parent_isin,
        v.holding_isin  AS nested_isin,
        e.issuer        AS nested_issuer,
        COALESCE(e.name, '') AS nested_name,
        v.weight
    FROM v_holdings v
    JOIN etfs e
      ON e.isin = v.holding_isin
    ORDER BY parent_isin, nested_isin;
    """
    cur = conn.execute(sql)
    rows = [
        {
            "parent_isin": r[0],
            "nested_isin": r[1],
            "nested_issuer": r[2],
            "nested_name": r[3],
            "weight": r[4],
        }
        for r in cur.fetchall()
    ]
    return rows


def main(db_path: str = "database.db"):
    with open_db(db_path) as conn:
        rows = find_nested(conn)

    if not rows:
        print("No nested ETFs found.")
        return

    # Unique list of ETFs to unroll
    nested_isins = sorted({r["nested_isin"] for r in rows})
    print(f"ETFs to unroll ({len(nested_isins)}):")
    print(nested_isins)  # ready to paste into your pipeline

    # Optional: show where they appear
    by_parent = defaultdict(list)
    for r in rows:
        by_parent[r["parent_isin"]].append(r)

    print("\nDetails (parent → nested @ weight):")
    for parent, items in by_parent.items():
        print(f"- {parent}")
        for r in items:
            w = f"{r['weight']:.4f}" if r["weight"] is not None else "NA"
            print(
                f"    → {r['nested_isin']} ({r['nested_issuer']}) @ {w}  {r['nested_name'][:60]}"
            )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "database.db")
