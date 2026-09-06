"""Small inventory fixture for Ray local engineering evaluations."""
def silver_inventory(rows):
    latest = {}
    for row in rows:
        key = row["product_id"]
        if not isinstance(key, str) or not key or key.strip() != key:
            raise ValueError("product_id must be a canonical string")
        quantity = row["quantity"]
        if type(quantity) is not int or quantity < 0:
            raise ValueError("quantity must be a nonnegative integer")
        if key not in latest or row["updated_at"] > latest[key]["updated_at"]:
            latest[key] = dict(row)
    return [latest[key] for key in sorted(latest)]
