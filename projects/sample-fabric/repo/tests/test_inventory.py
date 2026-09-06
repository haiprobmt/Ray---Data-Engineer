import unittest
from inventory import silver_inventory

class InventoryTests(unittest.TestCase):
    def test_latest_preserves_leading_zero(self):
        rows = [{"product_id": "001", "quantity": 1, "updated_at": "2026-09-01"},
                {"product_id": "001", "quantity": 2, "updated_at": "2026-09-02"}]
        self.assertEqual(silver_inventory(rows), [rows[1]])
        self.assertEqual(silver_inventory(rows), silver_inventory(rows + rows))

    def test_negative_quantity_rejected(self):
        with self.assertRaises(ValueError):
            silver_inventory([{"product_id": "001", "quantity": -1, "updated_at": "2026-09-01"}])

if __name__ == "__main__":
    unittest.main()
