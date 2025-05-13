import unittest
from datetime import datetime


class TestCreateInvoiceIntegration(unittest.TestCase):

    def setUp(self):
        self.db = get_db_connection()
        self.cursor = self.db.cursor(dictionary=True)

        self.test_uuid = f"test-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.order_data = {
            "date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "uuid": self.test_uuid,
            "products": [
                {"product_nr": 101, "quantity": 2, "unit_price": 10.0},
                {"product_nr": 102, "quantity": 1, "unit_price": 15.0}
            ]
        }

    def test_create_invoice_inserts_data(self):
        invoice = create_invoice(self.order_data)

        self.assertIn("id", invoice)
        self.assertEqual(invoice["uuid"], self.test_uuid)
        self.assertEqual(invoice["total_amount"], 35.0)

        # Query invoice directly from DB to confirm it's there
        self.cursor.execute("SELECT * FROM invoices WHERE uuid = %s", (self.test_uuid,))
        row = self.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["total"], 35.0)
        self.assertEqual(row["currency"], "USD")

        # Check invoice items
        self.cursor.execute("SELECT COUNT(*) as count FROM invoice_items WHERE invoice_id = %s", (invoice["id"],))
        item_count = self.cursor.fetchone()["count"]
        self.assertEqual(item_count, 2)

    def tearDown(self):
        # Clean up the inserted test data
        self.cursor.execute("DELETE FROM invoice_items WHERE invoice_id IN (SELECT id FROM invoices WHERE uuid = %s)", (self.test_uuid,))
        self.cursor.execute("DELETE FROM invoices WHERE uuid = %s", (self.test_uuid,))
        self.db.commit()
        self.cursor.close()
        self.db.close()

if __name__ == '_main_':
    unittest.main()