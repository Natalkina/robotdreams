import unittest
from sales_api import is_valid_record

class TestIsValidRecord(unittest.TestCase):

    def test_valid_record(self):
        record = {
            "client": "John Doe",
            "purchase_date": "2022-08-09",
            "product": "Laptop",
            "price": 1200
        }
        result = is_valid_record(record)
        self.assertTrue(result)

    def test_invalid_record(self):
        record = {
            "client": "John Doe",
            "purchase_date": "2022-08-09",
            "product": "Laptop",
            # Missing 'price' field
        }
        result = is_valid_record(record)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
