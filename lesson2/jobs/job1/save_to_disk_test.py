import unittest
import os
import tempfile

from sales_api import save_to_disk


class TestSaveToDisk(unittest.TestCase):

    def test_save_to_disk(self):
        json_content = [
            {"client": "John Doe", "purchase_date": "2022-08-09", "product": "Laptop", "price": 1200}
        ]
        with tempfile.TemporaryDirectory() as raw_dir:
            date = '2022-08-09'
            save_to_disk(json_content, raw_dir, date)

            file_path = os.path.join(raw_dir, f"sales_{date}.json")
            self.assertTrue(os.path.exists(file_path))

    def test_invalid_record_not_saved(self):
        json_content = [
            {"client": "John Doe", "purchase_date": "2022-08-09", "product": "Laptop", "invalid_field": "value"}
        ]
        with tempfile.TemporaryDirectory() as raw_dir:
            date = '2022-08-09'
            save_to_disk(json_content, raw_dir, date)

            file_path = os.path.join(raw_dir, f"sales_{date}.json")
            self.assertFalse(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main()
