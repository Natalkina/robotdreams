import unittest
from unittest.mock import patch, MagicMock
from sales_api import import_data_from_pages


class TestImportDataFromPages(unittest.TestCase):

    @patch('sales_api.requests.get')
    def test_import_data_from_pages(self, mock_get):
        mock_response1 = MagicMock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = [{'test': 'data'}]
        mock_response2 = MagicMock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = []
        mock_get.side_effect = [mock_response1, mock_response2]

        result = import_data_from_pages('2022-08-09')

        self.assertEqual(result, [{'test': 'data'}])



if __name__ == '__main__':
    unittest.main()
