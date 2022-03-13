import unittest
import os.path
import pandas as pd
from ScistarterAPI import ScistarterAPI


class TestScistarterAPI(unittest.TestCase):
    
    def setUp(self):
        self.api = ScistarterAPI()
        self.fields = ['location_point',
                       'start_datetimes',
                       'has_end',
                       'end_datetimes']
    
    def test_send_request(self):
        url = f'{self.api.BASE_URL}{self.api.endpoints["opportunities_list"]}'
        self.assertIsNotNone(self.api.send_request(url))
        pass

    def test_get_opportunities(self):
        opp = self.api.get_opportunities()
        self.assertGreater(len(opp), 0)
        save_base_url = self.api.BASE_URL
        self.api.BASE_URL = ''
        opp = self.api.get_opportunities()
        self.assertIsNone(opp)
        self.api.BASE_URL = save_base_url
        pass

    def test_get_opportunity_info(self):
        valid_uid = '786f01ae-2d8b-567c-8f71-67b8a3d53e40'
        obj = self.api.get_opportunity_info(valid_uid)
        self.assertIsNotNone(obj)
        obj = self.api.get_opportunity_info(valid_uid, fields=self.fields)
        self.assertEqual(len(obj), len(self.fields))
        invalid_uid = '786f01a0'
        obj = self.api.get_opportunity_info(invalid_uid)
        self.assertIsNone(obj)
        obj = self.api.get_opportunity_info(invalid_uid, fields=self.fields)
        self.assertIsNone(obj)
        pass

    def test_load_opportunities_df(self):
        # create new df
        self.api.load_opportunities_df('test_df.csv', fields=self.fields)
        self.assertIsNotNone(self.api.opportunities)
        self.assertIsNotNone(self.api.opportunities_df)
        self.assertTrue(os.path.isfile('test_df.csv'))
        # load existing df
        self.api = ScistarterAPI()
        self.assertIsNone(self.api.opportunities)
        self.assertIsNone(self.api.opportunities_df)
        self.api.load_opportunities_df('test_df.csv', fields=self.fields)
        self.assertIsNotNone(self.api.opportunities)
        self.assertIsNotNone(self.api.opportunities_df)
        # load existing df with missing rows
        self.api = ScistarterAPI()
        remove_last_line = pd.read_csv('test_df.csv')
        remove_last_line[:-1].to_csv('test_df.csv', index=False)
        self.api.load_opportunities_df('test_df.csv', fields=self.fields)
        self.assertIsNotNone(self.api.opportunities)
        self.assertIsNotNone(self.api.opportunities_df)
        pass

if __name__ == '__main__':
    unittest.main()
    
    
    
    # load_opportunities_df, get_opportunity_info, get_opportunities, send_request, __init__