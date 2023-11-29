import unittest
import pandas as pd
from src.loader import slack_parser

class SlackParserTestCase(unittest.TestCase):
    def test_slack_parser_columns(self):
        # Provide the path to the test data file
        slack_data_file = 'tests/test.json'

        # Call the slack_parser function with the file path to get the DataFrame
        df = slack_parser(slack_data_file)

        # Define the expected columns
        expected_columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type', 'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

        # Check if the DataFrame has the expected columns
        self.assertEqual(list(df.columns), expected_columns)

if __name__ == '__main__':
    unittest.main()