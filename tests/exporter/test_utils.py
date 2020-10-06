from datetime import datetime
from unittest import TestCase

from exporter import utils


class UtilsTest(TestCase):

    def test_parse_date_string_returns_correct_date_obj_given_iso(self):
        # given:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        # and:
        iso_date = '2019-06-12T09:49:25.000Z'

        # when:
        actual_date = utils.parse_date_string(iso_date)

        # expect:
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string_returns_correct_date_obj_given_iso_short(self):
        # given:
        iso_date_short = '2019-06-12T09:49:25Z'

        # when:
        actual_date = utils.parse_date_string(iso_date_short)

        # expect:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string_returns_correct_date_obj_given_unknown_format(self):
        # given:
        unknown = '2019:06:12Y09-49-25.000X'

        # expect:
        with self.assertRaises(ValueError):
            utils.parse_date_string(unknown)

    def test_to_dcp_version_returns_correct_dcp_format_given_short_date(self):
        # given:
        date_string = '2019-05-23T16:53:40Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.000000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version_returns_correct_dcp_format_given_3_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.931Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.931000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version_returns_correct_dcp_format_given_2_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.93Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.930000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version_returns_correct_dcp_format_given_6_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.123456Z'

        # expect:
        self.assertEqual(date_string, utils.to_dcp_version(date_string))
