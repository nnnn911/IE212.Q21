import unittest

from people_counter.analytics import build_stats_query


class AnalyticsSqlTests(unittest.TestCase):
    def test_minute_query_groups_by_camera_and_minute_bucket(self):
        query = build_stats_query("minute")

        self.assertIn("date_trunc('minute', timestamp)", query)
        self.assertIn("GROUP BY camera_id, bucket", query)

    def test_query_can_filter_by_camera_id(self):
        query = build_stats_query("camera", camera_id="people-video")

        self.assertIn("WHERE camera_id = 'people-video'", query)

    def test_hour_query_groups_by_hour_bucket(self):
        query = build_stats_query("hour")

        self.assertIn("date_trunc('hour', timestamp)", query)

    def test_camera_query_groups_by_camera_only(self):
        query = build_stats_query("camera")

        self.assertIn("GROUP BY camera_id", query)
        self.assertNotIn("date_trunc", query)

    def test_invalid_group_by_is_rejected(self):
        with self.assertRaises(ValueError):
            build_stats_query("day")


if __name__ == "__main__":
    unittest.main()
