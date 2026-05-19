import os
import unittest
from unittest.mock import patch

from people_counter.config import settings_from_env


class ConfigTests(unittest.TestCase):
    def test_max_frames_defaults_to_unlimited(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = settings_from_env()

        self.assertEqual(settings.max_frames, 0)

    def test_max_frames_can_be_overridden_from_environment(self):
        with patch.dict(os.environ, {"MAX_FRAMES": "12"}, clear=True):
            settings = settings_from_env()

        self.assertEqual(settings.max_frames, 12)


if __name__ == "__main__":
    unittest.main()
