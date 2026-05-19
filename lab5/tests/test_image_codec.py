import unittest

from people_counter.processing.image_codec import (
    decode_image_bytes_base64,
    encode_image_bytes_base64,
)


class ImageCodecTests(unittest.TestCase):
    def test_base64_bytes_round_trip(self):
        raw = b"fake-jpeg-bytes"

        encoded = encode_image_bytes_base64(raw)
        decoded = decode_image_bytes_base64(encoded)

        self.assertIsInstance(encoded, str)
        self.assertEqual(decoded, raw)


if __name__ == "__main__":
    unittest.main()
