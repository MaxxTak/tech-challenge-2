import unittest
import sys
import os

# Add project root to path so we can import the script
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.ingestion.stream_source_bronze import deserialize_value, validate_event

class TestStreamSourceBronze(unittest.TestCase):
    def test_deserialize_value_valid(self):
        self.assertEqual(deserialize_value(b'{"ano": 2023}'), {"ano": 2023})

    def test_deserialize_value_invalid(self):
        self.assertIsNone(deserialize_value(b'invalid-json'))
        self.assertIsNone(deserialize_value(None))

    def test_validate_event_valid(self):
        valid_event = {
            "ano": 2023,
            "sigla_uf": "SP",
            "serie": 2,
            "rede": 1,
            "taxa_alfabetizacao": 85.5,
            "media_portugues": 210.4
        }
        self.assertTrue(validate_event(valid_event))

    def test_validate_event_missing_field(self):
        invalid_event = {
            "ano": 2023,
            "sigla_uf": "SP"
        }
        self.assertFalse(validate_event(invalid_event))

    def test_validate_event_none_value(self):
        invalid_event = {
            "ano": 2023,
            "sigla_uf": "SP",
            "serie": 2,
            "rede": 1,
            "taxa_alfabetizacao": None,
            "media_portugues": 210.4
        }
        self.assertFalse(validate_event(invalid_event))

if __name__ == "__main__":
    unittest.main()
