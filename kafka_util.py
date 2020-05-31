import json
import logging
import logging.config
from pathlib import Path

logging.config.fileConfig(Path(__file__).parents[0] / "logging.ini")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class JsonSerializer:

    def serialize(self, record: dict):
        """Serialize  a kafka record dict into encoded JSON bytes"""
        return json.dumps(record).encode('utf-8')

    def deserialize(self, record: bytes):
        """De-serialzie a encoded JSON kafka bytes record into dict"""
        return json.loads(record.decode('utf-8'))