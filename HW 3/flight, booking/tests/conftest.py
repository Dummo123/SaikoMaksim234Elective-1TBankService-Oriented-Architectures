import sys
from unittest.mock import MagicMock

# Protobuf modules are generated inside Docker — mock them for local test runs
sys.modules["flight_pb2"]      = MagicMock()
sys.modules["flight_pb2_grpc"] = MagicMock()
