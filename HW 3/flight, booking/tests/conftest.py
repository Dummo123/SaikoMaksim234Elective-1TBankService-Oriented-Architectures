import sys
from unittest.mock import MagicMock

# Мокаем сгенерированные protobuf модули — они есть только внутри Docker
sys.modules['flight_pb2'] = MagicMock()
sys.modules['flight_pb2_grpc'] = MagicMock()
