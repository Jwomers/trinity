from pathlib import Path

from eth.tools.logging import ExtendedDebugLogger


class BasePeerInfoPersistance:
    def __init__(self, logger: ExtendedDebugLogger) -> None:
        self.logger = logger


class NoopPeerInfoPersistance(BasePeerInfoPersistance):
    def __init__(self) -> None:
        super().__init__(None)


class SQLPeerInfoPersistance(BasePeerInfoPersistance):
    def __init__(self, path: Path, logger: ExtendedDebugLogger) -> None:
        super().__init__(logger)
        self.path = path


class MemoryPeerInfoPersistance(SQLPeerInfoPersistance):
    def __init__(self, logger: ExtendedDebugLogger) -> None:
        super().__init__(Path(":memory:"), logger)
