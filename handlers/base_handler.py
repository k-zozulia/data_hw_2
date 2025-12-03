"""
Base Handler - abstract interface for all file format handlers
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterator, Callable
from pathlib import Path


class BaseFileHandler(ABC):
    """Abstract base class for file format handlers"""

    def __init__(self, filepath: Path = None):
        self.filepath = Path(filepath) if filepath else None

    @abstractmethod
    def read(self, file_path: Path = None) -> List[Dict[str, Any]]:
        """Read entire file and return list of records"""
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], filepath: Path = None) -> None:
        """Write data to file"""
        pass

    @abstractmethod
    def read_chunks(
        self, filepath: Path = None, chunk_size: int = 1000
    ) -> Iterator[List[Dict[str, Any]]]:
        """Read file in chunks (streaming)"""
        pass

    def filter(
        self, data: List[Dict[str, Any]], condition: Callable[[Dict], bool]
    ) -> List[Dict[str, Any]]:
        """Filter data by condition"""
        return [record for record in data if condition(record)]

    def aggregate(
        self, data: List[Dict[str, Any]], column: str, operation: str = "sum"
    ) -> float:
        """
        Perform aggregation on a column

        Supported operations: sum, count, min, max, avg
        """
        if not data:
            return 0

        values = [
            record.get(column) for record in data if record.get(column) is not None
        ]

        if not values:
            return 0

        if operation == "sum":
            return sum(values)
        elif operation == "count":
            return len(values)
        elif operation == "min":
            return min(values)
        elif operation == "max":
            return max(values)
        elif operation == "avg":
            return sum(values) / len(values) if values else 0
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def sort(
        self, data: List[Dict[str, Any]], column: str, reverse: bool = False
    ) -> List[Dict[str, Any]]:
        """Sort data by column"""
        return sorted(data, key=lambda x: x.get(column, 0), reverse=reverse)

    def get_file_size_mb(self, filepath: Path = None) -> float:
        """Get file size in MB"""
        path = filepath if filepath else self.filepath
        if path and path.exists():
            return path.stat().st_size / (1024 * 1024)
        return 0

    def count_rows(self, data: List[Dict[str, Any]]) -> int:
        """Count number of rows"""
        return len(data)

    def count_columns(self, data: List[Dict[str, Any]]) -> int:
        """Count number of columns"""
        if data:
            return len(data[0].keys())
        return 0
