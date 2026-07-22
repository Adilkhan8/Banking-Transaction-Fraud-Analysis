import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest import _resolve_csv_path


class IngestPathTests(unittest.TestCase):
    def test_resolve_default_filename_uses_raw_data_directory(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        resolved = _resolve_csv_path("banking_transactions_20000.csv")

        self.assertEqual(
            resolved,
            project_root / "Data" / "raw" / "banking_transactions_20000.csv",
        )


if __name__ == "__main__":
    unittest.main()
