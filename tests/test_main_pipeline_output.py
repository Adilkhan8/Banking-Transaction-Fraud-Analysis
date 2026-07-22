import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import main


class MainPipelineOutputTests(unittest.TestCase):
    def test_run_pipeline_capture_output_returns_report_text(self) -> None:
        def fake_dataset_summary(transactions, customers, merchants):
            print("Loaded and cleaned datasets:")

        def fake_fraud_summary(flagged_transactions):
            print("Fraud detection results:")

        with patch("main.ingest_data", return_value=(object(), object(), object())):
            with patch("main.apply_all_fraud_rules", return_value=object()):
                with patch("main.print_dataset_summary", side_effect=fake_dataset_summary):
                    with patch("main.print_fraud_summary", side_effect=fake_fraud_summary):
                        with patch("main.show_transactions") as mock_show:
                            result = main.run_pipeline(show_sample=False, capture_output=True)

        self.assertIn("Loaded and cleaned datasets:", result)
        self.assertIn("Fraud detection results:", result)
        mock_show.assert_not_called()

    def test_run_pipeline_return_results_includes_flagged_transactions(self) -> None:
        fake_transactions = object()
        fake_flagged_transactions = object()

        with patch("main.ingest_data", return_value=(fake_transactions, object(), object())):
            with patch("main.apply_all_fraud_rules", return_value=fake_flagged_transactions):
                result = main.run_pipeline(show_sample=False, return_results=True)

        self.assertEqual(result["transactions"], fake_transactions)
        self.assertIs(result["flagged_transactions"], fake_flagged_transactions)


if __name__ == "__main__":
    unittest.main()
