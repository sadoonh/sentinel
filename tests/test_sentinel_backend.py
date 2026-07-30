import unittest

import pandas as pd

import sentinel_backend as backend


class KeyNormalizationTests(unittest.TestCase):
    def test_exact_normalization_preserves_the_key(self):
        self.assertEqual(
            backend.normalize_comparison_key(" Folder/Report.CSV ", "exact"),
            " Folder/Report.CSV ",
        )

    def test_text_normalization_handles_case_unicode_and_whitespace(self):
        self.assertEqual(
            backend.normalize_comparison_key("  ＲＥＰＯＲＴ   123  ", "text"),
            "report 123",
        )

    def test_filename_normalization_handles_paths_and_compound_extensions(self):
        self.assertEqual(
            backend.normalize_comparison_key(
                r"C:\incoming\Daily_Report.CSV.GZ", "filename"
            ),
            "daily_report",
        )

    def test_filename_normalization_handles_pdf_and_xlsm_extensions(self):
        for filename in ("Report.PDF", "Report.XLSM"):
            with self.subTest(filename=filename):
                self.assertEqual(
                    backend.normalize_comparison_key(filename, "filename"),
                    "report",
                )

    def test_unknown_normalization_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            backend.normalize_comparison_key("report", "unknown")


class PipelineKeyMatchingTests(unittest.TestCase):
    def setUp(self):
        self.notebook_tables = {
            "source": pd.DataFrame(
                {
                    "file_name": ["incoming/Report_123.CSV"],
                    "seen_at": [pd.Timestamp("2026-01-01")],
                }
            ),
            "target": pd.DataFrame(
                {
                    "file_name": ["report_123"],
                    "seen_at": [pd.Timestamp("2026-01-02")],
                }
            ),
        }

    @staticmethod
    def stage(table, name, normalization="exact"):
        return {
            "stage_name": name,
            "schema": backend.NOTEBOOK_SCHEMA,
            "table": table,
            "comparison_column": "file_name",
            "timestamp_column": "seen_at",
            "key_normalization": normalization,
        }

    def test_exact_keys_remain_separate(self):
        result, _ = backend.run_pipeline(
            engine=None,
            stages=[
                self.stage("source", "source"),
                self.stage("target", "target"),
            ],
            notebook_tables=self.notebook_tables,
        )

        self.assertEqual(len(result), 2)

    def test_filename_normalization_matches_extensionless_key(self):
        result, _ = backend.run_pipeline(
            engine=None,
            stages=[
                self.stage("source", "source", "filename"),
                self.stage("target", "target", "filename"),
            ],
            notebook_tables=self.notebook_tables,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "compare_column"], "report_123")
        self.assertEqual(result.loc[0, "exists_in"], "source, target")

    def test_drilldown_loads_full_rows_from_each_matching_stage(self):
        self.notebook_tables["source"]["detail"] = ["source detail"]
        self.notebook_tables["target"]["detail"] = ["target detail"]
        stages = [
            self.stage("source", "source", "filename"),
            self.stage("target", "target", "filename"),
        ]
        result, _ = backend.run_pipeline(
            engine=None,
            stages=stages,
            notebook_tables=self.notebook_tables,
        )

        drilldown = backend.load_record_drilldown(
            engine=None,
            stages=stages,
            notebook_tables=self.notebook_tables,
            pipeline_result=result,
            compare_key="report_123",
        )

        self.assertEqual(list(drilldown), ["source", "target"])
        self.assertEqual(drilldown["source"].loc[0, "detail"], "source detail")
        self.assertEqual(drilldown["target"].loc[0, "detail"], "target detail")
        self.assertIsInstance(drilldown["source"].index, pd.RangeIndex)

    def test_normalization_collisions_are_aggregated_and_reported(self):
        self.notebook_tables["source"] = pd.DataFrame(
            {
                "file_name": ["Report.csv", "report.json"],
                "seen_at": [
                    pd.Timestamp("2026-01-01"),
                    pd.Timestamp("2026-01-03"),
                ],
            }
        )
        result, _ = backend.run_pipeline(
            engine=None,
            stages=[
                self.stage("source", "source", "filename"),
                self.stage("target", "target", "filename"),
            ],
            notebook_tables=self.notebook_tables,
            duplicate_policy="latest",
        )

        source_row = result.loc[result["compare_column"] == "report"].iloc[0]
        self.assertEqual(source_row["source"], pd.Timestamp("2026-01-03"))
        self.assertTrue(result.attrs["matching_warnings"])
        self.assertIn("combined 1 canonical key", result.attrs["matching_warnings"][0])

    def test_mismatched_record_filter_removes_rows_in_every_stage(self):
        self.notebook_tables["source"].loc[1] = [
            "only_source.csv",
            pd.Timestamp("2026-01-03"),
        ]
        result, stage_names = backend.run_pipeline(
            engine=None,
            stages=[
                self.stage("source", "source", "filename"),
                self.stage("target", "target", "filename"),
            ],
            notebook_tables=self.notebook_tables,
        )

        filtered = backend.filter_mismatched_records(result, stage_names)

        self.assertEqual(filtered["compare_column"].tolist(), ["only_source"])
        self.assertIsInstance(filtered.index, pd.RangeIndex)

    def test_invalid_stage_normalization_fails_validation(self):
        with self.assertRaisesRegex(
            backend.PipelineValidationError, "unsupported key normalization"
        ):
            backend.run_pipeline(
                engine=None,
                stages=[
                    self.stage("source", "source", "unknown"),
                    self.stage("target", "target"),
                ],
                notebook_tables=self.notebook_tables,
            )


class PresetTests(unittest.TestCase):
    def test_preset_persists_key_normalization(self):
        preset = backend.make_monitor_preset(
            stages=[
                {
                    "stage_name": "source",
                    "schema": "public",
                    "table": "events",
                    "comparison_column": "file_name",
                    "timestamp_column": "created_at",
                    "key_normalization": "filename",
                }
            ],
            duplicate_policy="latest",
            lookback_days=0,
            mismatched_records_only=True,
        )

        self.assertEqual(preset["stages"][0]["key_normalization"], "filename")
        self.assertTrue(preset["mismatched_records_only"])


if __name__ == "__main__":
    unittest.main()
