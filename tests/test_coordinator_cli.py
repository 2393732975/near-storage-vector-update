"""CLI errors/help must terminate before any cluster connection is attempted."""

from pathlib import Path
import subprocess
import unittest


BINARY = Path(__file__).resolve().parents[1] / "build/nsvu-update-coordinator"


@unittest.skipUnless(BINARY.exists(), "build Coordinator before CLI tests")
class CoordinatorCliTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [str(BINARY), *args], capture_output=True, text=True, timeout=5
        )

    def test_help_is_safe_without_cluster_or_input(self):
        proc = self.run_cli("--help")
        self.assertEqual(proc.returncode, 0)
        self.assertIn("--mode build|update|query", proc.stdout)
        self.assertEqual(proc.stderr, "")

    def test_unknown_option_does_not_fall_through_to_default_build(self):
        proc = self.run_cli("--invalid-baseline-option")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("unknown argument", proc.stderr)

    def test_query_rejects_modifying_executor_and_probe_flags(self):
        for flags in (("--distance-microbatch",), ("--distance-split-probe",),
                      ("--frontier-window", "2")):
            with self.subTest(flags=flags):
                proc = self.run_cli(
                    "--mode", "query", "--input", "/nonexistent/query.fbin",
                    "--query-results-out", "/nonexistent/output.jsonl", *flags
                )
                self.assertEqual(proc.returncode, 1)
                self.assertIn("query requires", proc.stderr)

    def test_query_rejects_search_width_below_k_before_connecting(self):
        proc = self.run_cli(
            "--mode", "query", "--input", "/nonexistent/query.fbin",
            "--query-results-out", "/nonexistent/output.jsonl", "--ef", "1"
        )
        self.assertEqual(proc.returncode, 1)
        self.assertIn("invalid/absent", proc.stderr)


if __name__ == "__main__":
    unittest.main()
