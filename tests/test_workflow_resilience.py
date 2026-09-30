import unittest
from pathlib import Path


class CollectionWorkflowResilienceTests(unittest.TestCase):
    def test_failure_diagnostics_and_checkpoint_are_committed(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "collect_market.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("id: preflight", workflow)
        self.assertIn("id: collection", workflow)
        self.assertGreaterEqual(workflow.count("continue-on-error: true"), 3)
        self.assertIn("- name: Checkpoint database\n        if: always()", workflow)
        self.assertIn("- name: Commit summary database\n        if: always()", workflow)
        self.assertIn("Preserve collection failure status", workflow)

    def test_manual_recovery_inputs_are_exposed(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "collect_market.yml"
        ).read_text(encoding="utf-8")

        for input_name in (
            "date:",
            "mode:",
            "max_tickers:",
            "resume_from_checkpoint:",
        ):
            self.assertIn(input_name, workflow)
        self.assertIn("args+=(--resume-from-checkpoint)", workflow)


if __name__ == "__main__":
    unittest.main()
