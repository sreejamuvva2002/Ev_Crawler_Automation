from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.ev_llm_compare.excel_loader import (
    load_questions,
    load_reference_answers,
    load_workbook,
)


class ExcelLoaderTests(unittest.TestCase):
    def test_load_workbook_handles_tabular_and_note_sheets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "input.xlsx"
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                pd.DataFrame(
                    [{"Company": "A", "Category": "Tier 1", "Location": "Atlanta"}]
                ).to_excel(writer, sheet_name="Data", index=False)
                pd.DataFrame({"Definitions": ["Tier 1 means direct supplier"]}).to_excel(
                    writer, sheet_name="Definitions", index=False
                )

            rows, notes = load_workbook(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(len(notes), 1)
            self.assertEqual(rows[0].values["Company"], "A")

    def test_load_questions_picks_question_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "questions.xlsx"
            pd.DataFrame({"Question": ["What is A?", "What is B?"]}).to_excel(
                path, index=False
            )
            questions = load_questions(path)
            self.assertEqual(questions, ["What is A?", "What is B?"])

    def test_load_reference_answers_reads_golden_answer_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "golden.xlsx"
            pd.DataFrame(
                {
                    "Question": ["What is A?", "What is B?"],
                    "Golden_Answer": ["Ref A", "Ref B"],
                }
            ).to_excel(path, index=False)
            references = load_reference_answers(path)
            self.assertEqual(references, {"What is A?": "Ref A", "What is B?": "Ref B"})


if __name__ == "__main__":
    unittest.main()
