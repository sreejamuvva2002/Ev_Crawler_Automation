import unittest

from src.ev_llm_compare.retrieval import HybridRetriever, build_collection_fingerprint
from src.ev_llm_compare.schemas import Chunk
from src.ev_llm_compare.settings import RetrievalSettings


class RetrievalTests(unittest.TestCase):
    def test_collection_fingerprint_changes_when_chunk_content_changes(self) -> None:
        chunks_a = [
            Chunk(chunk_id="1", text="Company: A", metadata={"row_key": "row-1"}),
            Chunk(chunk_id="2", text="Company: B", metadata={"row_key": "row-2"}),
        ]
        chunks_b = [
            Chunk(chunk_id="1", text="Company: A", metadata={"row_key": "row-1"}),
            Chunk(chunk_id="2", text="Company: C", metadata={"row_key": "row-2"}),
        ]

        self.assertNotEqual(
            build_collection_fingerprint(chunks_a, "embedding-model"),
            build_collection_fingerprint(chunks_b, "embedding-model"),
        )

    def test_query_plan_detects_structured_aggregation_queries(self) -> None:
        retriever = HybridRetriever.__new__(HybridRetriever)
        retriever.known_categories = ["Tier 1", "Tier 1/2"]
        retriever.known_companies = ["Acme EV"]
        retriever.role_terms = ["battery pack", "battery cell"]

        plan = HybridRetriever._plan_query(
            retriever,
            "Show all Tier 1 companies grouped by EV Supply Chain Role for battery pack.",
        )

        self.assertEqual(plan.intent, "aggregation")
        self.assertTrue(plan.prefer_structured)
        self.assertTrue(plan.group_by_role)
        self.assertEqual(plan.matched_categories, ["Tier 1"])
        self.assertEqual(plan.matched_role_terms, ["battery pack"])

    def test_structured_summary_prefers_grouped_output_for_exhaustive_role_queries(self) -> None:
        retriever = HybridRetriever.__new__(HybridRetriever)
        retriever.settings = RetrievalSettings(structured_summary_limit=2)
        query_plan = HybridRetriever._plan_query(
            self._seed_retriever_for_summary(retriever),
            "Show all Tier 1 companies grouped by EV Supply Chain Role.",
        )
        matched_rows = [
            {
                "company": "A",
                "category": "Tier 1",
                "ev_supply_chain_role": "Battery Pack",
                "product_service": "Pack",
                "location": "Atlanta",
                "employment": "100",
                "source_file": "input.xlsx",
                "sheet_name": "Data",
                "row_number": "1",
                "row_key": "row-1",
                "row_summary": "A",
            },
            {
                "company": "B",
                "category": "Tier 1",
                "ev_supply_chain_role": "Battery Pack",
                "product_service": "Pack",
                "location": "Atlanta",
                "employment": "100",
                "source_file": "input.xlsx",
                "sheet_name": "Data",
                "row_number": "2",
                "row_key": "row-2",
                "row_summary": "B",
            },
            {
                "company": "C",
                "category": "Tier 1",
                "ev_supply_chain_role": "Battery Pack",
                "product_service": "Pack",
                "location": "Atlanta",
                "employment": "100",
                "source_file": "input.xlsx",
                "sheet_name": "Data",
                "row_number": "3",
                "row_key": "row-3",
                "row_summary": "C",
            },
        ]

        summary = HybridRetriever._build_structured_summary(retriever, query_plan, matched_rows)
        self.assertIn("Grouped by EV Supply Chain Role:", summary)
        self.assertIn("- Battery Pack: A; B; C", summary)

    def test_query_plan_does_not_treat_oem_contracts_as_oem_category(self) -> None:
        retriever = HybridRetriever.__new__(HybridRetriever)
        retriever.known_categories = ["OEM", "Tier 1", "Tier 2/3"]
        retriever.known_companies = []
        retriever.known_locations = []
        retriever.known_primary_oems = []
        retriever.role_terms = ["dc fast charging"]

        plan = HybridRetriever._plan_query(
            retriever,
            "Which suppliers manufacture DC fast charging hardware and have existing OEM contracts?",
        )

        self.assertEqual(plan.matched_categories, [])

    def _seed_retriever_for_summary(self, retriever: HybridRetriever) -> HybridRetriever:
        retriever.known_categories = ["Tier 1"]
        retriever.known_companies = []
        retriever.known_locations = []
        retriever.known_primary_oems = []
        retriever.role_terms = ["battery pack"]
        return retriever


if __name__ == "__main__":
    unittest.main()
