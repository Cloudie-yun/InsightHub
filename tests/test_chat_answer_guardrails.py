import unittest

from services.chat_answer_service import ChatAnswerService
from services.text_answer_service import NO_EVIDENCE_PROMPT_VERSION


class ChatAnswerGuardrailTests(unittest.TestCase):
    def setUp(self):
        self.service = ChatAnswerService.__new__(ChatAnswerService)
        self.retrieval_payload = {
            "strategy": "postgres_hybrid",
            "returned_count": 2,
            "results": [
                {
                    "block_id": "block-1",
                    "document_id": "doc-1",
                    "document_name": "Alpha Notes.pdf",
                    "score": 0.93,
                    "snippet": "Alpha snippet",
                    "source_metadata": {"page": 3},
                },
                {
                    "block_id": "block-2",
                    "document_id": "doc-1",
                    "document_name": "Alpha Notes.pdf",
                    "score": 0.88,
                    "snippet": "Beta snippet",
                    "source_metadata": {"page": 4},
                },
            ],
        }

    def test_missing_supported_citations_falls_back_to_no_evidence(self):
        answer_payload = {
            "answer_text": "This answer cites unsupported material.",
            "citation_block_ids": ["missing-block"],
            "model_provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "prompt_version": "grounded_answer_v2",
            "prompt_profile": "default",
            "confidence": "high",
            "response_headers": {"x-test": "1"},
        }

        guarded = self.service._apply_grounding_guardrails(
            retrieval_payload=self.retrieval_payload,
            answer_payload=answer_payload,
        )

        self.assertEqual(guarded["citation_block_ids"], [])
        self.assertEqual(guarded["confidence"], "insufficient")
        self.assertEqual(guarded["prompt_version"], NO_EVIDENCE_PROMPT_VERSION)
        self.assertEqual(guarded["model_provider"], "gemini")
        self.assertEqual(guarded["model_name"], "gemini-2.5-flash")

    def test_single_supported_citation_downgrades_high_confidence(self):
        answer_payload = {
            "answer_text": "Supported by one chunk.",
            "citation_block_ids": ["block-1", "block-1", "missing-block"],
            "model_provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "prompt_version": "grounded_answer_v2",
            "prompt_profile": "default",
            "confidence": "high",
            "response_headers": {},
        }

        guarded = self.service._apply_grounding_guardrails(
            retrieval_payload=self.retrieval_payload,
            answer_payload=answer_payload,
        )

        self.assertEqual(guarded["citation_block_ids"], ["block-1"])
        self.assertEqual(guarded["confidence"], "partial")

        citations = ChatAnswerService._build_citations(
            retrieval_payload=self.retrieval_payload,
            answer_payload=guarded,
        )
        self.assertEqual(len(citations), 1)
        self.assertEqual(citations[0]["block_id"], "block-1")
        self.assertEqual(citations[0]["page_label"], "p. 3")


if __name__ == "__main__":
    unittest.main()
