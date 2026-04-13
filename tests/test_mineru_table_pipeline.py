import unittest

from services.extraction_normalizer import normalize_extraction_result
from services.parsers.mineru.zip_parser import (
    build_flat_intermediate_block,
    build_segments_from_blocks,
)


class MinerUTablePipelineTests(unittest.TestCase):
    def test_flat_table_item_preserves_structured_fields_in_segment_metadata(self):
        item = {
            "type": "table",
            "page_idx": 2,
            "page_index": 3,
            "normalized_bbox": [0.1, 0.2, 0.8, 0.9],
            "table_body": (
                "<table><tr><td colspan=\"2\">Task</td><td>Simulated Flowchart</td></tr>"
                "<tr><td>OCR</td><td>Answer</td><td>monotrochal</td></tr></table>"
            ),
            "table_caption": ["Table 1: Common VQA tasks across both subsets."],
            "table_footnote": ["Footnote A", "Footnote B"],
        }

        block = build_flat_intermediate_block(item, 1)

        self.assertIsNotNone(block)
        self.assertEqual(block["table_caption_text"], "Table 1: Common VQA tasks across both subsets.")
        self.assertEqual(block["table_footnote_texts"], ["Footnote A", "Footnote B"])
        self.assertIn("<table>", block["table_html"])
        self.assertIn("| Task |", block["table_text"])

        segments, _ = build_segments_from_blocks([block])

        self.assertEqual(len(segments), 1)
        segment = segments[0]
        self.assertEqual(segment["source_type"], "table")
        self.assertEqual(segment["metadata"]["table_caption"], "Table 1: Common VQA tasks across both subsets.")
        self.assertEqual(segment["metadata"]["table_footnote"], ["Footnote A", "Footnote B"])
        self.assertIn("<table>", segment["metadata"]["table_html"])

    def test_normalizer_prefers_explicit_mineru_caption_and_footnotes(self):
        table_html = (
            "<table>"
            "<tr><td colspan=\"2\">Task</td><td>Simulated Flowchart</td></tr>"
            "<tr><td rowspan=\"2\">OCR</td><td>Question</td><td>monotrochal</td></tr>"
            "<tr><td>Answer</td><td>monotrochal</td></tr>"
            "</table>"
        )
        parser_result = {
            "file_type": "pdf",
            "metadata": {"parser": "mineru_api"},
            "segments": [
                {
                    "segment_id": "mineru-table-1",
                    "text": "garbled fallback text",
                    "source_type": "table",
                    "source_index": 3,
                    "block_index": 7,
                    "paragraph_index": None,
                    "metadata": {
                        "role": "table",
                        "table_html": table_html,
                        "table_caption": "Table 1: Common VQA tasks across both subsets.",
                        "table_footnote": ["Footnote A", "Footnote B"],
                    },
                }
            ],
        }

        blocks, assets, metadata = normalize_extraction_result(
            document_id="doc-1",
            parser_result=parser_result,
        )

        self.assertEqual(len(assets), 0)
        self.assertEqual(metadata["block_count"], 1)
        block = blocks[0]
        normalized = block["normalized_content"]

        self.assertEqual(block["caption_text"], "Table 1: Common VQA tasks across both subsets.")
        self.assertEqual(normalized["footnotes"], ["Footnote A", "Footnote B"])
        self.assertEqual(len(normalized["merged_cells"]), 2)
        self.assertEqual(normalized["matrix"][0], ["Task", "Task", "Simulated Flowchart"])
        self.assertEqual(normalized["matrix"][1], ["OCR", "Question", "monotrochal"])
        self.assertIn("Table 1: Common VQA tasks across both subsets.", normalized["linearized_text"])
        self.assertIn("Footnote A", normalized["linearized_text"])

    def test_normalizer_falls_back_to_text_parsing_when_html_missing(self):
        parser_result = {
            "file_type": "pdf",
            "metadata": {"parser": "mineru_api"},
            "segments": [
                {
                    "segment_id": "mineru-table-2",
                    "text": "Table 2: Fallback caption\n| Col A | Col B |\n| --- | --- |\n| 1 | 2 |",
                    "source_type": "table",
                    "source_index": 1,
                    "block_index": 2,
                    "paragraph_index": None,
                    "metadata": {
                        "role": "table",
                    },
                }
            ],
        }

        blocks, _, _ = normalize_extraction_result(
            document_id="doc-2",
            parser_result=parser_result,
        )

        block = blocks[0]
        normalized = block["normalized_content"]

        self.assertEqual(block["caption_text"], "Table 2: Fallback caption")
        self.assertEqual(normalized["matrix"], [["Col A", "Col B"], ["1", "2"]])
        self.assertEqual(normalized["footnotes"], [])


if __name__ == "__main__":
    unittest.main()
