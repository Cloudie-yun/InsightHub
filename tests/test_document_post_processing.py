import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.parsers.mineru.zip_parser import (
    build_segments_from_blocks,
    post_process_segments,
)


class DocumentPostProcessingTests(unittest.TestCase):
    maxDiff = None

    def print_case_output(self, title, lines):
        print(f"\n[{title}]")
        for line in lines:
            print(line)

    def test_removes_header_footer_and_page_number_noise(self):
        intermediate_blocks = [
            {
                "page_index": 1,
                "order_index": 1,
                "block_type": "page_header",
                "text": "Journal of Sample Research",
                "bbox": [0.0, 0.0, 1.0, 0.05],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:1",
                "source_anchor_key": "content_list:page:1:block:1",
            },
            {
                "page_index": 1,
                "order_index": 2,
                "block_type": "paragraph",
                "text": "This is the real paragraph content from MinerU output.",
                "bbox": [0.08, 0.12, 0.92, 0.22],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:2",
                "source_anchor_key": "content_list:page:1:block:2",
                "original_type": "text",
            },
            {
                "page_index": 1,
                "order_index": 3,
                "block_type": "page_number",
                "text": "1",
                "bbox": [0.48, 0.95, 0.52, 0.99],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:3",
                "source_anchor_key": "content_list:page:1:block:3",
            },
            {
                "page_index": 1,
                "order_index": 4,
                "block_type": "page_footer",
                "text": "Confidential draft footer",
                "bbox": [0.0, 0.95, 1.0, 1.0],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:4",
                "source_anchor_key": "content_list:page:1:block:4",
            },
        ]

        segments, segment_context = build_segments_from_blocks(intermediate_blocks)
        suppressed_counts = segment_context["suppressed_counts"]

        self.print_case_output(
            "Noise Removal",
            [
                "Input:",
                "Header: Journal of Sample Research",
                "Body: This is the real paragraph content from MinerU output.",
                "Page number: 1",
                "Footer: Confidential draft footer",
                "",
                "Output:",
                f"Kept segment count: {len(segments)}",
                f"Cleaned text: {segments[0]['text'] if segments else ''}",
                f"Suppressed header count: {suppressed_counts['page_header']}",
                f"Suppressed footer count: {suppressed_counts['page_footer']}",
                f"Suppressed page number count: {suppressed_counts['page_number']}",
            ],
        )

        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["text"], "This is the real paragraph content from MinerU output.")
        self.assertEqual(suppressed_counts["page_header"], 1)
        self.assertEqual(suppressed_counts["page_footer"], 1)
        self.assertEqual(suppressed_counts["page_number"], 1)

    def test_merges_paragraph_text_across_page_break(self):
        intermediate_blocks = [
            {
                "page_index": 1,
                "order_index": 5,
                "block_type": "paragraph",
                "text": "This paragraph continues",
                "bbox": [0.08, 0.78, 0.46, 0.84],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:5",
                "source_anchor_key": "content_list:page:1:block:5",
                "original_type": "text",
            },
            {
                "page_index": 2,
                "order_index": 1,
                "block_type": "paragraph",
                "text": "on the next page after a page break.",
                "bbox": [0.08, 0.10, 0.46, 0.16],
                "source_file": "content_list.json",
                "source_locator": "page:2:block:1",
                "source_anchor_key": "content_list:page:2:block:1",
                "original_type": "text",
            },
        ]

        segments, _ = build_segments_from_blocks(intermediate_blocks)
        processed_segments, stats = post_process_segments(segments)

        self.print_case_output(
            "Page Break Cleanup",
            [
                "Input:",
                "Page 1 text: This paragraph continues",
                "Page 2 text: on the next page after a page break.",
                "",
                "Output:",
                f"Merged segment count: {stats['merged_segment_count']}",
                f"Cleaned text: {processed_segments[0]['text'] if processed_segments else ''}",
            ],
        )

        self.assertEqual(len(processed_segments), 1)
        self.assertEqual(
            processed_segments[0]["text"],
            "This paragraph continues on the next page after a page break.",
        )
        self.assertEqual(stats["merged_segment_count"], 1)

    def test_preserves_meaningful_heading_structure(self):
        intermediate_blocks = [
            {
                "page_index": 1,
                "order_index": 1,
                "block_type": "paragraph",
                "text": "Introductory discussion ends here.",
                "bbox": [0.08, 0.14, 0.92, 0.20],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:1",
                "source_anchor_key": "content_list:page:1:block:1",
                "original_type": "text",
            },
            {
                "page_index": 1,
                "order_index": 2,
                "block_type": "heading",
                "text": "2 Results",
                "heading_level": 1,
                "bbox": [0.08, 0.24, 0.60, 0.29],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:2",
                "source_anchor_key": "content_list:page:1:block:2",
            },
            {
                "page_index": 1,
                "order_index": 3,
                "block_type": "paragraph",
                "text": "The first result paragraph starts here.",
                "bbox": [0.08, 0.33, 0.92, 0.40],
                "source_file": "content_list.json",
                "source_locator": "page:1:block:3",
                "source_anchor_key": "content_list:page:1:block:3",
                "original_type": "text",
            },
        ]

        segments, _ = build_segments_from_blocks(intermediate_blocks)
        processed_segments, stats = post_process_segments(segments)

        self.print_case_output(
            "Meaningful Content Preservation",
            [
                "Input:",
                "Paragraph: Introductory discussion ends here.",
                "Heading: 2 Results",
                "Paragraph: The first result paragraph starts here.",
                "",
                "Output:",
                f"Output segment count: {len(processed_segments)}",
                f"Heading preserved: {processed_segments[1]['text'] if len(processed_segments) > 1 else ''}",
                f"Merged segment count: {stats['merged_segment_count']}",
            ],
        )

        self.assertEqual(len(processed_segments), 3)
        self.assertEqual(processed_segments[1]["metadata"]["role"], "heading")
        self.assertEqual(processed_segments[1]["text"], "2 Results")
        self.assertEqual(stats["merged_segment_count"], 0)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(DocumentPostProcessingTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)

    if result.wasSuccessful():
        print("\nTest Result: PASS")
    else:
        print("\nTest Result: FAIL")
