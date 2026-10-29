import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("check_doc_links", Path(__file__).parents[1] / "check_doc_links.py")
CHECK = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECK
SPEC.loader.exec_module(CHECK)


class DocLinksTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.tracked = set()

    def put(self, path, value="", tracked=True):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(value)
        if tracked:
            self.tracked.add(path)
        return file

    def check(self):
        return CHECK.check(self.root, self.tracked)

    def test_local_targets_and_heading_fragments(self):
        self.put("docs/a.md", "[text](b.md#same-heading-1) ![asset](asset.png) [root](../README.md#main)")
        self.put("docs/b.md", "# Same heading\n## Same heading\n")
        self.put("docs/asset.png")
        self.put("README.md", "# Main\n")
        self.assertEqual((3, []), self.check())

    def test_existing_ignored_jobs_cannot_satisfy_link(self):
        self.put("docs/a.md", "[private result](../jobs/batch/result.json)")
        self.put("jobs/batch/result.json", "{}", tracked=False)
        self.assertIn("target is not tracked", self.check()[1][0])

    def test_escaped_path_is_rejected_without_access(self):
        self.put("docs/a.md", "[bad](../../outside.md) [encoded](%2fUsers%2fprivate.md)")
        self.assertEqual(2, len(self.check()[1]))

    def test_code_comments_and_external_urls_are_ignored(self):
        self.put("docs/a.md", "[web](https://example.com/a) ` [no](missing) `\n```md\n[no](missing)\n```\n<!-- [no](missing) -->\n")
        self.assertEqual((0, []), self.check())

    def test_reference_angle_encoded_and_balanced_links(self):
        self.put("docs/a.md", '[one][ref]\n[ref]: <space file.md> "title"\n[two](a(b).md) [three](space%20file.md)\n')
        self.put("docs/space file.md")
        self.put("docs/a(b).md")
        self.assertEqual((4, []), self.check())

    def test_missing_anchor_and_unresolved_reference_fail(self):
        self.put("docs/a.md", "[bad](b.md#missing) [bad][unknown]")
        self.put("docs/b.md", "# Present")
        self.assertEqual(2, len(self.check()[1]))

    def test_heading_inside_fenced_code_is_not_an_anchor(self):
        self.put("docs/a.md", "[bad](b.md#fake) [good](b.md#real-code)")
        self.put("docs/b.md", "```md\n# Fake\n```\n# Real `code`\n")
        errors = self.check()[1]
        self.assertEqual(1, len(errors))
        self.assertIn("#fake", errors[0])

    def test_html_links_and_directory_targets(self):
        self.put("docs/a.md", '<img src="asset.png"><a href="nested/">nested</a>')
        self.put("docs/asset.png")
        self.put("docs/nested/README.md")
        self.assertEqual((2, []), self.check())

    def test_symlink_target_rejected(self):
        self.put("docs/a.md", "[link](alias.md)")
        target = self.put("docs/real.md")
        (self.root / "docs/alias.md").symlink_to(target)
        self.tracked.add("docs/alias.md")
        self.assertTrue(any("symbolic" in error for error in self.check()[1]))

    def test_deleted_tracked_target_rejected(self):
        self.put("docs/a.md", "[link](gone.txt)")
        self.tracked.add("docs/gone.txt")
        self.assertIn("missing", self.check()[1][0])


if __name__ == "__main__":
    unittest.main()
