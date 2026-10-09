#!/usr/bin/env python3
"""Unit tests for release metadata guards; no GitHub access or credentials."""
from importlib.machinery import SourceFileLoader
from pathlib import Path
import tempfile
import textwrap
import unittest

checker = SourceFileLoader("release_checker", str(Path(__file__).with_name("check-release.py"))).load_module()


def notes(dated="TBD", schema="- Initial schema", migration="- No migration"):
    return textwrap.dedent(f"""\
    # Changelog

    ## [Unreleased]

    ### Added
    - Pending

    ## [0.1.0] - {dated}

    ### MCP tool-schema changes
    {schema}

    ### Index/state migrations
    {migration}
    """)


class ReleaseMetadata(unittest.TestCase):
    def test_draft_is_valid_without_tag(self):
        self.assertEqual(checker.check("0.1.0", notes()), [])

    def test_tag_must_match_manifest_and_be_dated(self):
        self.assertTrue(checker.check("0.1.0", notes(), "v0.1.0"))
        self.assertEqual(checker.check("0.1.0", notes("2026-10-09"), "v0.1.0"), [])
        self.assertTrue(checker.check("0.1.0", notes("2026-10-09"), "v0.2.0"))
        self.assertTrue(checker.check("0.1.0", notes("2026-02-30"), "v0.1.0"))

    def test_requires_schema_and_migration_disclosures(self):
        self.assertTrue(checker.check("0.1.0", notes(schema="Not recorded")))
        self.assertTrue(checker.check("0.1.0", notes(migration="None")))
        self.assertTrue(checker.check("0.1.0", notes().replace("### MCP tool-schema changes", "### Other")))
        self.assertTrue(checker.check("0.1.0", notes().replace("### Index/state migrations", "### Other")))

    def test_wrong_or_duplicate_version_rejected(self):
        self.assertTrue(checker.check("0.2.0", notes()))
        self.assertTrue(checker.check("0.1.0", notes() + "\n## [0.1.0] - TBD\n"))
        self.assertTrue(checker.check("0.1", notes()))

    def test_release_section_cannot_borrow_other_versions_bullets(self):
        changed = notes().replace("## [0.1.0] - TBD", "## [0.2.0] - TBD") + """
## [0.1.0] - TBD

### MCP tool-schema changes

### Index/state migrations
"""
        self.assertTrue(checker.check("0.1.0", changed))

    def test_unreleased_section_is_mandatory(self):
        self.assertTrue(checker.check("0.1.0", notes().replace("## [Unreleased]", "## Work")))


if __name__ == "__main__":
    unittest.main()
