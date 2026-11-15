"""Regression checks for renamed anchors and examples that resemble links."""
import contextlib
import importlib.util
import io
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("links", pathlib.Path(__file__).with_name("check-doc-links.py"))
links = importlib.util.module_from_spec(spec)
spec.loader.exec_module(links)


class LinkTests(unittest.TestCase):
    def test_paths_anchors_and_examples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "docs").mkdir()
            (root / "docs/target.md").write_text("# A `field` (#11)\n\n## Again\n\n## Again\n<a id='old'></a>\n")
            source = root / "README.md"
            valid = ("[field](docs/target.md#a-field-11)\n[repeat](docs/target.md#again-1)\n"
                     "[legacy][old]\n\n[old]: docs/target.md#old\n"
                     "`data[metric][samples]`\n```md\n[example](missing.md)\n```\n")
            original = links.ROOT
            links.ROOT = root
            try:
                source.write_text(valid)
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(links.check(), 0)
                for broken, reason in (("[x](docs/target.md#renamed)", "missing anchor"),
                                       ("[x](gone.md)", "missing path"),
                                       ("[x][undefined]", "undefined reference")):
                    source.write_text(valid + broken)
                    errors = io.StringIO()
                    with contextlib.redirect_stderr(errors):
                        self.assertEqual(links.check(), 1)
                    self.assertIn(reason, errors.getvalue())
            finally:
                links.ROOT = original


if __name__ == "__main__":
    unittest.main()
