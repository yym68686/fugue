import json
import unittest

from scripts.generate_postgres_image_catalog import render


class CatalogGenerationTests(unittest.TestCase):
    def test_receipt_is_preserved_and_cannot_inject_source(self):
        raw = json.dumps({"image": 'registry.invalid/pg:one`\n"; var unsafe = true'})
        generated = render(raw)
        literal = generated.split("var approvedRelease = []byte(", 1)[1].removesuffix(")\n")
        self.assertEqual(json.loads(literal), raw)
        self.assertEqual(len(generated.splitlines()), 5)

    def test_invalid_receipt_is_rejected(self):
        with self.assertRaises(json.JSONDecodeError):
            render("not JSON")


if __name__ == "__main__":
    unittest.main()
