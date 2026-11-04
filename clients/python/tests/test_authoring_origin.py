import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mycelium import Client


class AuthoringOriginTests(unittest.TestCase):
    def test_yaml_and_extension_readers_label_submissions(self):
        client = Client(token="fixture")
        with patch.object(client, "_call", return_value={"id": "job"}) as call:
            client.submit_yaml("name: example\nproject: example\n")
        self.assertEqual(call.call_args.args[2]["authoring_origin"], "job_spec")

        with tempfile.TemporaryDirectory() as folder:
            with patch.object(client, "submit_and_wait", return_value={
                "id": "job", "state": "succeeded", "output_refs": {}
            }) as submit:
                client.run("example@1", inputs={}, outputs=Path(folder), key="stable", project="example")
        self.assertEqual(submit.call_args.args[0]["authoring_origin"], "extension")
        self.assertEqual(submit.call_args.args[0]["submission_key"], "stable")
