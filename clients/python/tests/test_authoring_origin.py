import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mycelium import APIError, Client


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

    def test_python_mappings_are_labelled_python(self):
        client = Client(token="fixture")
        with patch.object(client, "_call", return_value={"id": "job"}) as call:
            client.submit({"name": "example", "project": "example"})
            # A label copied from a Job reply's spec is replaced, not kept.
            client.submit({"name": "example", "project": "example", "authoring_origin": "unknown"})
        self.assertEqual([c.args[2]["authoring_origin"] for c in call.call_args_list], ["python", "python"])

        # A queue older than the python label refuses it before creating a job;
        # the same submission is repeated unlabelled and recorded as api there.
        older = APIError(400, '{"code":"job_execution_invalid","error":"authoring_origin must be a supported reader label"}')
        with patch.object(client, "_call", side_effect=[older, {"id": "job"}]) as call:
            self.assertEqual(client.submit({"name": "example", "project": "example"}), {"id": "job"})
        self.assertNotIn("authoring_origin", call.call_args_list[1].args[2])
