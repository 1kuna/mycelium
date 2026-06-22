import importlib.util
import pathlib
import sys
import types
import unittest


class FakeGenerationConfig:
    max_new_tokens = 0
    temperature = 0
    top_p = 0
    top_k = 0


class FakeVLMPipeline:
    def __init__(self, model, device):
        self.model = model
        self.device = device

    def get_generation_config(self):
        return FakeGenerationConfig()

    def generate(self, *args, **kwargs):
        return "ok"


def load_wrapper():
    sys.modules["openvino"] = types.SimpleNamespace(Tensor=lambda value: value)
    sys.modules["openvino_genai"] = types.SimpleNamespace(VLMPipeline=FakeVLMPipeline)
    path = pathlib.Path(__file__).with_name("openvino-genai-openai.py")
    spec = importlib.util.spec_from_file_location("openvino_genai_openai_wrapper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class OpenVINOChatWrapperTest(unittest.TestCase):
    def test_chat_reports_busy_instead_of_waiting_behind_generation_lock(self):
        wrapper = load_wrapper()
        args = types.SimpleNamespace(
            model="/models/gemma",
            device="GPU",
            served_model_name="google/gemma-4-31B",
            default_max_tokens=8,
        )
        server = wrapper.OpenVINOChatServer(args)
        self.assertTrue(server.lock.acquire(blocking=False))
        try:
            with self.assertRaisesRegex(wrapper.BackendBusyError, "generation is already in progress"):
                server.chat({
                    "model": "google/gemma-4-31B",
                    "messages": [{"role": "user", "content": "Return ok"}],
                    "max_tokens": 1,
                })
        finally:
            server.lock.release()


if __name__ == "__main__":
    unittest.main()
