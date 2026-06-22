#!/usr/bin/env python3
import argparse
import base64
import json
import sys
import threading
import time
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO

import numpy as np
import openvino as ov
import openvino_genai as ov_genai
from PIL import Image


class BackendBusyError(RuntimeError):
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="OpenAI-compatible chat wrapper for OpenVINO GenAI VLM models")
    parser.add_argument("--model", required=True, help="Local OpenVINO GenAI model directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--device", default="CPU")
    parser.add_argument("--served-model-name", default="")
    parser.add_argument("--default-max-tokens", type=int, default=256)
    parser.add_argument("--max-body-bytes", type=int, default=128 * 1024 * 1024)
    return parser.parse_args()


def image_tensor_from_data_uri(uri):
    if not uri.startswith("data:image/"):
        raise ValueError("only data:image/...;base64 image_url values are supported")
    try:
        _, payload = uri.split(",", 1)
    except ValueError as exc:
        raise ValueError("image_url data URI is missing base64 payload") from exc
    raw = base64.b64decode(payload, validate=True)
    image = Image.open(BytesIO(raw)).convert("RGB")
    return ov.Tensor(np.array(image)[None])


def extract_prompt_and_images(messages):
    prompt_parts = []
    images = []
    for message in messages:
        role = message.get("role", "user")
        content = message.get("content", "")
        if isinstance(content, str):
            if content:
                prompt_parts.append(f"{role}: {content}")
            continue
        if not isinstance(content, list):
            raise ValueError("message content must be a string or content-part array")
        text_parts = []
        for part in content:
            kind = part.get("type")
            if kind == "text":
                text = part.get("text", "")
                if text:
                    text_parts.append(text)
            elif kind == "image_url":
                image_url = part.get("image_url", {})
                if not isinstance(image_url, dict):
                    raise ValueError("image_url content part must contain an object")
                images.append(image_tensor_from_data_uri(image_url.get("url", "")))
            else:
                raise ValueError(f"unsupported content part type {kind!r}")
        if text_parts:
            prompt_parts.append(f"{role}: {' '.join(text_parts)}")
    prompt = "\n".join(prompt_parts).strip()
    if not prompt:
        raise ValueError("request did not contain text content")
    return prompt, images


def generated_text(result):
    texts = getattr(result, "texts", None)
    if texts:
        return texts[0]
    if isinstance(result, str):
        return result
    return str(result)


class OpenVINOChatServer:
    def __init__(self, args):
        self.args = args
        self.served_model = args.served_model_name or args.model
        start = time.time()
        self.pipe = ov_genai.VLMPipeline(args.model, args.device)
        self.load_seconds = time.time() - start
        self.lock = threading.Lock()

    def generation_config(self, request):
        config = self.pipe.get_generation_config()
        max_tokens = request.get("max_completion_tokens", request.get("max_tokens", self.args.default_max_tokens))
        config.max_new_tokens = int(max_tokens)
        if "temperature" in request:
            config.temperature = float(request["temperature"])
        if "top_p" in request:
            config.top_p = float(request["top_p"])
        if "top_k" in request:
            config.top_k = int(request["top_k"])
        return config

    def chat(self, request):
        if request.get("stream"):
            raise ValueError("streaming is not supported by the OpenVINO GenAI wrapper")
        requested_model = request.get("model", "")
        if requested_model and requested_model != self.served_model:
            raise ValueError(f"model {requested_model!r} does not match served model {self.served_model!r}")
        prompt, images = extract_prompt_and_images(request.get("messages", []))
        config = self.generation_config(request)
        if not self.lock.acquire(blocking=False):
            raise BackendBusyError("openvino generation is already in progress")
        try:
            if images:
                result = self.pipe.generate(prompt, images=images, generation_config=config)
            else:
                result = self.pipe.generate(prompt, generation_config=config)
        finally:
            self.lock.release()
        text = generated_text(result)
        now = int(time.time())
        return {
            "id": f"chatcmpl-openvino-{now}",
            "object": "chat.completion",
            "created": now,
            "model": self.served_model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }


def make_handler(server):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

        def send_json(self, status, payload):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self.send_json(200, {
                    "status": "ok",
                    "model": server.served_model,
                    "device": server.args.device,
                    "load_seconds": round(server.load_seconds, 3),
                    "busy": server.lock.locked(),
                })
                return
            if self.path == "/v1/models":
                self.send_json(200, {"object": "list", "data": [{"id": server.served_model, "object": "model"}]})
                return
            self.send_json(404, {"error": {"message": "not found"}})

        def do_POST(self):
            if self.path != "/v1/chat/completions":
                self.send_json(404, {"error": {"message": "not found"}})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    raise ValueError("request body is required")
                if length > server.args.max_body_bytes:
                    raise ValueError("request body exceeds max-body-bytes")
                request = json.loads(self.rfile.read(length))
                self.send_json(200, server.chat(request))
            except BackendBusyError as exc:
                self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "1")
                payload = {
                    "error": {
                        "message": str(exc),
                        "type": "backend_busy",
                        "code": "openvino_generation_busy",
                    }
                }
                body = json.dumps(payload).encode("utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                traceback.print_exc()
                self.send_json(400, {"error": {"message": str(exc), "type": "invalid_request_error"}})

    return Handler


def main():
    args = parse_args()
    server = OpenVINOChatServer(args)
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(server))
    print(json.dumps({
        "event": "ready",
        "model": server.served_model,
        "device": args.device,
        "host": args.host,
        "port": args.port,
        "load_seconds": round(server.load_seconds, 3),
    }), flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
