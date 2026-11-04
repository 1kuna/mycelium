"""Call an installed image-chat catalog entry with two local JPEG/PNG images.

Install clients/python[openai] from the reviewed repository commit first.
Usage: python image-chat.py first.jpg second.jpg --project example
"""
import argparse
import base64
import json
from pathlib import Path
import time

from mycelium import Client


def image_part(path: Path) -> dict:
    mime = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}.get(path.suffix.lower())
    if mime is None:
        raise ValueError("use a JPEG or PNG image")
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{encoded}"}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("images", type=Path, nargs=2)
    parser.add_argument("--project", required=True)
    parser.add_argument("--model", default="qwen3.8-27b-bf16")
    parser.add_argument("--prompt", type=Path, help="optional UTF-8 task/context file")
    parser.add_argument("--calls", type=int, default=1, help="use 6 for one cold and five warm calls")
    args = parser.parse_args()
    if args.calls < 1:
        parser.error("--calls must be positive")
    prompt = args.prompt.read_text() if args.prompt else (
        'Compare the two images. Return only JSON with exactly the keys "action" and "reason". '
        'Use action="observe" and a short reason describing concrete visible objects in both images.'
    )
    body = {
        "model": args.model,
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": prompt}, *map(image_part, args.images),
        ]}],
        "max_tokens": 128,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
    }
    with Client.from_installed().openai(project=args.project, priority="experiment", timeout=1200) as sdk:
        for call in range(args.calls):
            start = time.perf_counter()
            reply = sdk.chat.completions.create(**body)
            seconds = time.perf_counter() - start
            result = json.loads(reply.choices[0].message.content)
            if (set(result) != {"action", "reason"} or not isinstance(result["action"], str)
                    or not isinstance(result["reason"], str) or not result["reason"]
                    or reply.choices[0].finish_reason != "stop"):
                raise ValueError("response must finish with an action/reason JSON object")
            print(json.dumps({"call": call + 1, "wall_seconds": seconds, "result": result,
                              "usage": reply.usage.model_dump() if reply.usage else None}), flush=True)


if __name__ == "__main__":
    main()
