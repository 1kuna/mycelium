"""Run as the main command of the warm pieces example in docs/jobs.md."""

import json
from pathlib import Path

from mycelium.warm_pieces import WarmPieces


def main():
    with WarmPieces() as pieces:
        weights = json.loads(Path("model.json").read_text())
        while item := pieces.next():
            try:
                value = json.loads(item.inputs["input.json"].read_text())["value"]
                score = weights["multiplier"] * value
            except (ValueError, KeyError, TypeError) as error:
                print(f"{item.key}: {type(error).__name__}", flush=True)
                pieces.failure()
                continue
            (item.output_dir / "score.json").write_text(json.dumps({"score": score}))
            pieces.success()


if __name__ == "__main__":
    main()
