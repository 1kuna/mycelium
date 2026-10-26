"""Tiny native PyTorch forward/backward proof for a project profile."""

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform", choices=("cuda", "xpu"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = Path("accelerator-result.json")
    if args.check:
        proof = json.loads(result.read_text())
        assert proof["platform"] == args.platform
        assert proof["device_type"] == args.platform
        assert proof["sum"] == 2048 and proof["gradient_sum"] == 2048
        assert proof["ok"] is True and proof["torch_version"] and proof["device_name"]
        return

    import torch

    accelerator = getattr(torch, args.platform)
    assert accelerator.is_available(), f"{args.platform} accelerator unavailable"
    device = torch.device(args.platform)
    x = torch.ones((32, 32), device=device, requires_grad=True)
    y = x @ (2 * torch.eye(32, device=device))
    y.sum().backward()
    accelerator.synchronize()
    torch.testing.assert_close(y, torch.full_like(y, 2))
    torch.testing.assert_close(x.grad, torch.full_like(x, 2))
    proof = {
        "platform": args.platform,
        "device_type": y.device.type,
        "device_name": accelerator.get_device_name(),
        "torch_version": str(torch.__version__),
        "sum": y.sum().item(),
        "gradient_sum": x.grad.sum().item(),
        "ok": True,
    }
    result.write_text(json.dumps(proof, sort_keys=True) + "\n")
    print(json.dumps(proof, sort_keys=True))


if __name__ == "__main__":
    main()
