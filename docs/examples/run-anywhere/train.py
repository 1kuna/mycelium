"""Train a small CNN on scikit-learn's bundled handwritten digits (8x8, 1,797 images).

Runs unchanged on CUDA, Intel Arc (XPU), Apple Metal (MPS) and CPU. The device
comes from MYCE_DEVICE, which Mycelium sets to the card it reserved.
"""
import json
import os
import platform
import time

import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split

# --- the device: the only Mycelium-specific lines ---------------------------
# Under Mycelium, MYCE_DEVICE is always set ("cuda:0", "xpu:0", "mps" or "cpu").
# A torch build that lacks that backend raises below; it never drops to CPU.
# The fallback after `or` applies only when running outside Mycelium.
acc = torch.accelerator.current_accelerator() if torch.accelerator.is_available() else None
device = torch.device(os.environ.get("MYCE_DEVICE") or acc or "cpu")
torch.empty(1, device=device)  # fail now, loudly, if this build cannot use the device
# -----------------------------------------------------------------------------

torch.manual_seed(0)
digits = load_digits()
# scikit-learn returns float64; Metal has no float64. The pixel values are
# integers 0..16, so float32 holds them exactly (no lossy conversion).
x = torch.tensor(digits.images, dtype=torch.float32).unsqueeze(1) / 16.0
y = torch.tensor(digits.target, dtype=torch.long)
idx_train, idx_test = train_test_split(range(len(y)), test_size=0.25, random_state=0, stratify=digits.target)
x_train, y_train = x[idx_train].to(device), y[idx_train].to(device)
x_test, y_test = x[idx_test].to(device), y[idx_test].to(device)


class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, padding=1)
        self.conv2 = nn.Conv2d(32, 64, 3, padding=1)
        self.fc1 = nn.Linear(64 * 4 * 4, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, t):
        t = F.relu(self.conv1(t))
        t = F.max_pool2d(F.relu(self.conv2(t)), 2)
        t = F.relu(self.fc1(t.flatten(1)))
        return self.fc2(t)


model = Net().to(device)
opt = torch.optim.AdamW(model.parameters(), lr=2e-3)
epochs, batch = 30, 64
gen = torch.Generator().manual_seed(0)

start = time.perf_counter()
for epoch in range(epochs):
    model.train()
    perm = torch.randperm(len(x_train), generator=gen).to(device)
    total = 0.0
    for i in range(0, len(perm), batch):
        b = perm[i : i + batch]
        loss = F.cross_entropy(model(x_train[b]), y_train[b])
        opt.zero_grad()
        loss.backward()
        opt.step()
        total += loss.item() * len(b)
    if epoch in (0, epochs - 1):
        print(f"epoch {epoch + 1}: train loss {total / len(perm):.4f}", flush=True)
if device.type != "cpu":
    torch.accelerator.synchronize()
seconds = time.perf_counter() - start

model.eval()
with torch.no_grad():
    accuracy = (model(x_test).argmax(1) == y_test).float().mean().item()

metrics = {
    "device": str(device),
    "device_name": (
        torch.cuda.get_device_name(device) if device.type == "cuda"
        else torch.xpu.get_device_name(device) if device.type == "xpu"
        else device.type
    ),
    "torch": torch.__version__,
    "os_arch": f"{platform.system().lower()}/{platform.machine().lower()}",
    "final_train_loss": round(total / len(perm), 6),
    "test_accuracy": round(accuracy, 4),
    "train_seconds": round(seconds, 2),
    "peak_accelerator_mib": (
        round(torch.accelerator.max_memory_allocated() / 2**20, 1) if device.type != "cpu" else None
    ),
}
print(json.dumps(metrics), flush=True)
with open("metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
