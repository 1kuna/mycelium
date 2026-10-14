"""Pinned source, build tools and all downloads live outside the small folder."""
import hashlib
import json
import os
from urllib.request import urlopen
from pathlib import Path
import subprocess

cache = Path(os.environ["MYCE_EXTENSION_CACHE"])
source = cache / "source"
platform = os.environ["MYCE_PLATFORM"]
backend = os.environ.get("MYCE_LLAMA_BACKEND", {"metal": "metal", "cuda": "cuda", "xpu": "sycl"}[platform])
if backend not in {"metal": ["metal"], "cuda": ["cuda", "vulkan"], "xpu": ["sycl", "vulkan"]}[platform]:
    raise SystemExit("build backend does not match assigned resource")
choice = cache / "backend"
if choice.exists() and choice.read_text().strip() != backend:
    raise SystemExit("backend is frozen in this cache; use a new extension version")
if not source.exists():
    subprocess.run(["git", "clone", "--depth", "1", "--branch", "b6800",
                    "https://github.com/ggml-org/llama.cpp.git", str(source)], check=True)
commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
if commit != "0398752dd450dfabdd1b9e289f6364c2600f6ab5":
    raise SystemExit("llama.cpp source identity differs from frozen release")
if not (cache / "build/bin/llama-cli").exists():
    options = ["-DGGML_METAL=OFF", "-DGGML_CUDA=OFF", "-DGGML_SYCL=OFF", "-DGGML_VULKAN=OFF"]
    options += ["-DGGML_" + backend.upper() + "=ON", "-DLLAMA_CURL=OFF"]
    if backend == "sycl":
        options += ["-DCMAKE_C_COMPILER=icx", "-DCMAKE_CXX_COMPILER=icpx"]
    subprocess.run(["cmake", "-S", str(source), "-B", str(cache / "build"), *options], check=True)
    subprocess.run(["cmake", "--build", str(cache / "build"), "--config", "Release",
                    "--target", "llama-cli", "-j", "2"], check=True)
choice.write_text(backend + "\n")

# Cache the matching tiny native-reference model once, never bundle its bytes.
reference = json.loads((Path(os.environ["MYCE_EXTENSION_DIR"]) / "baseline/input.json").read_text())
model = cache / reference["model"]
if not model.exists():
    partial = model.with_suffix(".partial")
    with urlopen(reference["url"], timeout=60) as response, partial.open("wb") as output:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
    if hashlib.sha256(partial.read_bytes()).hexdigest() != reference["sha256"]:
        raise SystemExit("native-reference model hash mismatch")
    partial.rename(model)
if hashlib.sha256(model.read_bytes()).hexdigest() != reference["sha256"]:
    raise SystemExit("cached native-reference model hash mismatch")
