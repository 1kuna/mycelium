#!/usr/bin/env bash
set -euo pipefail

image="${MYCELIUM_VLLM_DOCKER_IMAGE:-nvcr.io/nvidia/vllm:26.04-py3}"
cache_dir="${MYCELIUM_HF_CACHE:-${HOME}/.cache/huggingface}"
docker_memory="${MYCELIUM_VLLM_DOCKER_MEMORY:-}"
docker_memory_swap="${MYCELIUM_VLLM_DOCKER_MEMORY_SWAP:-${docker_memory}}"
port=""
previous=""
for arg in "$@"; do
	if [[ "${previous}" == "--port" ]]; then
		port="${arg}"
		break
	fi
	previous="${arg}"
done
container_name="${MYCELIUM_VLLM_CONTAINER_NAME:-myc-vllm-${port:-$$}}"

cleanup() {
	docker rm -f "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup INT TERM EXIT

cleanup
docker_args=(
	--rm
	--name "${container_name}"
	--gpus all
	--network host
	--ipc=host
	--ulimit memlock=-1
	--ulimit stack=67108864
	-v "${cache_dir}:/root/.cache/huggingface"
	-e VLLM_WORKER_MULTIPROC_METHOD=spawn
)
if [[ -n "${docker_memory}" ]]; then
	docker_args+=(--memory "${docker_memory}")
fi
if [[ -n "${docker_memory_swap}" ]]; then
	docker_args+=(--memory-swap "${docker_memory_swap}")
fi
if [[ -n "${HF_TOKEN:-}" ]]; then
	docker_args+=(-e HF_TOKEN)
fi
if [[ -n "${HUGGING_FACE_HUB_TOKEN:-}" ]]; then
	docker_args+=(-e HUGGING_FACE_HUB_TOKEN)
fi

docker run "${docker_args[@]}" "${image}" vllm "$@" &
child=$!
wait "${child}"
