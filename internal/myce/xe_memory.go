package myce

import (
	"encoding/binary"
	"errors"
)

// DRM_XE_DEVICE_QUERY_MEM_REGIONS reply layout (include/uapi/drm/xe_drm.h):
// drm_xe_query_mem_regions is a count and pad, then 88-byte drm_xe_mem_region
// entries. The placement mask addresses at most 32 regions.
const (
	xeMemRegionSize    = 88
	xeMaxMemRegions    = 32
	xeMemRegionClassVR = 1 // DRM_XE_MEM_REGION_CLASS_VRAM; never system/GTT.
)

// The same validation as the reference broker (deploy/gpu-telemetry).
func xeVRAMRegions(raw []byte) (total, used uint64, err error) {
	if len(raw) < 8 {
		return 0, 0, errors.New("incomplete Xe memory regions")
	}
	count, pad := binary.NativeEndian.Uint32(raw), binary.NativeEndian.Uint32(raw[4:])
	if pad != 0 || count == 0 || count > xeMaxMemRegions || len(raw) != 8+int(count)*xeMemRegionSize {
		return 0, 0, errors.New("invalid Xe memory regions")
	}
	seen := map[uint16]bool{}
	for offset := 8; offset < len(raw); offset += xeMemRegionSize {
		r := raw[offset:]
		class, instance := binary.NativeEndian.Uint16(r), binary.NativeEndian.Uint16(r[2:])
		size, usage := binary.NativeEndian.Uint64(r[8:]), binary.NativeEndian.Uint64(r[16:])
		if instance >= xeMaxMemRegions || seen[instance] || usage > size {
			return 0, 0, errors.New("invalid Xe memory region")
		}
		seen[instance] = true
		if class == xeMemRegionClassVR {
			if total+size < total {
				return 0, 0, errors.New("invalid Xe memory region")
			}
			total, used = total+size, used+usage
		}
	}
	if total == 0 {
		return 0, 0, errors.New("Xe VRAM unavailable")
	}
	return total, used, nil
}

// Older kernels report zero usage to callers without CAP_PERFMON or
// CAP_SYS_ADMIN, and the worker holds neither. A zero therefore stays unknown,
// never free VRAM. Positive usage proves the counter is uncensored; Linux 7.0
// reports it to the unprivileged worker (#2185).
func xeFreeVRAMGB(raw []byte) (float64, error) {
	total, used, err := xeVRAMRegions(raw)
	if err != nil {
		return 0, err
	}
	if used == 0 {
		return 0, errors.New("zero Xe VRAM usage may be censored")
	}
	return float64(total-used) / (1 << 30), nil
}
