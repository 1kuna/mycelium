package myce

// Online native execution is an explicitly advertised, qualified capability.
// Missing reports (including old workers) never authorize relaxing isolation.
func nativeNetworkFits(j *Job, m *Machine) bool {
	if j.Spec.Run.Docker {
		return true
	}
	if j.Spec.Needs.Network && !contains(m.Capabilities, "native_network") {
		return false
	}
	return true
}

func nativeNetworkRequired(j *Job) bool {
	if j.Spec.Run.Docker {
		return false
	}
	if j.Spec.Needs.Network {
		return true
	}
	for _, e := range j.Extensions {
		for _, v := range e.Variants {
			if v.Needs.Network {
				return true
			}
		}
	}
	return false
}
