package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
)

const toolInventoryProtocol = "tool-inventory-v1"
const toolInventoryCapability = "tool_inventory_v1"

// Only these small fences persist. Inventory contents are immutable, private
// machine references: no transaction snapshot, commit or status copy encodes them.
type toolInventoryFence struct {
	Session string `json:"session"`
	Hash    string `json:"hash"`
	Version uint64 `json:"version"`
}
type toolInventory struct {
	hash  string
	tools map[string][]string
}

// Canonicalize sets so map insertion order, discovery order and duplicates do
// not produce different versions. Call only outside q.mu.
func prepareToolInventory(tools map[string][]string) *toolInventory {
	if tools == nil {
		return nil
	}
	copy := make(map[string][]string, len(tools))
	for key, names := range tools {
		names = slices.Clone(names)
		slices.Sort(names)
		copy[key] = slices.Compact(names)
	}
	data, _ := json.Marshal(copy) // map[string][]string cannot fail JSON encoding.
	sum := sha256.Sum256(data)
	return &toolInventory{hash: hex.EncodeToString(sum[:]), tools: copy}
}
func inventoryError(code, message string) error {
	return &HTTPError{Status: 409, Code: code, Message: message}
}
func prepareCheckinToolInventory(c Checkin) (*toolInventory, error) {
	if c.ToolInventoryHash != "" {
		decoded, err := hex.DecodeString(c.ToolInventoryHash)
		if err != nil || len(decoded) != sha256.Size || c.ToolInventoryVersion == 0 {
			return nil, inventoryError("tool_inventory_invalid", "inventory hash requires SHA-256 and a positive version")
		}
	} else if c.ToolInventoryVersion != 0 {
		return nil, inventoryError("tool_inventory_invalid", "inventory version requires a hash")
	}
	for _, names := range c.Machine.ToolInventory {
		for _, name := range names {
			if !validToolName(name) {
				return nil, inventoryError("tool_inventory_invalid", "inventory tools must be executable basenames")
			}
		}
	}
	return prepareToolInventory(c.Machine.ToolInventory), nil
}

// The session admission checks have already succeeded. Recheck the persisted
// high-water fence here, after preparation and lock acquisition. Hash-only cache
// misses keep contact/custody alive but cannot qualify any needs.tools work.
func (q *Queue) acceptToolInventory(c Checkin, m, old *Machine, prepared *toolInventory, reply *Reply) error {
	fence := q.s.ToolInventories[m.ID]
	if fence.Session == m.Session {
		if c.ToolInventoryHash == "" && fence.Version > 0 && slices.Contains(m.Capabilities, toolInventoryCapability) {
			// A restarted worker may retain its session while recovering custody,
			// but loses its in-memory counter. Negotiate above the durable fence;
			// never apply an unversioned (possibly delayed) inventory over it.
			reply.ToolInventoryRequired = true
			reply.ToolInventoryVersion = fence.Version
			return nil
		}
		if c.ToolInventoryVersion < fence.Version || c.ToolInventoryVersion == fence.Version && c.ToolInventoryHash != "" && c.ToolInventoryHash != fence.Hash {
			return inventoryError("tool_inventory_stale", "inventory report is older than the accepted worker version")
		}
	}
	if c.ToolInventoryHash == "" {
		m.toolInventory = prepared // Legacy workers replace the full snapshot each time.
	} else {
		if prepared != nil && prepared.hash == c.ToolInventoryHash {
			m.toolInventory = prepared
		} else if prepared == nil && old != nil && old.Session == m.Session && old.toolInventory != nil && old.toolInventory.hash == c.ToolInventoryHash {
			m.toolInventory = old.toolInventory
		}
		reply.ToolInventoryRequired = m.toolInventory == nil
	}
	if q.s.ToolInventories == nil {
		q.s.ToolInventories = map[string]toolInventoryFence{}
	}
	q.s.ToolInventories[m.ID] = toolInventoryFence{Session: m.Session, Hash: c.ToolInventoryHash, Version: c.ToolInventoryVersion}
	if m.toolInventory != nil {
		reply.ToolInventoryHash = m.toolInventory.hash
		reply.ToolInventoryVersion = c.ToolInventoryVersion
		// Legacy reports have no hash; persist the computed one for the next request.
		fence := q.s.ToolInventories[m.ID]
		fence.Hash = m.toolInventory.hash
		q.s.ToolInventories[m.ID] = fence
	}
	return nil
}

// A worker sends the legacy full shape until the queue advertises support.
// No extra request fields reach strict older queues. A lost reply leaves the
// prior acknowledgement unchanged, so retry resends the full snapshot safely.
type workerToolReport struct {
	machine, session string
	supported        bool
	hash             string
	version          uint64
	acknowledged     string
}

func (r *workerToolReport) checkin(c Checkin) Checkin {
	if r.machine != c.Machine.ID || r.session != c.Machine.Session {
		*r = workerToolReport{machine: c.Machine.ID, session: c.Machine.Session}
	}
	if !slices.Contains(c.Machine.Capabilities, toolInventoryCapability) {
		c.Machine.Capabilities = append(slices.Clone(c.Machine.Capabilities), toolInventoryCapability)
	}
	if len(c.Machine.ToolInventory) == 0 {
		c.Machine.ToolInventory = map[string][]string{"": nil}
	}
	inventory := prepareToolInventory(c.Machine.ToolInventory)
	hash := ""
	if inventory != nil {
		hash = inventory.hash
	}
	if hash != r.hash {
		r.hash, r.version = hash, r.version+1
	}
	if r.supported && hash != "" {
		c.ToolInventoryHash, c.ToolInventoryVersion = hash, r.version
		if r.acknowledged == hash {
			c.Machine.ToolInventory = nil
		}
	}
	return c
}
func (r *workerToolReport) accept(c Checkin, reply Reply) {
	r.supported = reply.ToolInventoryProtocol == toolInventoryProtocol
	r.acknowledged = ""
	if r.supported && c.ToolInventoryHash == "" && reply.ToolInventoryRequired {
		r.version = max(r.version, reply.ToolInventoryVersion) + 1
	}
	if r.supported && !reply.ToolInventoryRequired && reply.ToolInventoryHash == r.hash && reply.ToolInventoryVersion == c.ToolInventoryVersion {
		r.acknowledged = r.hash
	}
}
