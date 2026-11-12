package myce

import (
	"bytes"
	"errors"
	"slices"
	"strconv"
)

// A settled job record repeats whole values: each attempt's storage and memory
// envelope copies the job's, attempt history and the ledger copy the placement
// decision, the ledger repeats the timing key, and timing repeats the placement
// reason. The stored form keeps every repeated value once in a leading "$refs"
// table and writes {"$ref":N} where it occurred. Expansion restores the exact
// original bytes, so every reader decodes the same values. This is a SQLite
// form only; replies, status and the wire keep the ordinary job JSON.
//
// MY-42: every release since r6.11 expands this form, so with r6.12 as the
// previous release the writer is on. Open converts retained records once.
const slimJobRecordsDefault = true

var (
	jobRecordRefsPrefix = []byte(`{"$refs":[`)
	jobRecordRefToken   = []byte(`{"$ref":`)
	jobRecordRefMarker  = []byte(`"$ref`)
)

// A reference costs at most jobRecordRefBytes; a value is shared only when its
// removed copies outweigh the references and its table entry.
const (
	minSharedJobValueBytes = 16
	jobRecordRefBytes      = 12
)

type jsonValueSpan struct {
	start, end, last int // last: index of the final descendant in pre-order.
	candidate        bool
}

// spanJSON indexes compact JSON as produced by encoding/json. Object keys are
// never candidates: only values can be replaced by a reference.
func spanJSON(data []byte) ([]jsonValueSpan, bool) {
	spans := make([]jsonValueSpan, 0, len(data)/24)
	var value func(i int) (int, bool)
	value = func(i int) (int, bool) {
		if i >= len(data) {
			return 0, false
		}
		index := len(spans)
		spans = append(spans, jsonValueSpan{start: i})
		end := 0
		switch data[i] {
		case '{', '[':
			closing, object := byte('}'), data[i] == '{'
			if !object {
				closing = ']'
			}
			i++
			if i < len(data) && data[i] == closing {
				end = i + 1
				break
			}
			for {
				if object {
					k, ok := skipJSONString(data, i)
					if !ok || k >= len(data) || data[k] != ':' {
						return 0, false
					}
					i = k + 1
				}
				var ok bool
				if i, ok = value(i); !ok || i >= len(data) {
					return 0, false
				}
				if data[i] == closing {
					end = i + 1
					break
				}
				if data[i] != ',' {
					return 0, false
				}
				i++
			}
		case '"':
			var ok bool
			if end, ok = skipJSONString(data, i); !ok {
				return 0, false
			}
		default:
			end = i
			for end < len(data) && data[end] != ',' && data[end] != '}' && data[end] != ']' {
				end++
			}
			if end == i {
				return 0, false
			}
		}
		spans[index].end = end
		spans[index].last = len(spans) - 1
		c := data[spans[index].start]
		spans[index].candidate = index > 0 && (c == '{' || c == '[' || c == '"') && end-spans[index].start >= minSharedJobValueBytes
		return end, true
	}
	end, ok := value(0)
	return spans, ok && end == len(data) && len(data) > 0 && data[0] == '{'
}

func skipJSONString(data []byte, i int) (int, bool) {
	if i >= len(data) || data[i] != '"' {
		return 0, false
	}
	for i++; i < len(data); i++ {
		switch data[i] {
		case '\\':
			i++
		case '"':
			return i + 1, true
		}
	}
	return 0, false
}

// slimJobRecord returns data unchanged when nothing repeats or the record
// already contains the reference marker, so expansion is never ambiguous.
func slimJobRecord(data []byte) []byte {
	if bytes.Contains(data, jobRecordRefMarker) {
		return data
	}
	spans, ok := spanJSON(data)
	if !ok {
		return data
	}
	group := make([]int, len(spans))
	ids := map[string]int{}
	var count, representative []int
	for i, s := range spans {
		group[i] = -1
		if !s.candidate {
			continue
		}
		id, seen := ids[string(data[s.start:s.end])]
		if !seen {
			id = len(count)
			ids[string(data[s.start:s.end])] = id
			count, representative = append(count, 0), append(representative, i)
		}
		group[i] = id
		count[id]++
	}
	// Largest first: once a value is shared, the copies nested inside its other
	// occurrences disappear with them and no longer count as repeats.
	order := []int{}
	for id, n := range count {
		if n > 1 {
			order = append(order, id)
		}
	}
	slices.SortStableFunc(order, func(a, b int) int {
		x, y := spans[representative[a]], spans[representative[b]]
		return (y.end - y.start) - (x.end - x.start)
	})
	shared := make([]bool, len(count))
	repeated := false
	for _, id := range order {
		n := count[id]
		size := spans[representative[id]].end - spans[representative[id]].start
		if n < 2 || (n-1)*size <= n*jobRecordRefBytes+1 {
			continue
		}
		shared[id], repeated = true, true
		r := representative[id]
		for d := r + 1; d <= spans[r].last; d++ {
			if group[d] >= 0 {
				count[group[d]] -= n - 1
			}
		}
	}
	if !repeated {
		return data
	}
	entries := [][]byte{}
	entry := make([]int, len(count))
	for i := range entry {
		entry[i] = -1
	}
	var emit func(out []byte, node int) []byte
	ref := func(out []byte, node int) []byte {
		id := group[node]
		if entry[id] < 0 {
			entry[id] = len(entries)
			entries = append(entries, nil)
			entries[entry[id]] = emit(nil, node)
		}
		out = append(out, jobRecordRefToken...)
		out = strconv.AppendInt(out, int64(entry[id]), 10)
		return append(out, '}')
	}
	emit = func(out []byte, node int) []byte {
		s := spans[node]
		position := s.start
		for d := node + 1; d <= s.last; d++ {
			if spans[d].start < position || group[d] < 0 || !shared[group[d]] {
				continue
			}
			out = append(out, data[position:spans[d].start]...)
			out = ref(out, d)
			position = spans[d].end
		}
		return append(out, data[position:s.end]...)
	}
	body := emit(nil, 0)
	out := make([]byte, 0, len(body)+len(entries)*minSharedJobValueBytes)
	out = append(out, jobRecordRefsPrefix...)
	for i, e := range entries {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, e...)
	}
	out = append(out, ']')
	if len(body) > 2 {
		out = append(out, ',')
	}
	out = append(out, body[1:]...)
	if len(out) >= len(data) {
		return data
	}
	return out
}

var errJobRecordRefs = errors.New("invalid shared job record reference")

// expandJobRecord returns ordinary job JSON. Records without the table are
// returned as the same slice, so ordinary rows pay only a prefix comparison.
func expandJobRecord(data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, jobRecordRefsPrefix) {
		return data, nil
	}
	var table [][]byte
	i := len(jobRecordRefsPrefix)
	if i < len(data) && data[i] == ']' {
		i++
	} else {
		for {
			end, ok := skipJSONValue(data, i)
			if !ok || end >= len(data) {
				return nil, errJobRecordRefs
			}
			table = append(table, data[i:end])
			i = end + 1
			if data[end] == ']' {
				break
			}
			if data[end] != ',' {
				return nil, errJobRecordRefs
			}
		}
	}
	if i < len(data) && data[i] == ',' {
		i++
	}
	expanded := make([][]byte, len(table))
	state := make([]byte, len(table)) // 0 unseen, 1 expanding, 2 done
	var expand func(out, segment []byte) ([]byte, error)
	resolve := func(index int) ([]byte, error) {
		if index < 0 || index >= len(table) || state[index] == 1 {
			return nil, errJobRecordRefs
		}
		if state[index] == 0 {
			state[index] = 1
			value, err := expand(nil, table[index])
			if err != nil {
				return nil, err
			}
			expanded[index], state[index] = value, 2
		}
		return expanded[index], nil
	}
	expand = func(out, segment []byte) ([]byte, error) {
		position := 0
		for j := 0; j < len(segment); j++ {
			switch segment[j] {
			case '"':
				end, ok := skipJSONString(segment, j)
				if !ok {
					return nil, errJobRecordRefs
				}
				j = end - 1
			case '{':
				if !bytes.HasPrefix(segment[j:], jobRecordRefToken) {
					continue
				}
				k := j + len(jobRecordRefToken)
				digits := k
				for k < len(segment) && segment[k] >= '0' && segment[k] <= '9' {
					k++
				}
				if k == digits || k >= len(segment) || segment[k] != '}' {
					return nil, errJobRecordRefs
				}
				index, err := strconv.Atoi(string(segment[digits:k]))
				if err != nil {
					return nil, errJobRecordRefs
				}
				value, err := resolve(index)
				if err != nil {
					return nil, err
				}
				out = append(append(out, segment[position:j]...), value...)
				position, j = k+1, k
			}
		}
		return append(out, segment[position:]...), nil
	}
	out := make([]byte, 0, 2*len(data))
	out = append(out, '{')
	return expand(out, data[i:])
}

func skipJSONValue(data []byte, i int) (int, bool) {
	depth := 0
	for ; i < len(data); i++ {
		switch data[i] {
		case '"':
			end, ok := skipJSONString(data, i)
			if !ok {
				return 0, false
			}
			i = end - 1
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return i, true
			}
			depth--
		case ',':
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// convertJobRecords rewrites settled rows still in the ordinary form, once,
// in the opening transaction. Live rows convert when they settle; rows with
// nothing repeated stay ordinary.
func (q *Queue) convertJobRecords() {
	if !q.slimJobRecords {
		return
	}
	for id, j := range q.s.Jobs {
		data := q.persisted["jobs/"+id]
		if len(data) > 0 && !bytes.HasPrefix(data, jobRecordRefsPrefix) && !mutableJob(j) && len(slimJobRecord(data)) < len(data) {
			q.touchJob(id)
		}
	}
}
