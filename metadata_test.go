package main

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func metadataTestClient(t *testing.T, s *Server, r *Room, id uint64, game string) *Client {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	c := &Client{id: id, server: s, room: r, team: r.findOrCreateTeam("team"), conn: a, sendCh: make(chan string, 4096),
		state: `{"isSaveLoaded":true,"diptych":{"proto":1,"seed":"seed","game":"` + game + `"}}`}
	r.clients.Store(id, c)
	zero := uint64(0)
	p := metadataPacket{Type: metadataPrefix + "HELLO", Target: &zero, Nonce: "nonce"}
	metadataTestSend(c, p)
	hello := metadataTestRead(t, c)
	if hello.ClientID != 0 || hello.Epoch != s.metadataEpoch || hello.Nonce != "nonce" || hello.Cap != 1 {
		t.Fatal("untrusted capability reply", hello)
	}
	return c
}

func metadataTestSend(c *Client, p metadataPacket) {
	zero := uint64(0)
	p.Target = &zero
	if p.Epoch == "" && p.Type != metadataPrefix+"HELLO" {
		p.Epoch = c.server.metadataEpoch
	}
	if p.Scope.Proto == 0 {
		p.Scope = metadataScope{1, "seed", c.room.id, "team"}
	}
	data, _ := json.Marshal(p)
	c.handlePacket(string(data))
}
func metadataTestRead(t *testing.T, c *Client) metadataPacket {
	t.Helper()
	select {
	case raw := <-c.sendCh:
		var p metadataPacket
		if json.Unmarshal([]byte(raw), &p) != nil {
			t.Fatal(raw)
		}
		return p
	case <-time.After(time.Second):
		t.Fatal("missing metadata packet")
		return metadataPacket{}
	}
}
func metadataTestEmpty(t *testing.T, c *Client) {
	t.Helper()
	select {
	case p := <-c.sendCh:
		t.Fatal("unexpected packet", p)
	default:
	}
}
func metadataTestRoom() (*Server, *Room) {
	s := NewServer()
	r := NewRoom("room", 1, `{"roomState":{"pvpMode":1,"showLocationsMode":1,"teleportMode":1,"syncItemsAndFlags":1}}`)
	return s, r
}
func metadataTestRows(rows ...[]interface{}) [][]json.RawMessage {
	out := [][]json.RawMessage{}
	for _, row := range rows {
		raw := []json.RawMessage{}
		for _, v := range row {
			b, _ := json.Marshal(v)
			raw = append(raw, b)
		}
		out = append(out, raw)
	}
	return out
}
func metadataTestBaseline(t *testing.T, c *Client, known []bool, rows [][]json.RawMessage) {
	t.Helper()
	nom := metadataTestRead(t, c)
	if !nom.Baseline || nom.Type != metadataPrefix+"REQUEST" || nom.ClientID != 0 {
		t.Fatal("expected server nomination", nom)
	}
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Known: known, Of: 1, Checks: rows})
}

func metadataTestCap2(t *testing.T, c *Client) {
	t.Helper()
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "HELLO", Cap: 2, Nonce: "cap2"})
	p := metadataTestRead(t, c)
	if p.Cap != 2 || p.Nonce != "cap2" || c.metadataCapability() != 2 {
		t.Fatal("cap2 negotiation", p)
	}
}

func metadataTestSwitches(rows ...[]interface{}) json.RawMessage {
	b, _ := json.Marshal(metadataTestRows(rows...))
	return b
}

func metadataTestSwitchBaseline(t *testing.T, c *Client, nom metadataPacket, known bool, rows ...[]interface{}) {
	t.Helper()
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1,
		Known: []bool{true, true}, OotSwitchKnown: &known, OotSwitches: metadataTestSwitches(rows...)})
}

func TestMetadataSwitchCapabilityAndLegacySequence(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 10, "oot")
	legacy := metadataTestClient(t, s, r, 20, "oot")
	metadataTestCap2(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	nom := metadataTestRead(t, a)
	metadataTestSwitchBaseline(t, a, nom, true, []interface{}{0, 4})
	first := metadataTestRead(t, a)
	if first.OotSwitchKnown == nil || !*first.OotSwitchKnown {
		t.Fatal("missing switch authority", first)
	}
	metadataTestSend(legacy, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2})
	old := metadataTestRead(t, legacy)
	if old.OotSwitchKnown != nil || old.OotSwitches != nil || old.Seq != first.Seq {
		t.Fatal("legacy snapshot extended", old)
	}
	// Preserve repeated same-bit rows and their final value, rather than unioning SETs.
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", OotSwitches: metadataTestSwitches(
		[]interface{}{0, 2, false}, []interface{}{0, 2, true}, []interface{}{0, 2, false})})
	got := metadataTestRead(t, a)
	old = metadataTestRead(t, legacy)
	rows, ok := metadataSwitchEntries(got)
	if !ok || len(rows) != 3 || got.Seq != first.Seq+1 || old.Seq != got.Seq || old.OotSwitches != nil ||
		len(old.Checks) != 0 || len(old.Entrances) != 0 {
		t.Fatal("legacy sequence or ordered edit lost", got, old)
	}
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if ns.switches[0] != 0 || !ns.switchKnown {
		t.Fatal("UNSET lost", ns.switches)
	}
	// A capability 1 sender cannot smuggle new rows or reset the established bank.
	seq := ns.seq
	metadataTestSend(legacy, metadataPacket{Type: metadataPrefix + "EDIT", OotSwitches: metadataTestSwitches([]interface{}{0, 2, true})})
	if ns.seq != seq {
		t.Fatal("legacy peer changed switches")
	}
	metadataTestEmpty(t, a)
	metadataTestEmpty(t, legacy)
}

func TestMetadataSwitchUnknownFillPreservesEstablishedTracker(t *testing.T) {
	s, r := metadataTestRoom()
	old := metadataTestClient(t, s, r, 1, "oot")
	metadataTestSend(old, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	metadataTestBaseline(t, old, []bool{true, true}, metadataTestRows([]interface{}{0, 176, 2, true}))
	metadataTestRead(t, old)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if ns.switchKnown {
		t.Fatal("cap1 initialized switch authority")
	}
	a := metadataTestClient(t, s, r, 10, "oot")
	metadataTestCap2(t, a)
	game := 0
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2, Game: &game})
	nom := metadataTestRead(t, a)
	if !nom.Baseline {
		t.Fatal("known tracker blocked unknown switch fill", nom)
	}
	metadataTestSwitchBaseline(t, a, nom, true, []interface{}{0, 8})
	full := metadataTestRead(t, a)
	if full.Type != metadataPrefix+"STATE" || !ns.switchKnown || ns.switches[0] != 8 ||
		!ns.checks[176].skipped || *ns.checks[176].status != 2 {
		t.Fatal("fill replaced tracker cache", full)
	}
	metadataTestRead(t, a) // resync for existing members after authority changes
	metadataTestRead(t, old)
	// A stale lower-ID joiner is served the existing bank, never nominated to replace it.
	stale := metadataTestClient(t, s, r, 2, "oot")
	metadataTestCap2(t, stale)
	metadataTestSend(stale, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 3, Game: &game})
	p := metadataTestRead(t, stale)
	if p.Type != metadataPrefix+"STATE" || p.Baseline {
		t.Fatal("stale joiner nominated", p)
	}
	metadataTestSwitchBaseline(t, stale, metadataPacket{Request: 999}, true, []interface{}{0, 16})
	if ns.switches[0] != 8 {
		t.Fatal("stale baseline overwrote known switches")
	}
}

func TestMetadataSwitchCanonicalBoundsAndUnknown(t *testing.T) {
	known := true
	for _, row := range [][]interface{}{{110, 1}, {-1, 1}, {0, 0}, {0, uint64(1) << 32}, {3, 1 << 27}, {5, 1 << 29}, {26, 1 << 23}} {
		p := metadataPacket{Type: metadataPrefix + "STATE", OotSwitchKnown: &known, OotSwitches: metadataTestSwitches(row)}
		if _, ok := metadataSwitchEntries(p); ok {
			t.Fatal("invalid baseline", row)
		}
	}
	for _, row := range [][]interface{}{{110, 0, true}, {0, 32, true}, {3, 27, false}, {5, 30, true}, {26, 23, true}, {0, 2, 1}} {
		p := metadataPacket{Type: metadataPrefix + "EDIT", OotSwitches: metadataTestSwitches(row)}
		if _, ok := metadataSwitchEntries(p); ok {
			t.Fatal("invalid edit", row)
		}
	}
	unknown := false
	p := metadataPacket{Type: metadataPrefix + "STATE", OotSwitchKnown: &unknown, OotSwitches: metadataTestSwitches()}
	if _, ok := metadataSwitchEntries(p); !ok {
		t.Fatal("empty unknown rejected")
	}
	p.OotSwitches = metadataTestSwitches([]interface{}{0, 1})
	if _, ok := metadataSwitchEntries(p); ok {
		t.Fatal("unknown projection supplied authoritative row")
	}
	p.OotSwitchKnown = &known
	p.OotSwitches = metadataTestSwitches([]interface{}{0, 1}, []interface{}{0, 2})
	if _, ok := metadataSwitchEntries(p); ok {
		t.Fatal("duplicate baseline scene accepted")
	}
}

func TestMetadataSwitchBufferedOwnershipAndRoomGates(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 10, "oot")
	metadataTestCap2(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	nom := metadataTestRead(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", OotSwitches: metadataTestSwitches([]interface{}{0, 2, true})})
	a.mu.Lock()
	a.state = strings.Replace(a.state, `"oot"`, `"mm"`, 1)
	a.mu.Unlock()
	metadataTestSwitchBaseline(t, a, nom, true)
	metadataTestRead(t, a) // baseline retained from journal while parked; edit ownership rechecked
	metadataTestRead(t, a) // rejected buffered edit requests resync
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if ns.switches[0] != 0 || ns.seq != 1 {
		t.Fatal("parked buffered edit applied", ns)
	}
	a.mu.Lock()
	a.state = strings.Replace(a.state, `"mm"`, `"oot"`, 1)
	a.mu.Unlock()
	for _, scope := range []metadataScope{{1, "other", r.id, "team"}, {1, "seed", r.id, "other"}} {
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", Scope: scope, OotSwitches: metadataTestSwitches([]interface{}{0, 2, true})})
	}
	r.mu.Lock()
	r.state = `{"syncItemsAndFlags":0}`
	r.mu.Unlock()
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", OotSwitches: metadataTestSwitches([]interface{}{0, 2, true})})
	if ns.seq != 1 || ns.switches[0] != 0 {
		t.Fatal("OFF/scope gate bypassed")
	}
	metadataTestEmpty(t, a)
}

func TestMetadataSwitchSnapshotPageBudget(t *testing.T) {
	ns := &metadataNamespace{scope: metadataScope{1, "seed", "room", "team"}, known: [2]bool{true, true},
		seq: 1, switchKnown: true, checks: map[int]metadataCheck{}, entrances: map[int]bool{}, switches: map[int]uint32{}}
	for scene := 0; scene < 110; scene++ {
		ns.switches[scene] = metadataSwitchMask(scene)
	}
	for check := 1; check <= 60; check++ {
		ns.checks[check] = metadataCheck{game: 0, check: check, skipped: true}
	}
	packets := metadataSnapshotPackets(ns, 1, "epoch")
	if len(packets) != 3 {
		t.Fatal("switches not in shared page budget", len(packets))
	}
	total := 0
	for _, p := range packets {
		bits, ok := metadataSwitchEntries(p)
		if !ok {
			t.Fatal("bad generated switch page", p)
		}
		total += len(bits) + len(p.Checks) + len(p.Entrances)
	}
	if total != 170 || !metadataSnapshotFits(ns) {
		t.Fatal("snapshot budget", total)
	}
}

func TestMetadataFirstCompleteWaiterBatchAndStaleJoiner(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 10, "oot")
	b := metadataTestClient(t, s, r, 1, "mm")
	team := a.team
	team.state = `{"nativeSave":"preserved"}`
	team.queue = []string{`{"nativeFlag":3}`}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 91})
	nom := metadataTestRead(t, a)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 7})
	metadataTestEmpty(t, b)
	// A lower-ID waiting joiner cannot answer or impersonate the nominated member.
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "STATE", ClientID: a.id, Request: nom.Request, Of: 1, Known: []bool{true, true}})
	metadataTestEmpty(t, a)
	metadataTestEmpty(t, b)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Page: 1, Of: 2, Known: []bool{true, true}, Checks: metadataTestRows([]interface{}{1, 2, nil, true})})
	metadataTestEmpty(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Page: 0, Of: 2, Known: []bool{true, true}, Checks: metadataTestRows([]interface{}{0, 1, 2, false})})
	pa, pb := metadataTestRead(t, a), metadataTestRead(t, b)
	if pa.Request != 91 || pb.Request != 7 || pa.Seq != 1 || pb.Seq != 1 || len(pb.Checks) != 2 {
		t.Fatal("baseline was not one atomic waiter batch", pa, pb)
	}
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1, Known: []bool{true, true}})
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 8})
	pb = metadataTestRead(t, b)
	if len(pb.Checks) != 2 || pb.Seq != 1 {
		t.Fatal("joining baseline replaced cache", pb)
	}
	if team.state != `{"nativeSave":"preserved"}` || len(team.queue) != 1 || team.droppedFromQueue != 0 {
		t.Fatal("stock native cache touched")
	}
}

func TestMetadataKnownProjectionFillAndOrderedEdits(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 5, "oot")
	b := metadataTestClient(t, s, r, 8, "mm")
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	metadataTestBaseline(t, a, []bool{true, false}, metadataTestRows([]interface{}{0, 4, 3, true}))
	metadataTestRead(t, a)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2})
	p := metadataTestRead(t, b)
	if p.Known[1] {
		t.Fatal("parked unknown game became known")
	}
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "EDIT", Checks: metadataTestRows([]interface{}{1, 7, nil, true})})
	if !metadataTestRead(t, b).Resync {
		t.Fatal("unknown projection edit accepted")
	}
	game := 1
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 3, Game: &game})
	metadataTestBaseline(t, b, []bool{true, true}, metadataTestRows([]interface{}{0, 4, 0, false}, []interface{}{1, 7, nil, true}))
	filled := metadataTestRead(t, b)
	if filled.Seq != 2 || !filled.Known[1] || string(filled.Checks[0][2]) != "3" {
		t.Fatal("unknown fill overwrote known owner", filled)
	}
	if !metadataTestRead(t, b).Resync || !metadataTestRead(t, a).Resync {
		t.Fatal("fill did not notify existing peers")
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 4})
	snapshot := metadataTestRead(t, a)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "EDIT", ClientID: 999, Checks: metadataTestRows([]interface{}{1, 7, nil, false})})
	editA, editB := metadataTestRead(t, a), metadataTestRead(t, b)
	if snapshot.Seq != 2 || editA.Seq != 3 || editB.Seq != 3 || editA.ClientID != 0 || editA.Source != b.id {
		t.Fatal("snapshot/edit ordering or sender authority lost", snapshot, editA, editB)
	}
}

func TestMetadataMutualUnknownOffersRemainOutstanding(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 10, "oot")
	b := metadataTestClient(t, s, r, 1, "mm")
	oot, mm := 0, 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 11, Game: &oot})
	first := metadataTestRead(t, a)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 22, Game: &mm})
	metadataTestEmpty(t, b)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: first.Request, Of: 1, Known: []bool{true, false}, Checks: metadataTestRows([]interface{}{0, 1, 2, false})})
	stateA := metadataTestRead(t, a)
	second := metadataTestRead(t, b)
	if stateA.Request != 11 || stateA.Seq != 1 || !second.Baseline {
		t.Fatal("unknown owner offer prematurely completed", stateA, second)
	}
	// The later upload disagrees about OoT, but may initialize only its unknown MM projection.
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "STATE", Request: second.Request, Of: 1, Known: []bool{true, true}, Checks: metadataTestRows([]interface{}{0, 1, 0, true}, []interface{}{1, 2, nil, true})})
	stateB := metadataTestRead(t, b)
	if stateB.Request != 22 || stateB.Seq != 2 || string(stateB.Checks[0][2]) != "2" {
		t.Fatal("mutual baseline replaced known authority", stateB)
	}
	if !metadataTestRead(t, a).Resync || !metadataTestRead(t, b).Resync {
		t.Fatal("unknown fill was silent")
	}
}

func TestMetadataSharingMembershipAndReconnectFences(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 2, "oot")
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	metadataTestBaseline(t, a, []bool{true, true}, nil)
	metadataTestRead(t, a)
	r.mu.Lock()
	r.state = `{"syncItemsAndFlags":0}`
	r.mu.Unlock()
	r.cancelMetadataWork()
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", Checks: metadataTestRows([]interface{}{0, 3, 1, true})})
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2})
	metadataTestEmpty(t, a)
	// Capability negotiation is nonmutating and remains available while sharing is OFF.
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "HELLO", Nonce: "off"})
	if metadataTestRead(t, a).Nonce != "off" {
		t.Fatal("OFF HELLO failed")
	}
	r.mu.Lock()
	r.state = `{"syncItemsAndFlags":1}`
	r.mu.Unlock()
	for _, scope := range []metadataScope{{1, "wrong", r.id, "team"}, {1, "seed", r.id, "other"}, {1, "seed", "other", "team"}, {2, "seed", r.id, "team"}} {
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 3, Scope: scope})
	}
	metadataTestEmpty(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 3, Epoch: "stale"})
	metadataTestEmpty(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 4, Game: &game})
	metadataTestEmpty(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 5})
	p := metadataTestRead(t, a)
	if p.Seq != 1 || len(p.Checks) != 0 {
		t.Fatal("OFF erased cache or admitted an edit", p)
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", Checks: metadataTestRows([]interface{}{1, 2, nil, true})})
	if !metadataTestRead(t, a).Resync {
		t.Fatal("parked game edit admitted")
	}
	a.mu.Lock()
	a.metadataCapable = false
	a.mu.Unlock() // attachConnLocked resets negotiation on actual reconnect.
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 6})
	metadataTestEmpty(t, a)
}

func TestMetadataBufferedEditAndValidation(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "oot")
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	nom := metadataTestRead(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", Checks: metadataTestRows([]interface{}{0, 1, 2, true})})
	metadataTestEmpty(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1, Known: []bool{true, true}})
	state, edit := metadataTestRead(t, a), metadataTestRead(t, a)
	if state.Seq != 1 || len(state.Checks) != 0 || edit.Seq != 2 || len(edit.Checks) != 1 {
		t.Fatal("buffered edit lost or interleaved", state, edit)
	}
	bad := []metadataPacket{
		{Checks: metadataTestRows([]interface{}{1, 4, 1, false})},
		{Checks: metadataTestRows([]interface{}{0, 0, 1, false})},
		{Checks: metadataTestRows([]interface{}{0, 2, 4, false})},
		{Entrances: []int{0x821}},
		{Resync: true},
	}
	for _, p := range bad {
		p.Type = metadataPrefix + "EDIT"
		metadataTestSend(a, p)
	}
	metadataTestEmpty(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2})
	p := metadataTestRead(t, a)
	if p.Seq != 2 || len(p.Checks) != 1 {
		t.Fatal("malformed edit mutated cache", p)
	}
}

func TestMetadataIncompleteBaselineExpires(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "oot")
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	metadataTestRead(t, a)
	select {
	case raw := <-a.sendCh:
		var p metadataPacket
		json.Unmarshal([]byte(raw), &p)
		if !p.Resync {
			t.Fatal("expiry did not require resync", p)
		}
	case <-time.After(11 * time.Second):
		t.Fatal("baseline never expired")
	}
	r.metadataMu.Lock()
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	pending := ns.baseline != nil || len(ns.waiters) != 0
	r.metadataMu.Unlock()
	if pending {
		t.Fatal("expired partial baseline retained")
	}
}

func TestMetadataCombinedSnapshotByteBoundIsAtomic(t *testing.T) {
	s, r := metadataTestRoom()
	r.id = strings.Repeat("r", 18000)
	a := metadataTestClient(t, s, r, 1, "oot")
	b := metadataTestClient(t, s, r, 2, "mm")
	upload := func(c *Client, game int, request uint32) {
		metadataTestSend(c, metadataPacket{Type: metadataPrefix + "REQUEST", Request: request, Game: &game})
		nom := metadataTestRead(t, c)
		for page := 0; page < 16; page++ {
			rows := [][]json.RawMessage{}
			for n := 0; n < 64; n++ {
				var status interface{} = 0
				if game == 1 {
					status = nil
				}
				rows = append(rows, metadataTestRows([]interface{}{game, page*64 + n + 1, status, false})[0])
			}
			known := []bool{true, game == 1}
			metadataTestSend(c, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Known: known, Page: page, Of: 16, Checks: rows})
		}
	}
	upload(a, 0, 1)
	for page := 0; page < 16; page++ {
		if metadataTestRead(t, a).Seq != 1 {
			t.Fatal("initial valid snapshot rejected")
		}
	}
	upload(b, 1, 2)
	if !metadataTestRead(t, b).Resync {
		t.Fatal("oversize combined snapshot was not rejected")
	}
	r.metadataMu.Lock()
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	seq, known, count := ns.seq, ns.known, len(ns.checks)
	r.metadataMu.Unlock()
	if seq != 1 || known[1] || count != 1024 {
		t.Fatal("failed fill partially changed accepted cache", seq, known, count)
	}
}

func TestMetadataPageAndPendingEditBounds(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "oot")
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 1})
	nom := metadataTestRead(t, a)
	rows := [][]json.RawMessage{}
	for n := 1; n <= 65; n++ {
		rows = append(rows, metadataTestRows([]interface{}{0, n, 0, false})[0])
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1, Known: []bool{true, true}, Checks: rows})
	if !metadataTestRead(t, a).Resync {
		t.Fatal("oversize page admitted")
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 2})
	metadataTestRead(t, a)
	for n := 0; n < 513; n++ {
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", Checks: metadataTestRows([]interface{}{0, 1, 0, false})})
	}
	if !metadataTestRead(t, a).Resync {
		t.Fatal("pending edit overflow silently discarded")
	}
	r.metadataMu.Lock()
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	seq, pending := ns.seq, len(ns.edits)
	r.metadataMu.Unlock()
	if seq != 0 || pending != 0 {
		t.Fatal("overflow left incomplete authority", seq, pending)
	}
}

func TestMetadataNamespaceCapacityAndStockRouting(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "oot")
	b := metadataTestClient(t, s, r, 2, "oot")
	native := `{"type":"FLAG_SET","clientId":1,"targetTeamId":"team","addToQueue":true,"flag":7}`
	a.handlePacket(native)
	if metadataTestRead(t, b).Type != "FLAG_SET" || len(a.team.queue) != 1 || a.team.queue[0] != native {
		t.Fatal("native queue/routing changed")
	}
	for n := 0; n < 32; n++ {
		seed := strings.Repeat("s", n+1)
		a.mu.Lock()
		a.state = `{"isSaveLoaded":true,"diptych":{"proto":1,"seed":"` + seed + `","game":"oot"}}`
		a.mu.Unlock()
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: uint32(n + 1), Scope: metadataScope{1, seed, r.id, "team"}})
		if !metadataTestRead(t, a).Baseline {
			t.Fatal("valid namespace not nominated")
		}
	}
	a.mu.Lock()
	a.state = `{"isSaveLoaded":true,"diptych":{"proto":1,"seed":"overflow","game":"oot"}}`
	a.mu.Unlock()
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Request: 33, Scope: metadataScope{1, "overflow", r.id, "team"}})
	if !metadataTestRead(t, a).Resync {
		t.Fatal("namespace overflow silently evicted an existing seed")
	}
	r.metadataMu.Lock()
	count := len(r.metadata)
	r.metadataMu.Unlock()
	if count != 32 {
		t.Fatal("namespace cap changed", count)
	}
	if a.team.state != "{}" || len(a.team.queue) != 1 {
		t.Fatal("metadata touched stock save state")
	}
}

func TestMetadataNativeSharingValuesAndCancellation(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 2, "oot")
	scope := metadataScope{1, "seed", r.id, "team"}
	r.metadata = make(map[metadataNamespaceKey]*metadataNamespace)
	for _, test := range []struct {
		value string
		on    bool
	}{
		{"true", true}, {"1", true}, {"false", false}, {"0", false},
		{"2", false}, {"-1", false}, {"1.0", false}, {"1e0", false},
		{`"1"`, false}, {"null", false}, {"{}", false},
	} {
		t.Run(test.value, func(t *testing.T) {
			r.state = `{"syncItemsAndFlags":` + test.value + `}`
			if a.metadataMembership(scope, true) != test.on {
				t.Fatal("native sharing admission differs", test.value)
			}
			ns := &metadataNamespace{waiters: []metadataWaiter{{id: a.id, request: 1}}}
			r.metadata[metadataNamespaceKey{team: "team", seed: "seed"}] = ns
			r.cancelMetadataWork()
			if (len(ns.waiters) != 0) != test.on {
				t.Fatal("cancellation disagrees with native room admission", test.value)
			}
		})
	}
	r.state = `{}`
	if a.metadataMembership(scope, true) {
		t.Fatal("missing native sharing flag enabled metadata")
	}
}
