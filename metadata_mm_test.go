package main

import "testing"

func metadataTestCap3(t *testing.T, c *Client) {
	t.Helper()
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "HELLO", Cap: 3, Nonce: "cap3"})
	if p := metadataTestRead(t, c); p.Cap != 3 || p.Nonce != "cap3" {
		t.Fatal("cap3 not server acknowledged", p)
	}
}

func metadataTestMmBaseline(t *testing.T, c *Client, nom metadataPacket, rows ...[]interface{}) {
	t.Helper()
	yes, no := true, false
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1,
		Known: []bool{false, false}, OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(),
		MmSwitchKnown: &yes, MmSwitches: metadataTestSwitches(rows...)})
}

func TestMetadataMmOrderedRetentionAndLegacy(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	old := metadataTestClient(t, s, r, 2, "mm")
	metadataTestCap3(t, a)
	metadataTestCap2(t, old)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmSwitchOnly: true, Request: 7})
	nom := metadataTestRead(t, a)
	if !nom.Baseline || !nom.MmSwitchOnly {
		t.Fatal("missing native category nomination", nom)
	}
	metadataTestMmBaseline(t, a, nom, []interface{}{0, 1024, 1})
	state := metadataTestRead(t, a)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if !ns.mmKnown || ns.known[1] || state.MmSwitchKnown == nil || !*state.MmSwitchKnown || state.Request != 7 {
		t.Fatal("MM bank falsely initialized tracker or lost request", state)
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmSwitches: metadataTestSwitches(
		[]interface{}{0, 10, false}, []interface{}{0, 32, false}, []interface{}{0, 63, true}, []interface{}{0, 63, false})})
	edit := metadataTestRead(t, a)
	legacy := metadataTestRead(t, old)
	if ns.mmSwitches[0] != [2]uint32{} || !ns.mmKnown || edit.Seq != state.Seq+1 || legacy.Seq != edit.Seq || legacy.MmSwitchKnown != nil || legacy.MmSwitches != nil {
		t.Fatal("ordered clears/known zero/legacy sequence lost", edit, legacy)
	}
	// New owner reconnect gets the retained zero; stale local SET cannot replace it.
	b := metadataTestClient(t, s, r, 3, "mm")
	metadataTestCap3(t, b)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmSwitchOnly: true, Request: 8})
	got := metadataTestRead(t, b)
	rows, ok := metadataMmSwitchEntries(got)
	if !ok || got.Baseline || got.Request != 8 || got.MmSwitchKnown == nil || !*got.MmSwitchKnown || len(rows) != 0 {
		t.Fatal("reconnect replaced known cleared state", got)
	}
}

func TestMetadataMmAdmissionAndBounds(t *testing.T) {
	yes, no := true, false
	for _, row := range [][]interface{}{{113, 1, 0}, {0, -1, 0}, {0, 4294967296, 0}, {0, 0, 0}, {0, 1}} {
		if _, ok := metadataMmSwitchEntries(metadataPacket{Type: metadataPrefix + "STATE", MmSwitchKnown: &yes, MmSwitches: metadataTestSwitches(row)}); ok {
			t.Fatal("invalid snapshot accepted", row)
		}
	}
	for _, row := range [][]interface{}{{0, 64, true}, {0, -1, false}, {0, 1, 1}, {113, 0, true}} {
		if _, ok := metadataMmSwitchEntries(metadataPacket{Type: metadataPrefix + "EDIT", MmSwitches: metadataTestSwitches(row)}); ok {
			t.Fatal("invalid edit accepted", row)
		}
	}
	if _, ok := metadataMmSwitchEntries(metadataPacket{Type: metadataPrefix + "STATE", MmSwitchKnown: &no, MmSwitches: metadataTestSwitches()}); !ok {
		t.Fatal("unknown empty state rejected")
	}
	s, r := metadataTestRoom()
	old := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap2(t, old)
	game := 1
	metadataTestSend(old, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1, MmSwitchOnly: true})
	metadataTestEmpty(t, old)
	if len(r.metadata) != 0 {
		t.Fatal("cap2 acquired MM authority")
	}
	oot := metadataTestClient(t, s, r, 2, "oot")
	metadataTestCap3(t, oot)
	metadataTestSend(oot, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1, MmSwitchOnly: true})
	metadataTestEmpty(t, oot)
	if len(r.metadata) != 0 {
		t.Fatal("OoT owner acquired MM authority")
	}
}

func TestMetadataMmBaselinePageBarrierAndBufferedOrder(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap3(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1, MmSwitchOnly: true})
	nom := metadataTestRead(t, a)
	yes, no := true, false
	page := metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 2,
		Known: []bool{false, false}, OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(),
		MmSwitchKnown: &yes, MmSwitches: metadataTestSwitches([]interface{}{0, 1, 0})}
	metadataTestSend(a, page)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if ns.mmKnown || ns.seq != 0 {
		t.Fatal("partial baseline became authority")
	}
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmSwitches: metadataTestSwitches([]interface{}{0, 0, false})})
	page.Page, page.MmSwitches = 1, metadataTestSwitches([]interface{}{1, 0, 1})
	metadataTestSend(a, page)
	metadataTestRead(t, a) // complete snapshot
	metadataTestRead(t, a) // ordered buffered clear
	if !ns.mmKnown || ns.mmSwitches[0] != [2]uint32{} || ns.mmSwitches[1] != [2]uint32{0, 1} || ns.seq != 2 {
		t.Fatal("baseline/edit folding wrong", ns.mmSwitches, ns.seq)
	}
}

func TestMetadataMmTrackerFillDoesNotReplaceBank(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap3(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1, MmSwitchOnly: true})
	metadataTestMmBaseline(t, a, metadataTestRead(t, a), []interface{}{0, 1, 2})
	metadataTestRead(t, a)
	b := metadataTestClient(t, s, r, 2, "mm")
	metadataTestCap3(t, b)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 2})
	nom := metadataTestRead(t, b)
	no := false
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1,
		Known: []bool{false, true}, Checks: metadataTestRows([]interface{}{1, 29, nil, true}),
		OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(), MmSwitchKnown: &no, MmSwitches: metadataTestSwitches()})
	metadataTestRead(t, b)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if !ns.known[1] || !ns.mmKnown || ns.mmSwitches[0] != [2]uint32{1, 2} || !ns.checks[4096+29].skipped {
		t.Fatal("tracker fill reset independent MM bank", ns)
	}
}

func TestMetadataMmOnlyRejectsOtherAuthority(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap3(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1, MmSwitchOnly: true})
	nom := metadataTestRead(t, a)
	yes, no := true, false
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1,
		Known: []bool{false, true}, OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(),
		MmSwitchKnown: &yes, MmSwitches: metadataTestSwitches([]interface{}{0, 1, 0})})
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if ns.seq != 0 || ns.known != [2]bool{} || ns.switchKnown || ns.mmKnown {
		t.Fatal("switch-only offer acquired unrelated authority", ns)
	}
	if !metadataTestRead(t, a).Resync {
		t.Fatal("invalid baseline did not request recovery")
	}
}

func TestMetadataCap3OotDoesNotWaitForMm(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "oot")
	metadataTestCap3(t, a)
	game := 0
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 1})
	nom := metadataTestRead(t, a)
	yes, no := true, false
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 1,
		Known: []bool{true, false}, OotSwitchKnown: &yes, OotSwitches: metadataTestSwitches([]interface{}{0, 4}),
		MmSwitchKnown: &no, MmSwitches: metadataTestSwitches()})
	state := metadataTestRead(t, a)
	if state.Type != metadataPrefix+"STATE" || state.Known[0] != true || state.OotSwitchKnown == nil || !*state.OotSwitchKnown || *state.MmSwitchKnown {
		t.Fatal("OoT capability waited for unsupported MM authority", state)
	}
}
