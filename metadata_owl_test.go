package main

import "testing"

func metadataTestCap4(t *testing.T, c *Client) {
	t.Helper()
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "HELLO", Cap: 4, Nonce: "cap4"})
	if p := metadataTestRead(t, c); p.Cap != 4 || c.metadataCapability() != 4 {
		t.Fatal("cap4 negotiation", p)
	}
}

func metadataTestOwlState(c *Client, nom metadataPacket, page, pages int, mask uint16) {
	yes, no := true, false
	metadataTestSend(c, metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request,
		Page: page, Of: pages, Known: []bool{false, false}, OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(),
		MmSwitchKnown: &yes, MmSwitches: metadataTestSwitches(), MmOwlKnown: &yes, MmOwls: &mask})
}

func TestMetadataOwlRetentionAndLegacySequence(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap4(t, a)
	old := metadataTestClient(t, s, r, 2, "mm")
	metadataTestCap3(t, old)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmProgressOnly: true, Request: 7})
	nom := metadataTestRead(t, a)
	if !nom.Baseline || !nom.MmProgressOnly || nom.MmSwitchOnly {
		t.Fatal("wrong nomination", nom)
	}
	metadataTestOwlState(a, nom, 0, 1, 0)
	state := metadataTestRead(t, a)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	if !ns.owlKnown || ns.owls != 0 || ns.known[0] || ns.known[1] || ns.switchKnown || state.MmOwls == nil {
		t.Fatal("owl baseline acquired other authority", state)
	}
	for _, mask := range []uint16{4, 64, 4} {
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmOwls: &mask})
		edit, legacy := metadataTestRead(t, a), metadataTestRead(t, old)
		if edit.Seq != legacy.Seq || legacy.MmOwls != nil || legacy.MmOwlKnown != nil || legacy.MmProgressOnly {
			t.Fatal("legacy field leak/sequence gap", edit, legacy)
		}
	}
	if ns.owls != 68 {
		t.Fatal("monotone union lost", ns.owls)
	}
	// A reconnect snapshot carries the retained union without asking another owner.
	b := metadataTestClient(t, s, r, 3, "mm")
	metadataTestCap4(t, b)
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmProgressOnly: true, Request: 8})
	got := metadataTestRead(t, b)
	if got.Baseline || got.MmOwls == nil || *got.MmOwls != 68 || got.Request != 8 {
		t.Fatal("retained owl union unavailable", got)
	}
	zero := uint16(0)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmOwls: &zero})
	metadataTestEmpty(t, a)
	if ns.owls != 68 {
		t.Fatal("zero edit cleared access")
	}
	r.mu.Lock()
	r.state = `{"syncItemsAndFlags":0}`
	r.mu.Unlock()
	more := uint16(128)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmOwls: &more})
	metadataTestEmpty(t, a)
	if ns.owls != 68 {
		t.Fatal("OFF admitted new access")
	}
}

func TestMetadataOwlBaselineAuthorityAndPageBarrier(t *testing.T) {
	for _, bad := range []string{"tracker", "oot", "scalar"} {
		t.Run(bad, func(t *testing.T) {
			s, r := metadataTestRoom()
			a := metadataTestClient(t, s, r, 1, "mm")
			metadataTestCap4(t, a)
			game := 1
			metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmProgressOnly: true, Request: 1})
			nom := metadataTestRead(t, a)
			yes, no := true, false
			mask := uint16(4)
			p := metadataPacket{Type: metadataPrefix + "STATE", Request: nom.Request, Of: 2,
				Known: []bool{false, false}, OotSwitchKnown: &no, OotSwitches: metadataTestSwitches(), MmSwitchKnown: &yes,
				MmSwitches: metadataTestSwitches(), MmOwlKnown: &yes, MmOwls: &mask}
			metadataTestSend(a, p)
			ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
			if ns.seq != 0 || ns.owlKnown {
				t.Fatal("incomplete page acquired authority")
			}
			p.Page = 1
			switch bad {
			case "tracker":
				p.Known[1] = true
			case "oot":
				p.OotSwitchKnown = &yes
			case "scalar":
				mask = 8
			}
			metadataTestSend(a, p)
			metadataTestRead(t, a) // Refused nomination requests a resync.
			if ns.seq != 0 || ns.owlKnown || ns.known[1] || ns.switchKnown {
				t.Fatal("invalid page acquired authority")
			}
		})
	}
	for _, p := range []metadataPacket{
		{Type: metadataPrefix + "STATE", MmOwlKnown: new(bool), MmOwls: func() *uint16 { x := uint16(1); return &x }()},
		{Type: metadataPrefix + "EDIT", MmOwls: func() *uint16 { x := uint16(1024); return &x }()},
	} {
		if metadataOwlValid(p) {
			t.Fatal("invalid owl scalar accepted")
		}
	}
}

func TestMetadataCap3AndOotDoNotWaitForOwls(t *testing.T) {
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap3(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmSwitchOnly: true, Request: 1})
	nom := metadataTestRead(t, a)
	metadataTestMmBaseline(t, a, nom)
	metadataTestRead(t, a)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmSwitchOnly: true, Request: 2})
	if got := metadataTestRead(t, a); got.Baseline || got.MmOwls != nil {
		t.Fatal("cap3 waits for owls", got)
	}
	b := metadataTestClient(t, s, r, 2, "oot")
	metadataTestCap4(t, b)
	ns := r.metadata[metadataNamespaceKey{"team", "seed"}]
	ns.known[0], ns.switchKnown = true, true
	game = 0
	metadataTestSend(b, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, Request: 3})
	if got := metadataTestRead(t, b); got.Baseline || got.MmOwlKnown == nil || *got.MmOwlKnown {
		t.Fatal("OoT waits for unrelated owl owner", got)
	}
}

func TestMetadataOwlRequestRejectsForeignAuthority(t *testing.T) {
	for _, gameName := range []string{"mm", "oot"} {
		s, r := metadataTestRoom()
		a := metadataTestClient(t, s, r, 1, gameName)
		metadataTestCap4(t, a)
		game := 1
		metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game,
			MmProgressOnly: true, Request: 1, Known: []bool{false, true}})
		metadataTestEmpty(t, a)
		if len(r.metadata) != 0 {
			t.Fatal("invalid request created namespace", gameName)
		}
	}
	s, r := metadataTestRoom()
	a := metadataTestClient(t, s, r, 1, "mm")
	metadataTestCap3(t, a)
	game := 1
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "REQUEST", Game: &game, MmProgressOnly: true, Request: 1})
	metadataTestEmpty(t, a)
	mask := uint16(4)
	metadataTestSend(a, metadataPacket{Type: metadataPrefix + "EDIT", MmOwls: &mask})
	metadataTestEmpty(t, a)
	if len(r.metadata) != 0 {
		t.Fatal("cap3 acquired owl authority")
	}
}
