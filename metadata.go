package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const metadataPrefix = "DIPTYCH_METADATA_"
const metadataMaxEntries = 8192
const metadataMaxBytes = 512 * 1024
const metadataMaxSeq = uint64(1<<53 - 1)

type metadataScope struct {
	Proto uint32 `json:"proto"`
	Seed  string `json:"seed"`
	Room  string `json:"room"`
	Team  string `json:"team"`
}

type metadataPacket struct {
	Type           string              `json:"type"`
	ClientID       uint64              `json:"clientId"`
	Source         uint64              `json:"sourceClientId,omitempty"`
	Target         *uint64             `json:"targetClientId,omitempty"`
	Nonce          string              `json:"nonce,omitempty"`
	Cap            int                 `json:"cap,omitempty"`
	Epoch          string              `json:"epoch"`
	Scope          metadataScope       `json:"diptych"`
	Request        uint32              `json:"request,omitempty"`
	Game           *int                `json:"game,omitempty"`
	Baseline       bool                `json:"baseline,omitempty"`
	Page           int                 `json:"page"`
	Of             int                 `json:"of"`
	Seq            uint64              `json:"seq"`
	Known          []bool              `json:"known,omitempty"`
	Checks         [][]json.RawMessage `json:"checks"`
	Entrances      []int               `json:"entrances"`
	OotSwitchKnown *bool               `json:"ootSwitchKnown,omitempty"`
	OotSwitches    json.RawMessage     `json:"ootSwitches,omitempty"`
	MmSwitchKnown  *bool               `json:"mmSwitchKnown,omitempty"`
	MmSwitches     json.RawMessage     `json:"mmSwitches,omitempty"`
	MmSwitchOnly   bool                `json:"mmSwitchOnly,omitempty"`
	MmOwlKnown     *bool               `json:"mmOwlKnown,omitempty"`
	MmOwls         *uint16             `json:"mmOwls,omitempty"`
	MmProgressOnly bool                `json:"mmProgressOnly,omitempty"`
	Resync         bool                `json:"resync,omitempty"`
}

type metadataCheck struct {
	game, check int
	status      *int
	skipped     bool
}
type metadataWaiter struct {
	id             uint64
	request        uint32
	game           *int
	mmOnly         bool
	mmProgressOnly bool
}
type metadataBaseline struct {
	id             uint64
	request        uint32
	game           *int
	of             int
	known          []bool
	switchKnown    bool
	mmKnown        bool
	mmOnly         bool
	mmProgressOnly bool
	owlKnown       bool
	owls           uint16
	pages          map[int]metadataPacket
	bytes          int
}
type metadataNamespaceKey struct{ team, seed string }

type metadataNamespace struct {
	scope       metadataScope
	seq         uint64
	known       [2]bool
	checks      map[int]metadataCheck
	entrances   map[int]bool
	switchKnown bool
	switches    map[int]uint32
	mmKnown     bool
	mmSwitches  map[int][2]uint32
	owlKnown    bool
	owls        uint16
	waiters     []metadataWaiter
	baseline    *metadataBaseline
	edits       []metadataPacket
	nextRequest uint32
}

func newMetadataEpoch() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Native PrepRoomState emits canonical integer 0/1; older peers may emit JSON bools.
func metadataSharingOn(state string) bool {
	value := gjson.Get(state, "syncItemsAndFlags")
	return value.Type == gjson.True || (value.Type == gjson.Number && value.Raw == "1")
}

func (c *Client) metadataMembership(scope metadataScope, requireOn bool) bool {
	c.mu.Lock()
	online, capable, state, team := c.conn != nil, c.metadataCapable, c.state, c.team
	c.mu.Unlock()
	if !online || !capable || team == nil || scope.Proto != 1 || scope.Room != c.room.id || scope.Team != team.id ||
		scope.Seed == "" || len(scope.Seed) > 64 || gjson.Get(state, "diptych.proto").Raw != "1" ||
		gjson.Get(state, "diptych.seed").String() != scope.Seed {
		return false
	}
	if requireOn {
		c.room.mu.Lock()
		on := metadataSharingOn(c.room.state)
		c.room.mu.Unlock()
		if !on || c.room.id == "soh-global" {
			return false
		}
	}
	return true
}

func (c *Client) metadataOwnsGame(game int) bool {
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	want := "oot"
	if game == 1 {
		want = "mm"
	}
	return game >= 0 && game <= 1 && gjson.Get(state, "isSaveLoaded").Type == gjson.True &&
		gjson.Get(state, "diptych.game").String() == want
}

func (c *Client) metadataCapability() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.metadataCapable {
		return 0
	}
	if c.metadataCap < 2 {
		return 1
	}
	if c.metadataCap >= 4 {
		return 4
	}
	if c.metadataCap >= 3 {
		return 3
	}
	return 2
}

// The server validates the wire shape; SRAM persistence/reward eligibility belongs to MM.
type metadataMmSwitch struct {
	scene int
	banks [2]uint32
	flag  uint
	on    bool
}

func metadataMmSwitchEntries(p metadataPacket) ([]metadataMmSwitch, bool) {
	if len(p.MmSwitches) == 0 {
		return nil, p.MmSwitchKnown == nil
	}
	var raw [][]json.RawMessage
	if json.Unmarshal(p.MmSwitches, &raw) != nil || string(p.MmSwitches) == "null" {
		return nil, false
	}
	state := p.Type == metadataPrefix+"STATE"
	if state && p.MmSwitchKnown == nil || !state && p.MmSwitchKnown != nil {
		return nil, false
	}
	seen := map[int]bool{}
	rows := make([]metadataMmSwitch, 0, len(raw))
	for _, row := range raw {
		if len(row) != 3 {
			return nil, false
		}
		scene, ok := metadataUint(row[0], 0x70) // Native SCENE_MAX is 0x71.
		if !ok {
			return nil, false
		}
		v := metadataMmSwitch{scene: int(scene)}
		if state {
			a, aok := metadataUint(row[1], uint64(^uint32(0)))
			b, bok := metadataUint(row[2], uint64(^uint32(0)))
			if !aok || !bok || a|b == 0 || !*p.MmSwitchKnown || seen[int(scene)] {
				return nil, false
			}
			seen[int(scene)] = true
			v.banks = [2]uint32{uint32(a), uint32(b)}
		} else {
			flag, ok := metadataUint(row[1], 63)
			if !ok || (string(row[2]) != "true" && string(row[2]) != "false") {
				return nil, false
			}
			v.flag, v.on = uint(flag), string(row[2]) == "true"
		}
		rows = append(rows, v) // Preserve repeated SET/UNSET order.
	}
	return rows, true
}

// Match native saved-switch exclusions. Temporary switches (bits >=32) are not part of this bank.
func metadataSwitchMask(scene int) uint32 {
	mask := ^uint32(0)
	switch scene {
	case 3:
		mask &^= 1 << 27 // Forest Temple elevator
	case 5:
		mask &^= (1 << 28) | (1 << 29) | (1 << 30) // Water Temple levels
	case 26:
		mask &^= 1 << 23 // Ganon's Tower collapse timer
	}
	return mask
}

type metadataSwitch struct {
	scene int
	mask  uint32
	bit   uint
	on    bool
}

func metadataSwitchEntries(p metadataPacket) ([]metadataSwitch, bool) {
	if len(p.OotSwitches) == 0 {
		return nil, p.OotSwitchKnown == nil
	}
	var raw [][]json.RawMessage
	if json.Unmarshal(p.OotSwitches, &raw) != nil || string(p.OotSwitches) == "null" {
		return nil, false
	}
	if len(raw)+len(p.Checks)+len(p.Entrances) > 64 {
		return nil, false
	}
	state := p.Type == metadataPrefix+"STATE"
	if state && p.OotSwitchKnown == nil || !state && p.OotSwitchKnown != nil {
		return nil, false
	}
	seen := map[int]bool{}
	rows := make([]metadataSwitch, 0, len(raw))
	for _, row := range raw {
		if state && len(row) != 2 || !state && len(row) != 3 {
			return nil, false
		}
		scene, ok := metadataUint(row[0], 109)
		if !ok {
			return nil, false
		}
		value := metadataSwitch{scene: int(scene)}
		if state {
			mask, ok := metadataUint(row[1], uint64(^uint32(0)))
			if !ok || mask == 0 || mask&uint64(^metadataSwitchMask(int(scene))) != 0 ||
				!*p.OotSwitchKnown || seen[int(scene)] {
				return nil, false
			}
			seen[int(scene)] = true
			value.mask = uint32(mask)
		} else {
			bit, ok := metadataUint(row[1], 31)
			if !ok || (metadataSwitchMask(int(scene))&(uint32(1)<<bit)) == 0 ||
				(string(row[2]) != "true" && string(row[2]) != "false") {
				return nil, false
			}
			value.bit, value.on = uint(bit), string(row[2]) == "true"
		}
		rows = append(rows, value) // Repeated EDIT bits remain ordered; never collapse them into a map.
	}
	return rows, true
}

func metadataUint(raw json.RawMessage, max uint64) (uint64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(string(raw), 10, 64)
	return v, err == nil && v <= max
}

func metadataEntries(p metadataPacket) ([]metadataCheck, bool) {
	oot, ook := metadataSwitchEntries(p)
	mm, mok := metadataMmSwitchEntries(p)
	if !ook || !mok || !metadataOwlValid(p) || len(p.Checks)+len(p.Entrances)+len(oot)+len(mm) > 64 {
		return nil, false
	}
	checks := make([]metadataCheck, 0, len(p.Checks))
	seen := map[int]bool{}
	for _, row := range p.Checks {
		if len(row) != 4 {
			return nil, false
		}
		game, ok := metadataUint(row[0], 1)
		if !ok {
			return nil, false
		}
		check, ok := metadataUint(row[1], 4095)
		if !ok || check == 0 {
			return nil, false
		}
		var status *int
		if string(row[2]) != "null" {
			value, ok := metadataUint(row[2], 3)
			if !ok || game == 1 {
				return nil, false
			}
			s := int(value)
			status = &s
		}
		var skipped bool
		if err := json.Unmarshal(row[3], &skipped); err != nil || (string(row[3]) != "true" && string(row[3]) != "false") {
			return nil, false
		}
		key := int(game)*4096 + int(check)
		if seen[key] {
			return nil, false
		}
		seen[key] = true
		checks = append(checks, metadataCheck{int(game), int(check), status, skipped})
	}
	seenEntrance := map[int]bool{}
	for _, v := range p.Entrances {
		if v < 0 || v > 0x820 || seenEntrance[v] {
			return nil, false
		}
		seenEntrance[v] = true
	}
	return checks, true
}

func metadataOwlValid(p metadataPacket) bool {
	if p.MmOwls == nil && p.MmOwlKnown == nil {
		return true
	}
	if p.MmOwls == nil || *p.MmOwls > 0x3ff {
		return false
	}
	if p.Type == metadataPrefix+"STATE" {
		return p.MmOwlKnown != nil && (*p.MmOwlKnown || *p.MmOwls == 0)
	}
	return p.Type == metadataPrefix+"EDIT" && p.MmOwlKnown == nil && *p.MmOwls != 0
}

// Special packets never enter the stock Team state/queue or generic target routing.
// Replies are reconstructed with server origin; caller-supplied IDs are ignored.
func (c *Client) handleMetadata(raw string) bool {
	typ := gjson.Get(raw, "type").String()
	if !strings.HasPrefix(typ, metadataPrefix) {
		return false
	}
	var p metadataPacket
	if len(raw) > metadataMaxBytes || json.Unmarshal([]byte(raw), &p) != nil || p.Target == nil || *p.Target != 0 {
		return true
	}
	if typ == metadataPrefix+"HELLO" {
		if len(p.Nonce) == 0 || len(p.Nonce) > 32 || p.Cap < 0 {
			return true
		}
		c.mu.Lock()
		c.metadataCapable = true
		c.metadataCap = 1
		if p.Cap >= 2 {
			c.metadataCap = 2
		}
		if p.Cap >= 3 {
			c.metadataCap = 3
		}
		if p.Cap >= 4 {
			c.metadataCap = 4
		}
		capability := c.metadataCap
		state, team := c.state, c.team
		c.mu.Unlock()
		if team == nil {
			return true
		}
		scope := metadataScope{1, gjson.Get(state, "diptych.seed").String(), c.room.id, team.id}
		c.sendMetadata(metadataPacket{Type: typ, Nonce: p.Nonce, Cap: capability, Epoch: c.server.metadataEpoch, Scope: scope})
		return true
	}
	if p.Epoch != c.server.metadataEpoch || !c.metadataMembership(p.Scope, true) {
		return true
	}
	if c.metadataCapability() < 2 && (p.OotSwitchKnown != nil || len(p.OotSwitches) != 0) {
		return true
	}
	if c.metadataCapability() < 3 && (p.MmSwitchKnown != nil || len(p.MmSwitches) != 0 || p.MmSwitchOnly) {
		return true
	}
	if c.metadataCapability() < 4 && (p.MmOwlKnown != nil || p.MmOwls != nil || p.MmProgressOnly) {
		return true
	}
	if !metadataOwlValid(p) {
		return true
	}
	if p.MmProgressOnly && (p.MmSwitchOnly || typ != metadataPrefix+"REQUEST" || p.Game == nil || *p.Game != 1) {
		return true
	}
	if p.MmProgressOnly {
		if len(p.Checks) != 0 || len(p.Entrances) != 0 || len(p.OotSwitches) != 0 ||
			(p.OotSwitchKnown != nil && *p.OotSwitchKnown) {
			return true
		}
		for _, known := range p.Known {
			if known {
				return true
			}
		}
	}
	if p.MmSwitchOnly && (typ != metadataPrefix+"REQUEST" || p.Game == nil || *p.Game != 1) {
		return true
	}
	if typ != metadataPrefix+"REQUEST" && typ != metadataPrefix+"STATE" && typ != metadataPrefix+"EDIT" {
		return true
	}
	if p.Game != nil && !c.metadataOwnsGame(*p.Game) {
		return true
	}
	r := c.room
	r.metadataMu.Lock()
	defer r.metadataMu.Unlock()
	if r.metadata == nil {
		r.metadata = map[metadataNamespaceKey]*metadataNamespace{}
	}
	key := metadataNamespaceKey{p.Scope.Team, p.Scope.Seed}
	ns := r.metadata[key]
	if ns == nil {
		if typ != metadataPrefix+"REQUEST" || len(r.metadata) >= 32 {
			c.sendMetadataResync(p.Scope, 0)
			return true
		}
		ns = &metadataNamespace{scope: p.Scope, checks: map[int]metadataCheck{}, entrances: map[int]bool{}, switches: map[int]uint32{}, mmSwitches: map[int][2]uint32{}}
		r.metadata[key] = ns
	}
	switch typ {
	case metadataPrefix + "REQUEST":
		if p.Request == 0 {
			return true
		}
		if ns.seq != 0 && !c.metadataNeedsBaseline(ns, p.Game, p.MmSwitchOnly, p.MmProgressOnly) {
			r.sendMetadataSnapshot(c, ns, p.Request)
			return true
		}
		found := false
		for i, w := range ns.waiters {
			if w.id == c.id {
				ns.waiters[i] = metadataWaiter{c.id, p.Request, p.Game, p.MmSwitchOnly, p.MmProgressOnly}
				found = true
				break
			}
		}
		if !found {
			if len(ns.waiters) >= 256 {
				c.sendMetadataResync(p.Scope, ns.seq)
				return true
			}
			ns.waiters = append(ns.waiters, metadataWaiter{c.id, p.Request, p.Game, p.MmSwitchOnly, p.MmProgressOnly})
		}
		r.nominateMetadata(ns)
	case metadataPrefix + "STATE":
		b := ns.baseline
		if b == nil || b.id != c.id || b.request != p.Request || p.Seq != 0 || len(p.Known) != 2 || p.Of < 1 || p.Of > 128 || p.Page < 0 || p.Page >= p.Of {
			return true
		}
		if b.game != nil && !c.metadataOwnsGame(*b.game) {
			r.failMetadataBaseline(ns)
			return true
		}
		if _, ok := metadataEntries(p); !ok {
			r.failMetadataBaseline(ns)
			return true
		}
		if c.metadataCapability() >= 2 && p.OotSwitchKnown == nil {
			r.failMetadataBaseline(ns)
			return true
		}
		if c.metadataCapability() >= 3 && p.MmSwitchKnown == nil {
			r.failMetadataBaseline(ns)
			return true
		}
		if c.metadataCapability() >= 4 && (p.MmOwlKnown == nil || p.MmOwls == nil) {
			r.failMetadataBaseline(ns)
			return true
		}
		if (b.mmOnly || b.mmProgressOnly) && (p.Known[0] || p.Known[1] || len(p.Checks) != 0 || len(p.Entrances) != 0 ||
			(p.OotSwitchKnown != nil && *p.OotSwitchKnown)) {
			r.failMetadataBaseline(ns)
			return true
		}
		if b.of == 0 {
			b.of = p.Of
			b.known = append([]bool(nil), p.Known...)
			b.switchKnown = p.OotSwitchKnown != nil && *p.OotSwitchKnown
			b.mmKnown = p.MmSwitchKnown != nil && *p.MmSwitchKnown
			b.owlKnown = p.MmOwlKnown != nil && *p.MmOwlKnown
			if p.MmOwls != nil {
				b.owls = *p.MmOwls
			}
		}
		if b.of != p.Of || b.known[0] != p.Known[0] || b.known[1] != p.Known[1] || b.switchKnown != (p.OotSwitchKnown != nil && *p.OotSwitchKnown) || b.mmKnown != (p.MmSwitchKnown != nil && *p.MmSwitchKnown) {
			r.failMetadataBaseline(ns)
			return true
		}
		if b.owlKnown != (p.MmOwlKnown != nil && *p.MmOwlKnown) || (p.MmOwls != nil && b.owls != *p.MmOwls) {
			r.failMetadataBaseline(ns)
			return true
		}
		if _, duplicate := b.pages[p.Page]; duplicate {
			r.failMetadataBaseline(ns)
			return true
		}
		b.bytes += len(raw)
		if b.bytes > metadataMaxBytes {
			r.failMetadataBaseline(ns)
			return true
		}
		b.pages[p.Page] = p
		if len(b.pages) == b.of {
			r.finishMetadataBaseline(ns)
		}
	case metadataPrefix + "EDIT":
		if p.Resync || p.Seq > metadataMaxSeq {
			return true
		}
		if _, ok := metadataEntries(p); !ok {
			return true
		}
		if !c.metadataOwnsEntries(p) {
			c.sendMetadataResync(p.Scope, ns.seq)
			return true
		}
		p.Source = c.id
		if ns.seq == 0 {
			if ns.baseline == nil || len(ns.edits) >= 512 {
				r.failMetadataBaseline(ns)
				c.sendMetadataResync(p.Scope, ns.seq)
				return true
			}
			ns.edits = append(ns.edits, p)
			return true
		}
		r.applyMetadataEdit(ns, p)
	}
	return true
}

func (c *Client) sendMetadata(p metadataPacket) {
	p.ClientID = 0
	p.Target = nil
	if c.metadataCapability() < 2 {
		p.OotSwitchKnown, p.OotSwitches = nil, nil
	}
	if c.metadataCapability() < 3 {
		p.MmSwitchKnown, p.MmSwitches, p.MmSwitchOnly = nil, nil, false
	}
	if c.metadataCapability() < 4 {
		p.MmOwlKnown, p.MmOwls, p.MmProgressOnly = nil, nil, false
	}
	if p.Checks == nil {
		p.Checks = [][]json.RawMessage{}
	}
	if p.Entrances == nil {
		p.Entrances = []int{}
	}
	data, err := json.Marshal(p)
	if err == nil {
		c.sendPacket(string(data))
	}
}

func (c *Client) metadataNeedsBaseline(ns *metadataNamespace, game *int, mmOnly, mmProgressOnly bool) bool {
	if mmProgressOnly {
		return !ns.mmKnown || !ns.owlKnown
	}
	if mmOnly {
		return !ns.mmKnown
	}
	return game != nil && (!ns.known[*game] || (*game == 0 && c.metadataCapability() >= 2 && !ns.switchKnown) || (*game == 1 && c.metadataCapability() >= 3 && !ns.mmKnown) || (*game == 1 && c.metadataCapability() >= 4 && !ns.owlKnown))
}
func (c *Client) sendMetadataResync(scope metadataScope, seq uint64) {
	c.sendMetadata(metadataPacket{Type: metadataPrefix + "EDIT", Epoch: c.server.metadataEpoch, Scope: scope, Seq: seq, Resync: true})
}

func (r *Room) nominateMetadata(ns *metadataNamespace) {
	if ns.baseline != nil {
		return
	}
	// A fresh namespace has no authority: nominate the oldest authenticated owner offer.
	var nominee *Client
	var game *int
	var mmOnly bool
	var mmProgressOnly bool
	if nominee == nil {
		for _, w := range ns.waiters {
			v, ok := r.clients.Load(w.id)
			if !ok {
				continue
			}
			c := v.(*Client)
			if !c.metadataMembership(ns.scope, true) || (w.game != nil && !c.metadataOwnsGame(*w.game)) {
				continue
			}
			if ns.seq != 0 && !c.metadataNeedsBaseline(ns, w.game, w.mmOnly, w.mmProgressOnly) {
				continue
			}
			nominee = c
			game = w.game
			mmOnly = w.mmOnly
			mmProgressOnly = w.mmProgressOnly
			break
		}
	}
	if nominee == nil {
		return
	}
	ns.nextRequest++
	if ns.nextRequest == 0 {
		ns.nextRequest++
	}
	b := &metadataBaseline{id: nominee.id, request: ns.nextRequest, game: game, mmOnly: mmOnly, mmProgressOnly: mmProgressOnly, pages: map[int]metadataPacket{}}
	ns.baseline = b
	nominee.sendMetadata(metadataPacket{Type: metadataPrefix + "REQUEST", Epoch: nominee.server.metadataEpoch, Scope: ns.scope, Request: b.request, Baseline: true, Game: game, MmSwitchOnly: mmOnly, MmProgressOnly: mmProgressOnly})
	time.AfterFunc(10*time.Second, func() {
		r.metadataMu.Lock()
		defer r.metadataMu.Unlock()
		if ns.baseline == b {
			r.failMetadataBaseline(ns)
		}
	})
}

func (r *Room) failMetadataBaseline(ns *metadataNamespace) {
	ns.baseline = nil
	ns.edits = nil
	for _, w := range ns.waiters {
		if v, ok := r.clients.Load(w.id); ok && v.(*Client).metadataMembership(ns.scope, true) {
			v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		}
	}
	ns.waiters = nil
}

func (r *Room) finishMetadataBaseline(ns *metadataNamespace) {
	b := ns.baseline
	if b == nil {
		return
	}
	v, ok := r.clients.Load(b.id)
	if !ok || !v.(*Client).metadataMembership(ns.scope, true) {
		r.failMetadataBaseline(ns)
		return
	}
	fill := [2]bool{}
	for game := 0; game < 2; game++ {
		fill[game] = !ns.known[game] && b.known[game] && (ns.seq == 0 || b.game == nil || *b.game == game)
	}
	fillSwitch := !ns.switchKnown && b.switchKnown && v.(*Client).metadataCapability() >= 2 &&
		(ns.seq == 0 || b.game == nil || *b.game == 0)
	fillMm := !ns.mmKnown && b.mmKnown && v.(*Client).metadataCapability() >= 3 &&
		v.(*Client).metadataOwnsGame(1) && (b.game == nil || *b.game == 1)
	owlOffer := !b.mmOnly && b.owlKnown && v.(*Client).metadataCapability() >= 4 &&
		v.(*Client).metadataOwnsGame(1) && (b.game == nil || *b.game == 1)
	if b.mmProgressOnly && (!b.mmKnown || !owlOffer) || b.mmOnly && !fillMm ||
		!b.mmOnly && !b.mmProgressOnly && b.game != nil && !fill[*b.game] && !(*b.game == 0 && fillSwitch) && !(*b.game == 1 && (fillMm || owlOffer)) {
		r.failMetadataBaseline(ns)
		return
	}
	checks := map[int]metadataCheck{}
	entrances := map[int]bool{}
	switches := map[int]uint32{}
	mmSwitches := map[int][2]uint32{}
	for page := 0; page < b.of; page++ {
		p := b.pages[page]
		rows, _ := metadataEntries(p)
		for _, row := range rows {
			key := row.game*4096 + row.check
			if _, exists := checks[key]; exists {
				r.failMetadataBaseline(ns)
				return
			}
			checks[key] = row
		}
		for _, e := range p.Entrances {
			if entrances[e] {
				r.failMetadataBaseline(ns)
				return
			}
			entrances[e] = true
		}
		bits, _ := metadataSwitchEntries(p)
		for _, bit := range bits {
			if _, exists := switches[bit.scene]; exists {
				r.failMetadataBaseline(ns)
				return
			}
			switches[bit.scene] = bit.mask
		}
		mmBits, _ := metadataMmSwitchEntries(p)
		for _, bit := range mmBits {
			if _, exists := mmSwitches[bit.scene]; exists {
				r.failMetadataBaseline(ns)
				return
			}
			mmSwitches[bit.scene] = bit.banks
		}
	}
	count := len(ns.checks) + len(ns.entrances) + len(ns.switches) + len(ns.mmSwitches)
	for key, row := range checks {
		if fill[row.game] {
			if _, exists := ns.checks[key]; !exists {
				count++
			}
		}
	}
	if fill[0] {
		count += len(entrances)
	}
	if fillSwitch {
		count += len(switches)
	}
	if fillMm {
		count += len(mmSwitches)
	}
	if count > metadataMaxEntries || ns.seq == metadataMaxSeq {
		r.failMetadataBaseline(ns)
		return
	}
	candidate := *ns
	candidate.checks = maps.Clone(ns.checks)
	candidate.entrances = maps.Clone(ns.entrances)
	candidate.switches = maps.Clone(ns.switches)
	candidate.mmSwitches = maps.Clone(ns.mmSwitches)
	if fillSwitch {
		candidate.switches, candidate.switchKnown = switches, true
	}
	if fillMm {
		candidate.mmSwitches, candidate.mmKnown = mmSwitches, true
	}
	if owlOffer {
		candidate.owls |= b.owls
		candidate.owlKnown = true
	}
	for key, row := range checks {
		if fill[row.game] {
			candidate.checks[key] = row
		}
	}
	if fill[0] {
		for e := range entrances {
			candidate.entrances[e] = true
		}
	}
	if !metadataSnapshotFits(&candidate) {
		r.failMetadataBaseline(ns)
		return
	}
	ns.checks, ns.entrances, ns.switches, ns.switchKnown = candidate.checks, candidate.entrances, candidate.switches, candidate.switchKnown
	ns.mmSwitches, ns.mmKnown = candidate.mmSwitches, candidate.mmKnown
	ns.owls, ns.owlKnown = candidate.owls, candidate.owlKnown
	wasEstablished := ns.seq != 0
	ns.known[0] = ns.known[0] || fill[0]
	ns.known[1] = ns.known[1] || fill[1]
	ns.seq++
	ns.baseline = nil
	waiters := ns.waiters
	ns.waiters = nil
	for _, w := range waiters {
		if v, ok := r.clients.Load(w.id); ok {
			c := v.(*Client)
			if c.metadataMembership(ns.scope, true) {
				if c.metadataNeedsBaseline(ns, w.game, w.mmOnly, w.mmProgressOnly) {
					ns.waiters = append(ns.waiters, w)
				} else {
					r.sendMetadataSnapshot(c, ns, w.request)
				}
			}
		}
	}
	if wasEstablished {
		r.broadcastMetadata(ns, metadataPacket{Type: metadataPrefix + "EDIT", Seq: ns.seq, Resync: true})
	}
	edits := ns.edits
	ns.edits = nil
	for _, p := range edits {
		r.applyMetadataEdit(ns, p)
	}
	r.nominateMetadata(ns)
}

func (r *Room) applyMetadataEdit(ns *metadataNamespace, p metadataPacket) {
	v, ok := r.clients.Load(p.Source)
	if !ok || !v.(*Client).metadataMembership(ns.scope, true) {
		return
	}
	if !v.(*Client).metadataOwnsEntries(p) {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	rows, _ := metadataEntries(p)
	bits, _ := metadataSwitchEntries(p)
	mmBits, _ := metadataMmSwitchEntries(p)
	if len(bits) > 0 && !ns.switchKnown {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	if len(mmBits) > 0 && !ns.mmKnown {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	if p.MmOwls != nil && !ns.owlKnown {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	count := len(ns.checks) + len(ns.entrances) + len(ns.switches) + len(ns.mmSwitches)
	for _, row := range rows {
		if !ns.known[row.game] {
			v.(*Client).sendMetadataResync(ns.scope, ns.seq)
			return
		}
		if _, ok := ns.checks[row.game*4096+row.check]; !ok {
			count++
		}
	}
	if len(p.Entrances) > 0 && !ns.known[0] {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	for _, e := range p.Entrances {
		if !ns.entrances[e] {
			count++
		}
	}
	if count > metadataMaxEntries || ns.seq == metadataMaxSeq {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	candidate := *ns
	candidate.checks = maps.Clone(ns.checks)
	candidate.entrances = maps.Clone(ns.entrances)
	candidate.switches = maps.Clone(ns.switches)
	candidate.mmSwitches = maps.Clone(ns.mmSwitches)
	if p.MmOwls != nil {
		candidate.owls |= *p.MmOwls
	}
	for _, bit := range mmBits {
		banks := candidate.mmSwitches[bit.scene]
		mask := uint32(1) << (bit.flag % 32)
		if bit.on {
			banks[bit.flag/32] |= mask
		} else {
			banks[bit.flag/32] &^= mask
		}
		if banks == [2]uint32{} {
			delete(candidate.mmSwitches, bit.scene)
		} else {
			candidate.mmSwitches[bit.scene] = banks
		}
	}
	for _, bit := range bits {
		mask := uint32(1) << bit.bit
		if bit.on {
			candidate.switches[bit.scene] |= mask
		} else {
			candidate.switches[bit.scene] &^= mask
		}
		if candidate.switches[bit.scene] == 0 {
			delete(candidate.switches, bit.scene)
		}
	}
	for _, row := range rows {
		key := row.game*4096 + row.check
		if row.status == nil && row.game == 0 {
			if previous, ok := ns.checks[key]; ok {
				row.status = previous.status
			}
		}
		candidate.checks[key] = row
	}
	for _, e := range p.Entrances {
		candidate.entrances[e] = true
	}
	if len(candidate.checks)+len(candidate.entrances)+len(candidate.switches)+len(candidate.mmSwitches) > metadataMaxEntries || !metadataSnapshotFits(&candidate) {
		v.(*Client).sendMetadataResync(ns.scope, ns.seq)
		return
	}
	ns.checks, ns.entrances, ns.switches = candidate.checks, candidate.entrances, candidate.switches
	ns.mmSwitches = candidate.mmSwitches
	ns.owls = candidate.owls
	ns.seq++
	p.Type = metadataPrefix + "EDIT"
	p.Seq = ns.seq
	p.Resync = false
	r.broadcastMetadata(ns, p)
}

func (c *Client) metadataOwnsEntries(p metadataPacket) bool {
	rows, ok := metadataEntries(p)
	if !ok {
		return false
	}
	for _, row := range rows {
		if !c.metadataOwnsGame(row.game) {
			return false
		}
	}
	bits, _ := metadataSwitchEntries(p)
	if len(bits) > 0 && (c.metadataCapability() < 2 || !c.metadataOwnsGame(0)) {
		return false
	}
	mmBits, _ := metadataMmSwitchEntries(p)
	if len(mmBits) > 0 && (c.metadataCapability() < 3 || !c.metadataOwnsGame(1)) {
		return false
	}
	if p.MmOwls != nil && (c.metadataCapability() < 4 || !c.metadataOwnsGame(1)) {
		return false
	}
	return len(p.Entrances) == 0 || c.metadataOwnsGame(0)
}

func (r *Room) broadcastMetadata(ns *metadataNamespace, p metadataPacket) {
	p.Scope = ns.scope
	r.clients.Range(func(_, v interface{}) bool {
		c := v.(*Client)
		if c.metadataMembership(ns.scope, true) {
			p.Epoch = c.server.metadataEpoch
			c.sendMetadata(p)
		}
		return true
	})
}

func metadataRow(c metadataCheck) []json.RawMessage {
	status := "null"
	if c.status != nil {
		status = strconv.Itoa(*c.status)
	}
	return []json.RawMessage{json.RawMessage(strconv.Itoa(c.game)), json.RawMessage(strconv.Itoa(c.check)), json.RawMessage(status), json.RawMessage(strconv.FormatBool(c.skipped))}
}

func metadataSnapshotPackets(ns *metadataNamespace, request uint32, epoch string) []metadataPacket {
	keys := make([]int, 0, len(ns.checks))
	for key := range ns.checks {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	entrances := make([]int, 0, len(ns.entrances))
	for e := range ns.entrances {
		entrances = append(entrances, e)
	}
	sort.Ints(entrances)
	scenes := make([]int, 0, len(ns.switches))
	for scene := range ns.switches {
		scenes = append(scenes, scene)
	}
	sort.Ints(scenes)
	mmScenes := make([]int, 0, len(ns.mmSwitches))
	for scene := range ns.mmSwitches {
		mmScenes = append(mmScenes, scene)
	}
	sort.Ints(mmScenes)
	total := len(keys) + len(entrances) + len(scenes) + len(mmScenes)
	pages := (total + 63) / 64
	if pages == 0 {
		pages = 1
	}
	out := make([]metadataPacket, 0, pages)
	owlKnown, owls := ns.owlKnown, ns.owls
	for page := 0; page < pages; page++ {
		p := metadataPacket{Type: metadataPrefix + "STATE", Epoch: epoch, Scope: ns.scope, Request: request, Seq: ns.seq, Page: page, Of: pages, Known: []bool{ns.known[0], ns.known[1]}}
		p.OotSwitchKnown = &ns.switchKnown
		p.MmSwitchKnown = &ns.mmKnown
		p.MmOwlKnown, p.MmOwls = &owlKnown, &owls
		switchRows := [][]uint32{}
		mmRows := [][]uint32{}
		for index := page * 64; index < (page+1)*64 && index < total; index++ {
			if index < len(keys) {
				p.Checks = append(p.Checks, metadataRow(ns.checks[keys[index]]))
			} else if index < len(keys)+len(entrances) {
				p.Entrances = append(p.Entrances, entrances[index-len(keys)])
			} else if index < len(keys)+len(entrances)+len(scenes) {
				scene := scenes[index-len(keys)-len(entrances)]
				switchRows = append(switchRows, []uint32{uint32(scene), ns.switches[scene]})
			} else {
				scene := mmScenes[index-len(keys)-len(entrances)-len(scenes)]
				banks := ns.mmSwitches[scene]
				mmRows = append(mmRows, []uint32{uint32(scene), banks[0], banks[1]})
			}
		}
		p.OotSwitches, _ = json.Marshal(switchRows)
		p.MmSwitches, _ = json.Marshal(mmRows)
		if p.Checks == nil {
			p.Checks = [][]json.RawMessage{}
		}
		if p.Entrances == nil {
			p.Entrances = []int{}
		}
		out = append(out, p)
	}
	return out
}

func metadataSnapshotFits(ns *metadataNamespace) bool {
	bytes := 0
	for _, p := range metadataSnapshotPackets(ns, ^uint32(0), strings.Repeat("e", 32)) {
		p.Seq = metadataMaxSeq
		raw, err := json.Marshal(p)
		if err != nil {
			return false
		}
		bytes += len(raw)
	}
	return bytes <= metadataMaxBytes
}

func (r *Room) sendMetadataSnapshot(c *Client, ns *metadataNamespace, request uint32) {
	for _, p := range metadataSnapshotPackets(ns, request, c.server.metadataEpoch) {
		c.sendMetadata(p)
	}
}

func (r *Room) cancelMetadataWork() {
	r.mu.Lock()
	on := metadataSharingOn(r.state)
	r.mu.Unlock()
	if on {
		return
	}
	r.metadataMu.Lock()
	defer r.metadataMu.Unlock()
	for _, ns := range r.metadata {
		ns.baseline = nil
		ns.waiters = nil
		ns.edits = nil
	}
}
