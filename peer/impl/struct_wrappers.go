package impl

import (
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"math/rand"
	"sync"
	"time"
)

type RoutingTableWrapper struct {
	sync.RWMutex
	t peer.RoutingTable
}

type PeerListWrapper struct {
	sync.RWMutex
	// no self address
	plMap  map[string]bool // for fast lookup
	plList []string        // for random access
}

type VectorClock struct {
	sync.RWMutex
	vc map[string]int
}

type MsgStore struct {
	sync.RWMutex
	store map[string][]types.Rumor
}

type AckStore struct {
	sync.RWMutex
	store map[string]chan bool
}

type WaitingChannelStore struct {
	sync.RWMutex
	store map[string]chan *types.Message
}

type SafeChannelMapWrapper struct {
	sync.RWMutex
	store map[string]chan interface{}
}

type SafeMapWrapper struct {
	sync.RWMutex
	store map[string]interface{}
}

type CatalogWrapper struct {
	sync.RWMutex
	catalog peer.Catalog
}

func (c *CatalogWrapper) GetCatalog() (catalog peer.Catalog) {
	c.RLock()
	defer c.RUnlock()

	catalog = map[string]map[string]struct{}{}
	for k, v := range c.catalog {
		catalog[k] = make(map[string]struct{})
		for v1 := range v {
			catalog[k][v1] = struct{}{}
		}
	}
	return catalog
}

func (c *CatalogWrapper) GetRandomPeer(hash string) (peer string) {
	c.RLock()
	defer c.RUnlock()

	candidates, exist := c.catalog[hash]
	if !exist {
		return ""
	}

	for candidate := range candidates {
		return candidate
	}
	return ""
}

func (c *CatalogWrapper) UpdateCatalog(key string, peer string) {
	c.Lock()
	defer c.Unlock()

	if _, exist := c.catalog[key]; !exist {
		c.catalog[key] = make(map[string]struct{})
	}

	c.catalog[key][peer] = struct{}{}
}

func (c *CatalogWrapper) RemoveCatalog(key string, peer string) {
	c.Lock()
	defer c.Unlock()

	delete(c.catalog[key], peer)
}

func (s *WaitingChannelStore) Add(id string) chan *types.Message {
	s.Lock()
	defer s.Unlock()

	s.store[id] = make(chan *types.Message)
	return s.store[id]
}

func (s *WaitingChannelStore) Remove(id string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, id)
}

func (s *WaitingChannelStore) Get(id string) (waitingChan chan *types.Message, exist bool) {
	s.RLock()
	defer s.RUnlock()
	waitingChan, exist = s.store[id]
	return
}

func (s *SafeChannelMapWrapper) Add(id string) chan interface{} {
	s.Lock()
	defer s.Unlock()

	s.store[id] = make(chan interface{})
	return s.store[id]
}

func (s *SafeChannelMapWrapper) Remove(id string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, id)
}

func (s *SafeChannelMapWrapper) Get(id string) (waitingChan chan interface{}, exist bool) {
	s.RLock()
	defer s.RUnlock()
	waitingChan, exist = s.store[id]
	return
}

func (s *SafeMapWrapper) Set(key string, value interface{}) {
	s.Lock()
	defer s.Unlock()

	s.store[key] = value
}

func (s *SafeMapWrapper) Remove(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, key)
}

func (s *SafeMapWrapper) Get(key string) (value interface{}, exist bool) {
	s.RLock()
	defer s.RUnlock()
	value, exist = s.store[key]
	return
}

func (s *SafeMapWrapper) Reset() {
	s.Lock()
	defer s.Unlock()
	s.store = map[string]interface{}{}
}

func (s *AckStore) Add(pktID string) chan bool {
	s.Lock()
	defer s.Unlock()

	s.store[pktID] = make(chan bool)
	return s.store[pktID]
}

func (s *AckStore) Remove(pktID string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, pktID)
}

func (s *AckStore) Get(pktID string) (ackChan chan bool, exist bool) {
	s.RLock()
	defer s.RUnlock()
	ackChan, exist = s.store[pktID]
	return
}

func (s *MsgStore) Add(origin string, rumor types.Rumor) {
	s.Lock()
	defer s.Unlock()

	s.store[origin] = append(s.store[origin], rumor)
}

func (s *MsgStore) GetFromIndex(origin string, index int) (res []types.Rumor) {
	s.RLock()
	defer s.RUnlock()

	if index < 1 || index > len(s.store[origin]) {
		return
	}

	return s.store[origin][index-1:]
}

func (s *MsgStore) GetFullStore() (res map[string][]types.Rumor) {
	s.RLock()
	defer s.RUnlock()

	for k, v := range s.store {
		res[k] = v
	}
	return
}

func (p *PeerListWrapper) Add(peers []string) {
	p.Lock()
	defer p.Unlock()

	for _, k := range peers {
		if _, exist := p.plMap[k]; !exist {
			p.plMap[k] = true
			p.plList = append(p.plList, k)
		}
	}
}

func (p *PeerListWrapper) GetRandomPeer() (string, bool) {
	p.RLock()
	defer p.RUnlock()

	if len(p.plList) == 0 {
		return "", false
	}

	return p.plList[rand.Intn(len(p.plList))], true
}

// helper func for PeerListWrapper.GetRandomPeerAvoidAddr
func checkValidity(avoidAddr []string, target string) bool {
	for _, s := range avoidAddr {
		if target == s {
			return false
		}
	}
	return true
}

func (p *PeerListWrapper) GetRandomPeerAvoidAddr(avoidAddr []string) (string, bool) {
	p.RLock()
	defer p.RUnlock()
	if avoidAddr == nil {
		return p.GetRandomPeer()
	}

	if len(p.plList) == 0 {
		return "", false
	}

	res := p.plList[rand.Intn(len(p.plList))]
	if checkValidity(avoidAddr, res) {
		return res, true
	}

	newPlList := make([]string, 0)
	for _, old := range p.plList {
		if checkValidity(avoidAddr, old) {
			newPlList = append(newPlList, old)
		}
	}

	if len(newPlList) == 0 {
		return "", false
	}
	return newPlList[rand.Intn(len(newPlList))], true
}

func (p *PeerListWrapper) Contains(origin string) bool {
	p.RLock()
	defer p.RUnlock()

	_, exist := p.plMap[origin]
	return exist
}

func (p *PeerListWrapper) GetSize() int {
	return len(p.plMap)
}

func (p *PeerListWrapper) GetPeers(num int) (peers []string) {
	p.RLock()
	defer p.RUnlock()

	rand.Seed(time.Now().Unix())
	indexList := rand.Perm(len(p.plList))
	for i := 0; i < num; i++ {
		peers = append(peers, p.plList[indexList[i]])
	}
	return
}

func (p *PeerListWrapper) GetPeersAvoidAddr(num int, avoidAddr []string) (peers []string) {
	p.RLock()
	defer p.RUnlock()
	if avoidAddr == nil {
		return p.GetPeers(num)
	}

	if len(p.plList) == 0 {
		return []string{}
	}

	newPlList := make([]string, 0)
	for _, old := range p.plList {
		if checkValidity(avoidAddr, old) {
			newPlList = append(newPlList, old)
		}
	}

	if len(newPlList) == 0 {
		return []string{}
	}

	rand.Seed(time.Now().Unix())
	indexList := rand.Perm(len(newPlList))
	for i := 0; i < num && i < len(newPlList); i++ {
		peers = append(peers, newPlList[indexList[i]])
	}
	return peers
}

func (v *VectorClock) Add(origin string) {
	v.Lock()
	defer v.Unlock()

	v.vc[origin] = 1
}

func (v *VectorClock) SelfIncrement(origin string) int {
	v.Lock()
	defer v.Unlock()

	seq := v.vc[origin]
	seq++
	v.vc[origin] = seq
	return seq
}

func (v *VectorClock) CompareAndIncrement(origin string, expected int) bool {
	v.Lock()
	defer v.Unlock()

	seq := v.vc[origin]
	if seq+1 == expected {
		v.vc[origin] = expected
		return true
	}

	return false
}

func (v *VectorClock) GetView() map[string]uint {
	res := make(map[string]uint)
	v.RLock()
	defer v.RUnlock()
	for k, value := range v.vc {
		res[k] = uint(value)
	}
	return res
}

func (r *RoutingTableWrapper) Add(ks []string) {
	r.Lock()
	defer r.Unlock()

	for _, k := range ks {
		r.t[k] = k
	}
}

func (r *RoutingTableWrapper) Get(k string) string {
	r.RLock()
	defer r.RUnlock()

	return r.t[k]
}

func (r *RoutingTableWrapper) GetALL() peer.RoutingTable {
	res := make(peer.RoutingTable)
	r.RLock()
	defer r.RUnlock()
	for k, v := range r.t {
		res[k] = v
	}
	return res
}

func (r *RoutingTableWrapper) Update(k, v string) {
	r.Lock()
	defer r.Unlock()

	r.t[k] = v
}

func (r *RoutingTableWrapper) Delete(k string) {
	r.Lock()
	defer r.Unlock()

	delete(r.t, k)
}

type Paxos struct {
	step               SafeUint
	isConsensusReached SafeUint
	// proposer
	phase             SafeUint
	proposerRunning   SafeUint
	proposerCompleted chan bool
	// phase1
	collectedPromise        SafeUint
	majorityPromiseReceived chan bool
	// phase2
	majorityAcceptReceived chan *types.PaxosAcceptMessage
	highestAccepted        IDValuePair
	acceptedValueSet       SafeMapMapWrapper // map[string]map[string]struct{}
	//acceptor
	acceptorMaxSeenID SafeUint
	acceptorAccepted  IDValuePair
	//tlc
	tlcStore         TLCStore
	isTlcBroadcasted SafeUint
}

type SafeUint struct {
	sync.RWMutex
	val uint
}

func (s *SafeUint) Get() uint {
	s.RLock()
	defer s.RUnlock()

	return s.val
}

func (s *SafeUint) Set(v uint) {
	s.Lock()
	defer s.Unlock()

	s.val = v
}

func (s *SafeUint) IncrementAndGet() uint {
	s.Lock()
	defer s.Unlock()
	s.val++
	return s.val
}

func (s *SafeUint) CompareAndSetMax(newValue uint) (updated bool) {
	s.Lock()
	defer s.Unlock()
	if s.val < newValue {
		s.val = newValue
		return true
	}
	return false
}

type IDValuePair struct {
	sync.RWMutex
	id    uint
	value *types.PaxosValue
}

func (p *IDValuePair) Get() (uint, *types.PaxosValue) {
	p.RLock()
	defer p.RUnlock()

	return p.id, p.value
}

func (p *IDValuePair) Set(id uint, value *types.PaxosValue) {
	p.Lock()
	defer p.Unlock()

	p.id = id
	p.value = value
}

func (p *IDValuePair) CompareAndSet(id uint, value *types.PaxosValue) {
	p.Lock()
	defer p.Unlock()

	if value != nil && id > p.id {
		p.id = id
		p.value = value
	}
}

type SafeMapMapWrapper struct {
	sync.RWMutex
	store map[string]map[string]struct{}
}

func (s *SafeMapMapWrapper) Set(key string, secondKey string) {
	s.Lock()
	defer s.Unlock()

	_, exist := s.store[key]
	if !exist {
		s.store[key] = map[string]struct{}{}
	}
	s.store[key][secondKey] = struct{}{}
}

func (s *SafeMapMapWrapper) Remove(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.store, key)
}

func (s *SafeMapMapWrapper) Contains(key, secondKey string) bool {
	s.RLock()
	defer s.RUnlock()
	contains := false
	value, exist := s.store[key]
	if exist {
		_, contains = value[secondKey]
	}
	return contains
}

func (s *SafeMapMapWrapper) GetSizeByKey(key string) int {
	s.RLock()
	defer s.RUnlock()
	if value, exist := s.store[key]; exist {
		return len(value)
	}
	return 0
}

type TLCCountPair struct {
	count int
	msg   *types.TLCMessage
}

type TLCStore struct {
	sync.RWMutex
	store map[uint]*TLCCountPair // step: (count, msg)
}

func (s *TLCStore) Get(step uint) (count int, msg *types.TLCMessage) {
	s.RLock()
	defer s.RUnlock()

	if pair, exist := s.store[step]; exist {
		return pair.count, pair.msg
	}
	return 0, &types.TLCMessage{}
}

func (s *TLCStore) Add(step uint, msg *types.TLCMessage) (count int) {
	s.Lock()
	defer s.Unlock()

	if pair, exist := s.store[step]; exist {
		pair.count++
		return pair.count
	}
	s.store[step] = &TLCCountPair{1, msg}
	return 1
}

func (p *Paxos) SetProposerRunningOrWait() (proceed bool, waitChan chan bool) {
	p.proposerRunning.Lock()
	defer p.proposerRunning.Unlock()
	if p.proposerRunning.val == 1 {
		return false, p.proposerCompleted
	}
	p.proposerRunning.val = 1
	return true, nil
}

func (p *Paxos) ProposerExit() {
	p.proposerRunning.Lock()
	defer p.proposerRunning.Unlock()
	p.proposerRunning.val = 0

	select {
	case p.proposerCompleted <- true:
	default:
	}
}
