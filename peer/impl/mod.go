package impl

import (
	"context"
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	routing := make(peer.RoutingTable)
	routing[conf.Socket.GetAddress()] = conf.Socket.GetAddress()

	n := &node{
		conf:          conf,
		wg:            sync.WaitGroup{},
		stopped:       make(chan bool, 1),
		socketTimeout: time.Second * 1,
		router:        RoutingTableWrapper{t: routing},
		peers:         PeerListWrapper{plList: make([]string, 0), plMap: map[string]bool{}},
		vectorClock:   VectorClock{vc: map[string]int{}},
		msgStore:      MsgStore{store: map[string][]types.Rumor{}},
		ackStore:      AckStore{store: map[string]chan bool{}},
		waitingStore:  WaitingChannelStore{store: map[string]chan *types.Message{}},
		searchResult:  SafeChannelMapWrapper{store: map[string]chan interface{}{}},
		catalog:       CatalogWrapper{catalog: map[string]map[string]struct{}{}},
		cache:         SafeMapWrapper{store: map[string]interface{}{}},
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.paxos = Paxos{
		step:                    SafeUint{},
		isConsensusReached:      SafeUint{},
		phase:                   SafeUint{},
		collectedPromise:        SafeUint{},
		majorityPromiseReceived: make(chan bool, 10),
		majorityAcceptReceived:  make(chan *types.PaxosAcceptMessage, 10),
		highestAccepted:         IDValuePair{},
		acceptedValueSet:        SafeMapMapWrapper{store: map[string]map[string]struct{}{}},
		acceptorMaxSeenID:       SafeUint{},
		acceptorAccepted:        IDValuePair{},
		tlcStore:                TLCStore{store: map[uint]*TLCCountPair{}},
		isTlcBroadcasted:        SafeUint{},
		proposerRunning:         SafeUint{},
		proposerCompleted:       make(chan bool),
	}
	conf.MessageRegistry.RegisterMessageCallback(&types.EmptyMessage{}, ExecEmptyMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.ChatMessage{}, ExecChatMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.RumorsMessage{}, n.ExecRumorMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.StatusMessage{}, n.ExecStatusMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.AckMessage{}, n.ExecAckMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.PrivateMessage{}, n.ExecPrivateMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.DataReplyMessage{}, n.ExecDataReplyMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.DataRequestMessage{}, n.ExecDataRequestMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchReplyMessage{}, n.ExecSearchReplyMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.SearchRequestMessage{}, n.ExecSearchRequestMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosPrepareMessage{}, n.ExecPaxosPrepareMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosPromiseMessage{}, n.ExecPaxosPromiseMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosProposeMessage{}, n.ExecPaxosProposeMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.PaxosAcceptMessage{}, n.ExecPaxosAcceptMessage)
	conf.MessageRegistry.RegisterMessageCallback(&types.TLCMessage{}, n.ExecTLCMessage)

	return n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf          peer.Configuration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       bool
	stopped       chan bool
	socketTimeout time.Duration
	router        RoutingTableWrapper
	peers         PeerListWrapper
	vectorClock   VectorClock
	msgStore      MsgStore
	ackStore      AckStore
	waitingStore  WaitingChannelStore
	searchResult  SafeChannelMapWrapper
	catalog       CatalogWrapper
	cache         SafeMapWrapper
	paxos         Paxos
}

// Start implements peer.Service
func (n *node) Start() error {
	n.running = true
	n.stopped = make(chan bool, 1)
	go func() { // recv loop
		for {
			select {
			case <-n.ctx.Done():
				n.wg.Wait()
				if len(n.stopped) == 0 {
					n.stopped <- true
				} else {
					log.Error().Msgf("[peer.Peer.Stop] error: unreachable code/ unexpected exit")
				}
				return
			default:
				pkt, err := n.conf.Socket.Recv(n.socketTimeout)
				if errors.Is(err, transport.TimeoutError(0)) {
					continue
				}
				if err != nil {
					log.Error().Msgf("[peer.Peer.Start] error: %s", err.Error())
				}

				go n.RelayMsg(pkt)
			}
		}
	}()

	n.StartAntiEntropy()
	n.StartHeartbeat()

	return nil
}

func (n *node) RelayMsg(pkt transport.Packet) {
	if pkt.Header.Destination == n.conf.Socket.GetAddress() { // for this node
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("[peer.impl.RelayMsg] error: %s", err.Error())
		}
		return
	}

	if pkt.Header.TTL == 0 {
		log.Info().Msg("[peer.Peer.RelayMsg]: TTL is 0")
		//return
	}

	pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
	pkt.Header.TTL--

	dstAddr := n.router.Get(pkt.Header.Destination)
	if dstAddr == "" {
		log.Info().Msg("[peer.Peer.RelayMsg]: relay destination not found")
		return
	}

	err := n.conf.Socket.Send(dstAddr, pkt, n.socketTimeout)
	if err != nil {
		log.Error().Msgf("[peer.impl.RelayMsg] error: %s", err.Error())
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {
	if n.running {
		n.running = false
		n.cancel()
		<-n.stopped
	}
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	//log.Debug().Msgf("[Unicast] from %s to %s: %s", n.conf.Socket.GetAddress(), dest, msg.Type)
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}

	dstAddr := n.router.Get(dest)
	if dstAddr == "" {
		return fmt.Errorf("[peer.impl.Unicast]: dstAddr invalid: %s to %s", n.conf.Socket.GetAddress(), dest)
	}
	return n.conf.Socket.Send(dstAddr, pkt, n.socketTimeout)
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	n.router.Add(addr)
	n.peers.Add(addr)
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.router.GetALL()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.router.Delete(origin)
		return
	}

	n.router.Update(origin, relayAddr)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	self := n.conf.Socket.GetAddress()
	header := transport.NewHeader(self, self, self, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}

	// now it's a rumor
	seq := n.vectorClock.SelfIncrement(self)
	rumor := types.Rumor{
		Origin:   self,
		Sequence: uint(seq),
		Msg:      &msg,
	}
	n.msgStore.Add(self, rumor) // seq start from 1

	n.SendWithAckExpect(rumor)
	//log.Debug().Msgf("[peer.impl.Broadcast.ProcessPacket]: %s", rumor.String())

	if pkt.Msg.Type == "tlc" {
		// synchronous
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if err != nil {
			log.Error().Msgf("[peer.impl.Broadcast.ProcessPacket] error: %s", err.Error())
		}
	} else {
		go func() {
			// msg here is just a msg, not a rumor
			err := n.conf.MessageRegistry.ProcessPacket(pkt)
			if err != nil {
				log.Error().Msgf("[peer.impl.Broadcast.ProcessPacket] error: %s", err.Error())
			}
		}()
	}

	return nil
}

func (n *node) EndToEndSend(src, dstAddr string, raw types.Message) (pktID string, err error) {
	//log.Debug().Msgf("[EndToEndSend] from %s to %s: [%s]: %s", src, dstAddr, raw.Name(), raw.String())

	msg, err := n.conf.MessageRegistry.MarshalMessage(raw)
	if err != nil {
		log.Error().Msgf("[peer.impl.EndToEndSend.MarshalMessage] error: %s", err.Error())
	}
	header := transport.NewHeader(src, n.conf.Socket.GetAddress(), dstAddr, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	return header.PacketID, n.conf.Socket.Send(dstAddr, pkt, n.socketTimeout)
}

func (n *node) Send(directDst, realDst string, raw types.Message) (pktID string, err error) {
	//log.Debug().Msgf("[Send] from %s to %s:%s", n.conf.Socket.GetAddress(), realDst, raw.String())
	msg, err := n.conf.MessageRegistry.MarshalMessage(raw)
	if err != nil {
		log.Error().Msgf("[peer.impl.EndToEndSend.MarshalMessage] error: %s", err.Error())
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), realDst, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	return header.PacketID, n.conf.Socket.Send(directDst, pkt, n.socketTimeout)
}

// StartAntiEntropy HW1 Task3
func (n *node) StartAntiEntropy() {
	if n.conf.AntiEntropyInterval == 0 {
		return
	}

	t := time.NewTicker(n.conf.AntiEntropyInterval)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer t.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-t.C:
				_, dst, err := n.SendToRandomPeer(n.conf.Socket.GetAddress(), types.StatusMessage(n.vectorClock.GetView()), nil)
				if dst != "" && err != nil {
					log.Error().Msgf("[StartAntiEntropy.SendToRandomPeer] error: %s", err.Error())
				}
			}
		}
	}()
}

func (n *node) StartHeartbeat() {
	if n.conf.HeartbeatInterval == 0 {
		return
	}

	heartbeatMsg, _ := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
	err := n.Broadcast(heartbeatMsg)
	if err != nil {
		log.Error().Msgf("[StartHeartbeat.Broadcast] error: %s", err.Error())
	}

	t := time.NewTicker(n.conf.HeartbeatInterval)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer t.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-t.C:
				err := n.Broadcast(heartbeatMsg)
				if err != nil {
					log.Error().Msgf("[StartHeartbeat.Broadcast] error: %s", err.Error())
				}
			}
		}
	}()
}

func (n *node) SendToRandomPeer(src string, message types.Message, avoidPeers []string) (pktID, dstAddr string,
	err error) {
	dstAddr, success := n.peers.GetRandomPeerAvoidAddr(avoidPeers)
	if !success {
		return "", "", errors.New("fail to get random peer")
	}

	pktID, err = n.EndToEndSend(src, dstAddr, message)
	if err != nil {
		log.Error().Msgf("[SendToRandomPeer.EndToEndSend] error: %s", err.Error())
	}
	return pktID, dstAddr, err
}

func (n *node) SendWithAckExpect(rumor types.Rumor) {
	rumorsMsg := types.RumorsMessage{Rumors: []types.Rumor{rumor}}
	pktID, dstAddr, err := n.SendToRandomPeer(n.conf.Socket.GetAddress(), rumorsMsg, nil)
	if err != nil {
		log.Error().Msgf("[SendWithAckExpect] error: %s", err.Error())
		return
	}

	if n.conf.AckTimeout == 0 {
		return
	}

	go func() {
		waitChan := n.ackStore.Add(pktID)
		avoidPeers := []string{dstAddr}
		t := time.NewTimer(n.conf.AckTimeout)
		defer t.Stop()

		for {
			select {
			case <-n.ctx.Done():
				return
			case <-waitChan:
				n.ackStore.Remove(pktID)
				return
			case <-t.C:
				//log.Info().Msgf("[SendWithAckExpect.timeout] src:%s, pktID: %s, dstAddr: %s",
				//	n.conf.Socket.GetAddress(), pktID, dstAddr)
				n.ackStore.Remove(pktID)
				avoidPeers = append(avoidPeers, dstAddr)

				pktID, dstAddr, err = n.SendToRandomPeer(n.conf.Socket.GetAddress(), rumorsMsg, avoidPeers)
				if pktID != "" && err == nil {
					//log.Info().Msgf("[SendWithAckExpect.SendToRandomPeer] resent, " +
					//	"new pktID: %s, dstAddr: %s", pktID, dstAddr)
					waitChan = n.ackStore.Add(pktID)
					t.Reset(n.conf.AckTimeout)
				} else if err != nil {
					//log.Debug().Msgf("[SendWithAckExpect.SendToRandomPeer] error: %s", err.Error())
					return
				}
			}
		}
	}()
}

// WaitForReplyWithBackoff : blocking operation, wait for dataReplyMessage with backoff mechanism
func (n *node) WaitForReplyWithBackoff(dstAddr string, request types.DataRequestMessage) (reply []byte, err error) {
	msg, err := n.conf.MessageRegistry.MarshalMessage(request)
	if err != nil {
		return
	}

	err = n.Unicast(dstAddr, msg)
	if err != nil {
		return
	}

	requestID := request.RequestID
	waitChan := n.waitingStore.Add(requestID)

	retry := 0
	waitTime := n.conf.BackoffDataRequest.Initial
	for {
		t := time.NewTimer(waitTime)

		select {
		case <-n.ctx.Done():
			return
		case resp := <-waitChan:
			dataReplyMsg, _ := (*resp).(*types.DataReplyMessage)
			reply = dataReplyMsg.Value
			// some data integrity check ?
			t.Stop()
			n.waitingStore.Remove(requestID)
			return
		case <-t.C:
			log.Info().Msgf("[WaitForReplyWithBackoff.timeout] RequestID: %s, dstAddr: %s, retry: %d",
				requestID, dstAddr, retry)
			if uint(retry) < n.conf.BackoffDataRequest.Retry {
				err = n.Unicast(dstAddr, msg)
				if err != nil {
					return
				}
				retry++
				waitTime = time.Duration(uint(waitTime) * n.conf.BackoffDataRequest.Factor)
			} else {
				return nil, fmt.Errorf("all retries timeout, dstAddr: %s", dstAddr)
			}
		}
	}

}

func (n *node) Upload(data io.Reader) (metahash string, err error) {
	n.cache.Reset()
	chunk := make([]byte, n.conf.ChunkSize)

	metafileKey := make([]byte, 0)
	metafileValue := make([]byte, 0)
	for {
		bytesRead, err := data.Read(chunk)
		if errors.Is(err, io.EOF) {
			break
		}

		h := crypto.SHA256.New()
		_, err = h.Write(chunk[:bytesRead])
		if err != nil {
			return "", err
		}

		chunkHash := h.Sum(nil)
		chunkHex := hex.EncodeToString(chunkHash)

		// MetafileValue = F(c_0)||Sep||F(c_1)||Sep||...||F(c_N-2)||Sep||F(c_N-1)
		//  MetafileKey =
		//  F(SHA256(c_0)||SHA256(c_1)||...||SHA256(c_N-2)||SHA256(c_N-1)))
		metafileKey = append(metafileKey, chunkHash...)
		if len(metafileValue) != 0 {
			metafileValue = append(metafileValue, []byte(peer.MetafileSep)...)
		}
		metafileValue = append(metafileValue, chunkHex...)

		chunkCopy := make([]byte, bytesRead)
		copy(chunkCopy, chunk[:bytesRead])
		n.conf.Storage.GetDataBlobStore().Set(chunkHex, chunkCopy)
	}

	// store metafile
	h := crypto.SHA256.New()
	_, err = h.Write(metafileKey)
	if err != nil {
		return "", err
	}

	metahash = hex.EncodeToString(h.Sum(nil))
	n.conf.Storage.GetDataBlobStore().Set(metahash, metafileValue)
	return metahash, err
}

func (n *node) GetValueByHash(hash string) (replyValue []byte, err error) {
	// 1. check locally
	replyValue = n.conf.Storage.GetDataBlobStore().Get(hash)

	// 2. if not , get from peers
	if replyValue == nil {
		reqMsg := types.DataRequestMessage{
			RequestID: xid.New().String(),
			Key:       hash,
		}
		peerTarget := n.catalog.GetRandomPeer(hash)
		if peerTarget == "" {
			return nil, fmt.Errorf("[hash: %s] fail to get random peer from catalog", hash)
		}

		replyValue, err = n.WaitForReplyWithBackoff(peerTarget, reqMsg)
		if err != nil {
			return nil, err
		}
		if replyValue == nil {
			n.catalog.RemoveCatalog(hash, peerTarget)
			return nil, fmt.Errorf("[hash: %s] %s peer returns empty, catalog contains erroeous element",
				peerTarget, hash)
		}

		// update local storage
		n.conf.Storage.GetDataBlobStore().Set(hash, replyValue)
	}

	if replyValue == nil {
		return nil, fmt.Errorf("[hash: %s] fail to get element locally and remotely", hash)
	}
	return replyValue, err
}

func (n *node) Download(metahash string) (data []byte, err error) {
	n.cache.Reset()

	metafile, err := n.GetValueByHash(metahash)
	if err != nil {
		log.Error().Msgf("[Download.GetValueByHash] error: %s", err.Error())
		return nil, err
	}

	chunkKeys := strings.Split(string(metafile), peer.MetafileSep)

	for _, chunkHash := range chunkKeys {
		chunk, err := n.GetValueByHash(chunkHash)
		if err != nil {
			log.Error().Msgf("[Download.GetValueByHash] error: %s", err.Error())
			return nil, err
		}
		data = append(data, chunk...)
	}
	return data, err
}

func (n *node) Tag(name string, mh string) error {
	if n.conf.Storage.GetNamingStore().Get(name) != nil {
		return errors.New("[Tag] name exists")
	}

	if n.conf.TotalPeers <= 1 {
		n.conf.Storage.GetNamingStore().Set(name, []byte(mh))
		return nil
	}

	proceed, waitChan := n.paxos.SetProposerRunningOrWait()
	for !proceed && waitChan != nil {
		<-waitChan
		proceed, waitChan = n.paxos.SetProposerRunningOrWait()
	}

	for {
		success, err := n.RunSingleProposer(name, mh)
		if success {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (n *node) RunSingleProposer(name string, mh string) (success bool, err error) {
	defer n.paxos.phase.Set(0)
	// single instance paxos
	n.paxos.phase.Lock()
	// set up for proposer
	n.paxos.collectedPromise.Set(0)
	n.paxos.highestAccepted.Set(0, nil)
	n.paxos.phase.Unlock()
	var acceptMsg *types.PaxosAcceptMessage
	for {
		// main loop for paxos phase 1 + phase 2
		n.paxos.phase.Set(1)
		proceed, err := n.Phase1()
		if err != nil {
			return false, errors.New("[Phase1] error: " + err.Error())
		}
		if !proceed {
			continue
		}
		n.paxos.phase.Set(2)
		acceptMsg, err = n.Phase2(name, mh)
		if err != nil {
			return false, errors.New("[Phase2] error: " + err.Error())
		}
		if acceptMsg != nil {
			break
		}
	}

	if acceptMsg.Value.Filename == name && acceptMsg.Value.Metahash == mh {
		// self-proposed consensus reached
		return true, nil
	} else if acceptMsg.Value.Filename == name && acceptMsg.Value.Metahash != mh {
		return false, errors.New("[Tag] filename conflict")
	}

	return false, nil
}
func (n *node) Phase1() (bool, error) {
	prepareMsg := types.PaxosPrepareMessage{
		Step:   n.paxos.step.Get(),
		ID:     n.conf.PaxosID,
		Source: n.conf.Socket.GetAddress(),
	}
	//log.Debug().Msgf("[%s] prepare consensus: %d", n.conf.Socket.GetAddress(), n.conf.PaxosID)
	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
	if err != nil {
		return false, err
	}
	err = n.Broadcast(transportMsg)
	if err != nil {
		return false, err
	}

	t := time.NewTimer(n.conf.PaxosProposerRetry)
	proceed := false
	select {
	case <-n.paxos.majorityPromiseReceived:
		t.Stop()
		proceed = true
	case <-t.C:
		n.conf.PaxosID += n.conf.TotalPeers
	}
	return proceed, nil
}

func (n *node) Phase2(name string, mh string) (*types.PaxosAcceptMessage, error) {
	proposeMsg := types.PaxosProposeMessage{
		Step: n.paxos.step.Get(),
		ID:   n.conf.PaxosID,
	}
	if _, value := n.paxos.highestAccepted.Get(); value != nil {
		proposeMsg.Value = *value
	} else {
		// propose self message
		proposeMsg.Value = types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}
	}

	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(proposeMsg)
	if err != nil {
		return nil, err
	}

	err = n.Broadcast(transportMsg)
	if err != nil {
		return nil, err
	}

	t := time.NewTimer(n.conf.PaxosProposerRetry)
	var acceptMsg *types.PaxosAcceptMessage
	select {
	case acceptMsg = <-n.paxos.majorityAcceptReceived:
		t.Stop()

	case <-t.C:
		n.conf.PaxosID += n.conf.TotalPeers
	}

	return acceptMsg, nil
}

func (n *node) BroadcastTLC(step uint, acceptedValue types.PaxosValue) error {
	if updated := n.paxos.isTlcBroadcasted.CompareAndSetMax(1); !updated {
		// already broadcasted
		return nil
	}

	// build new block
	prevBlockHash := n.conf.Storage.GetBlockchainStore().Get(storage.LastBlockKey)
	if prevBlockHash == nil {
		// can only be initialised once, must be atomic
		prevBlockHash = make([]byte, 32)
	}
	block := types.BlockchainBlock{
		Index:    step,
		Hash:     nil,
		Value:    acceptedValue,
		PrevHash: prevBlockHash,
	}

	rawBlock := append([]byte(strconv.Itoa(int(block.Index))), []byte(block.Value.UniqID)...)
	rawBlock = append(rawBlock, []byte(block.Value.Filename)...)
	rawBlock = append(rawBlock, []byte(block.Value.Metahash)...)
	rawBlock = append(rawBlock, block.PrevHash...)

	h := crypto.SHA256.New()
	_, err := h.Write(rawBlock)
	if err != nil {
		return err
	}
	block.Hash = h.Sum(nil)

	tlcMsg := types.TLCMessage{
		Step:  step,
		Block: block,
	}
	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
	if err != nil {
		return err
	}
	err = n.Broadcast(transportMsg)
	if err != nil {
		return err
	}

	return nil
}

func (n *node) Resolve(name string) (metahash string) {
	value := n.conf.Storage.GetNamingStore().Get(name)
	if value == nil {
		return ""
	}
	return string(value)
}

func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.GetCatalog()
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.UpdateCatalog(key, peer)
}

// WaitForTimeout : blocking operation, wait until timeout, no reply returned
func (n *node) WaitForTimeout(dstAddr string, request types.SearchRequestMessage,
	timeout time.Duration) (err error) {
	msg, err := n.conf.MessageRegistry.MarshalMessage(request)
	if err != nil {
		return err
	}

	err = n.Unicast(dstAddr, msg)
	if err != nil {
		return err
	}

	t := time.NewTimer(timeout)
	select {
	case <-n.ctx.Done():
		return err
	case <-t.C:
		//log.Debug().Msgf("[WaitForTimeoutNoBackoff.timeout] RequestID: %s, dstAddr: %s, retry: %d",
		//	requestID, dstAddr)
	}
	return err
}

// WaitForReply : blocking operation, wait for searchReplyMessage and return reply
func (n *node) WaitForReply(ctx context.Context, dstAddr string, request types.SearchRequestMessage,
	timeout time.Duration) (err error) {
	msg, err := n.conf.MessageRegistry.MarshalMessage(request)
	if err != nil {
		return err
	}

	err = n.Unicast(dstAddr, msg)
	if err != nil {
		return err
	}
	t := time.NewTimer(timeout)
	select {
	case <-n.ctx.Done():
	case <-ctx.Done():
	case <-t.C:
		// reply == nil
		//log.Info().Msgf("[WaitForReplyNoBackoff.timeout] RequestID: %s, dstAddr: %s", requestID, dstAddr)
	}
	t.Stop()
	return err
}

func GetBucketDistribution(total int, numPeers int) (bucketList []int) {
	if numPeers == 0 {
		return []int{}
	}

	if numPeers > total {
		numPeers = total
	}
	baseBudget := total / numPeers
	reminder := total % numPeers

	for i := 0; i < numPeers; i++ {
		realBudget := baseBudget
		if reminder > 0 {
			realBudget++
			reminder--
		}
		bucketList = append(bucketList, realBudget)
	}
	return bucketList
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	budgetInt := int(budget)
	numPeers := n.peers.GetSize()
	budgetList := GetBucketDistribution(budgetInt, numPeers)
	peerTargets := n.peers.GetPeers(len(budgetList))
	var wg sync.WaitGroup // local wg, different from n.wg
	for i, dstAddr := range peerTargets {
		if i >= len(budgetList) {
			break
		}
		searchReq := types.SearchRequestMessage{
			RequestID: xid.New().String(),
			Origin:    n.conf.Socket.GetAddress(),
			Pattern:   reg.String(),
			Budget:    uint(budgetList[i]),
		}

		wg.Add(1)
		go func(dstAddr string, searchReq types.SearchRequestMessage) {
			defer wg.Done()
			err := n.WaitForTimeout(dstAddr, searchReq, timeout)
			if err != nil {
				log.Error().Msgf("[SearchAll.WaitForTimeoutNoBackoff] error: %s", err.Error())
			}
		}(dstAddr, searchReq)
	}

	wg.Wait() // wait until timeout before collect response

	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.Match([]byte(key)) {
			names = append(names, key)
		}
		return true
	})
	return names, err
}

func (n *node) IsFileFullyKnown(metahash string) bool {
	metafile := n.conf.Storage.GetDataBlobStore().Get(metahash)
	if metafile != nil {
		chunkKeys := strings.Split(string(metafile), peer.MetafileSep)

		for _, chunkHash := range chunkKeys {
			chunk := n.conf.Storage.GetDataBlobStore().Get(chunkHash)
			if chunk == nil {
				return false
			}
		}
		return true
	}
	return false
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	// first check locally
	name = n.findMatchLocally(pattern)
	if name != "" {
		return name, err
	}

	// search from peers
	numPeers := n.peers.GetSize()
	budget := int(conf.Initial)

	retry := 0
	for retry < int(conf.Retry) { // each try
		budgetList := GetBucketDistribution(budget, numPeers)
		peerTargets := n.peers.GetPeers(len(budgetList))
		requestID := xid.New().String()
		// local wg and local context
		var wg sync.WaitGroup
		searchCtx, searchCancel := context.WithCancel(context.Background())
		matchedName := n.searchResult.Add(requestID)
		defer n.searchResult.Remove(requestID)

		for i, dstAddr := range peerTargets {
			if i >= len(budgetList) {
				break
			}
			searchReq := types.SearchRequestMessage{
				RequestID: requestID,
				Origin:    n.conf.Socket.GetAddress(),
				Pattern:   pattern.String(),
				Budget:    uint(budgetList[i]),
			}

			wg.Add(1)
			go func(dstAddr string) {
				defer wg.Done()
				err := n.WaitForReply(searchCtx, dstAddr, searchReq, conf.Timeout)
				if err != nil {
					log.Error().Msgf("[SearchFirst.WaitForReply] error: %s", err.Error())
				}
			}(dstAddr)
		}

		go func() {
			wg.Wait()
			select {
			case matchedName <- "":
				//log.Debug().Msgf("[SearchFirst] matchedName is empty ")
			default:
			}
		}()

		res := <-matchedName
		name, ok := res.(string)
		if ok && name != "" {
			searchCancel()
			return name, err
		}

		retry++
		budget *= int(conf.Factor)
		searchCancel()
	}
	return name, err
}

func (n *node) findMatchLocally(pattern regexp.Regexp) (name string) {
	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if pattern.Match([]byte(key)) {
			if n.IsFileFullyKnown(string(val)) {
				name = key
				// stop searching
				return false
			}
		}
		return true
	})
	return name
}
