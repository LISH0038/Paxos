package impl

import (
	"encoding/hex"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math/rand"
	"regexp"
	"strings"
)

func ExecEmptyMessage(_ types.Message, _ transport.Packet) error {
	return nil
}

// ExecChatMessage be called when a chat message is received.
func ExecChatMessage(msg types.Message, _ transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return fmt.Errorf("wrong type: %T", msg)
	}

	// do your stuff here with chatMsg...
	log.Info().Msgf("[ExecChatMessage] packet received: %s", chatMsg.String())
	return nil
}

// ExecRumorMessage be called when a rumor message is received.
func (n *node) ExecRumorMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	rumorMsg, _ := msg.(*types.RumorsMessage)
	//log.Info().Msgf("[ExecRumorMessage] packet received: %s", rumorMsg.String())

	// 2. process packets
	toRelay := false
	for _, rumor := range rumorMsg.Rumors {
		if n.vectorClock.CompareAndIncrement(rumor.Origin, int(rumor.Sequence)) {
			toRelay = true
			origin := rumor.Origin
			relay := pkt.Header.RelayedBy
			// update routing table
			// *** fixed bug***  hw2 looped topology  T_T
			// only update routing table if the rumor is new!!!
			if !n.peers.Contains(origin) {
				n.router.Update(origin, relay)
				// connect to direct neighbor
				if origin == relay {
					n.peers.Add([]string{relay})
				}
			}

			// store
			n.msgStore.Add(origin, rumor)
			// process expected packet
			// *** there was data race problem for "rumor.Origin"
			header := transport.NewHeader(rumor.Origin, pkt.Header.RelayedBy, pkt.Header.Destination, pkt.Header.TTL)
			msgPkt := transport.Packet{Header: &header, Msg: rumor.Msg}
			go func() {
				// msgPkt here is just a msg, not a rumor
				err := n.conf.MessageRegistry.ProcessPacket(msgPkt)
				if err != nil {
					log.Error().Msgf("[ExecRumorMessage.ProcessPacket] packet error: %s", err.Error())
				}
			}()
		}
	}

	// 1. send back ack
	// *** [fixed bug] *** send ack after updating self view, send to RelayedBy
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        types.StatusMessage(n.vectorClock.GetView())}
	_, err := n.EndToEndSend(n.conf.Socket.GetAddress(), pkt.Header.RelayedBy, ack)
	if err != nil {
		return fmt.Errorf("SendAck error: %w", err)
	}
	// log.Debug().Msgf("[ExecRumorMessage] ack from %s to %s", n.conf.Socket.GetAddress(), pkt.Header.Source)

	// 3. send to random neighbor
	if toRelay {
		_, dst, err := n.SendToRandomPeer(pkt.Header.Source, rumorMsg, []string{pkt.Header.Source, pkt.Header.RelayedBy})
		if dst == "" {
			//log.Debug().Msgf("[ExecRumorMessage.SendToRandomPeer] error: %s", err.Error())
		} else if err != nil {
			log.Error().Msgf("[ExecRumorMessage.SendToRandomPeer] error: %s", err.Error())
		}
	}
	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	statusMsg, _ := msg.(*types.StatusMessage)
	//log.Info().Msgf("[ExecStatusMessage] packet received: %s", statusMsg.String())

	// 1. compare the statusMsg with its own view
	ownView := n.vectorClock.GetView()
	remoteView := *statusMsg
	toSendStatusMsg := false
	toSendRumorsMsg := false
	missingMsgToSend := make([]types.Rumor, 0)
	for origin, remoteSeq := range remoteView {
		if remoteSeq > ownView[origin] {
			// case 1. The remote peer has Rumors that the peer P doesn’t have.
			toSendStatusMsg = true
		} else if remoteSeq < ownView[origin] {
			// case 2. The peer P has Rumors that the remote peer doesn’t have.
			toSendRumorsMsg = true
			missingMsgToSend = append(missingMsgToSend, n.msgStore.GetFromIndex(origin, int(remoteSeq+1))...)
		}
		delete(ownView, origin)
	}

	for origin := range ownView {
		// if land in this loop,
		// -> case 2. The peer P has Rumors that the remote peer doesn’t have.
		toSendRumorsMsg = true
		missingMsgToSend = append(missingMsgToSend, n.msgStore.GetFromIndex(origin, 1)...)
	}

	if toSendStatusMsg {
		_, err := n.EndToEndSend(n.conf.Socket.GetAddress(), pkt.Header.Source, types.StatusMessage(n.vectorClock.GetView()))
		if err != nil {
			log.Error().Msgf("[ExecStatusMessage.EndToEndSend] error: %s", err.Error())
		}
	}

	if toSendRumorsMsg {
		_, err := n.EndToEndSend(n.conf.Socket.GetAddress(), pkt.Header.Source, types.RumorsMessage{Rumors: missingMsgToSend})
		if err != nil {
			log.Error().Msgf("[ExecStatusMessage.EndToEndSend] error: %s", err.Error())
		}
	}

	if !toSendStatusMsg && !toSendRumorsMsg {
		// case 4. Both peers have the same view.
		if rand.Float64() < n.conf.ContinueMongering {
			avoidPeers := []string{pkt.Header.Source}
			sMsg := types.StatusMessage(n.vectorClock.GetView())
			_, dst, err := n.SendToRandomPeer(n.conf.Socket.GetAddress(), sMsg, avoidPeers)
			if dst == "" {
				//log.Debug().Msgf("[ExecStatusMessage.SendToRandomPeer] error: %s", err.Error())
			} else if err != nil {
				log.Error().Msgf("[ExecStatusMessage.SendToRandomPeer] error: %s", err.Error())
			}
		}
	}

	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	ackMsg, _ := msg.(*types.AckMessage)
	//log.Info().Msgf("[ExecAckMessage] packet received: %s", ackMsg.String())

	waitChan, exist := n.ackStore.Get(ackMsg.AckedPacketID)
	if exist {
		select {
		case waitChan <- true: // Put ack in the channel unless it is full
		default:
			log.Info().Msgf("[ExecAckMessage] channel is full: packet %s\n", pkt.Header.PacketID)
		}
	}

	statusMsg, err := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
	if err != nil {
		log.Error().Msgf("[ExecAckMessage] MarshalMessage: error %s", err.Error())
		return err
	}
	err = n.conf.MessageRegistry.ProcessPacket(transport.Packet{
		Header: pkt.Header,
		Msg:    &statusMsg,
	})
	if err != nil {
		log.Error().Msgf("[ExecAckMessage] ProcessPacket: error %s", err.Error())
		return err
	}

	return nil
}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	privateMsg, _ := msg.(*types.PrivateMessage)
	//log.Info().Msgf("[ExecPrivateMessage] packet received: %s", privateMsg.String())

	if _, exist := privateMsg.Recipients[n.conf.Socket.GetAddress()]; exist {
		err := n.conf.MessageRegistry.ProcessPacket(transport.Packet{
			Header: pkt.Header,
			Msg:    privateMsg.Msg,
		})
		if err != nil {
			log.Error().Msgf("[ExecPrivateMessage] ProcessPacket: error %s", err.Error())
			return err
		}
	}
	return nil
}

func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	dataRequestMsg, _ := msg.(*types.DataRequestMessage)
	value := n.conf.Storage.GetDataBlobStore().Get(dataRequestMsg.Key)
	dataReply := types.DataReplyMessage{
		RequestID: dataRequestMsg.RequestID,
		Key:       dataRequestMsg.Key,
		Value:     value,
	}
	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(dataReply)
	if err != nil {
		return err
	}

	err = n.Unicast(pkt.Header.Source, transportMsg)
	if err != nil {
		return fmt.Errorf("[ExecDataRequestMessage] error: %w", err)
	}
	return nil
}

func (n *node) ExecDataReplyMessage(msg types.Message, _ transport.Packet) error {
	dataReplyMsg, _ := msg.(*types.DataReplyMessage)
	waitChan, exist := n.waitingStore.Get(dataReplyMsg.RequestID)
	if exist {
		select {
		case waitChan <- &msg: // Put ack in the channel unless it is full
		default:
			log.Info().Msgf("[ExecDataReplyMessage] wait channel is full: id %s\n", dataReplyMsg.RequestID)
		}
	}
	return nil
}

func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	searchRequest, _ := msg.(*types.SearchRequestMessage)

	if value, exist := n.cache.Get(searchRequest.RequestID); exist {
		// cache stores (requestID, reply)
		//log.Debug().Msgf("cache hit: %s", searchRequest.RequestID)
		_, err := n.Send(pkt.Header.Source, searchRequest.Origin, value.(types.Message))
		if err != nil {
			log.Error().Msgf("[ExecSearchRequestMessage] cache send, EndToEndSend error: %s", err.Error())
		}
		return err
	}

	budget := int(searchRequest.Budget) - 1
	if budget != 0 {
		numPeers := n.peers.GetSize()
		peerTargets := n.peers.GetPeersAvoidAddr(numPeers, []string{pkt.Header.Source, searchRequest.Origin})
		budgetList := GetBucketDistribution(budget, len(peerTargets))
		// if len(peerTargets) == 0, will omit sending more search request
		for i, dstAddr := range peerTargets {
			if i >= len(budgetList) {
				break
			}
			searchRequest.Budget = uint(budgetList[i])

			go func(dstAddr string, searchRequest types.SearchRequestMessage) {
				_, err := n.EndToEndSend(n.conf.Socket.GetAddress(), dstAddr, searchRequest)
				if err != nil {
					log.Error().Msgf("[ExecSearchRequestMessage] Unicast error: %s", err.Error())
				}
			}(dstAddr, *searchRequest)
		}
	}

	// prepare for reply
	fileInfos := n.prepareFileInfo(searchRequest)

	searchReply := types.SearchReplyMessage{
		RequestID: searchRequest.RequestID,
		Responses: fileInfos,
	}

	_, err := n.Send(pkt.Header.Source, searchRequest.Origin, searchReply)
	if err != nil {
		log.Error().Msgf("[ExecSearchRequestMessage] EndToEndSend error: %s", err.Error())
	}

	// put into cache, cache will be invalidated once there is new upload/download
	n.cache.Set(searchRequest.RequestID, searchReply)
	return nil
}

func (n *node) prepareFileInfo(searchRequest *types.SearchRequestMessage) []types.FileInfo {
	fileInfos := make([]types.FileInfo, 0)
	reg, err := regexp.Compile(searchRequest.Pattern)
	if err != nil {
		log.Error().Msgf("[ExecSearchRequestMessage] Compile error: %s", err.Error())
		return fileInfos
	}

	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.Match([]byte(key)) {
			metafile := n.conf.Storage.GetDataBlobStore().Get(string(val))
			if metafile != nil {
				chunks := make([][]byte, 0)
				chunkHashs := strings.Split(string(metafile), peer.MetafileSep)
				for _, chunkHash := range chunkHashs {
					if n.conf.Storage.GetDataBlobStore().Get(chunkHash) != nil {
						chunks = append(chunks, []byte(chunkHash))
					} else {
						chunks = append(chunks, nil)
					}
				}
				fileInfos = append(fileInfos, types.FileInfo{
					Name:     key,
					Metahash: string(val),
					Chunks:   chunks,
				})
			}
		}
		return true
	})

	return fileInfos
}

func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	searchReply, _ := msg.(*types.SearchReplyMessage)
	for _, fileInfo := range searchReply.Responses {
		n.conf.Storage.GetNamingStore().Set(fileInfo.Name, []byte(fileInfo.Metahash))
		n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
		fullyKnownFound := true
		for _, chunk := range fileInfo.Chunks {
			if chunk != nil {
				n.UpdateCatalog(string(chunk), pkt.Header.Source)
			} else {
				fullyKnownFound = false
			}
		}
		if fullyKnownFound {
			resChan, exist := n.searchResult.Get(searchReply.RequestID)
			if exist {
				select {
				case resChan <- fileInfo.Name:
				default:
					//log.Debug().Msgf("[ExecSearchReplyMessage] matchedName is not empty ")
				}
			}
		}
	}
	return nil
}

// ExecPaxosPrepareMessage - for acceptor
func (n *node) ExecPaxosPrepareMessage(msg types.Message, _ transport.Packet) error {
	prepareMsg, _ := msg.(*types.PaxosPrepareMessage)

	n.paxos.step.RLock()
	step := n.paxos.step.val
	if prepareMsg.Step != step {
		n.paxos.step.RUnlock()
		return nil
	}

	// for new max id, update max seen id and prepare for promise
	updated := n.paxos.acceptorMaxSeenID.CompareAndSetMax(prepareMsg.ID)
	if !updated {
		n.paxos.step.RUnlock()
		return nil
	}
	// if (proposal_accepted == true) // was a proposal already accepted?
	//            respond: PROMISE(ID, accepted_ID, accepted_VALUE)
	//        else
	//            respond: PROMISE(ID)

	acceptedID, acceptedValue := n.paxos.acceptorAccepted.Get()
	promiseMsg := types.PaxosPromiseMessage{
		Step:          step,
		ID:            prepareMsg.ID,
		AcceptedID:    acceptedID,
		AcceptedValue: acceptedValue,
	}
	n.paxos.step.RUnlock()

	transportMsg, err := n.conf.MessageRegistry.MarshalMessage(promiseMsg)
	if err != nil {
		return err
	}
	privateMsg := types.PrivateMessage{
		Recipients: map[string]struct{}{prepareMsg.Source: {}},
		Msg:        &transportMsg,
	}
	newTransportMsg, err := n.conf.MessageRegistry.MarshalMessage(privateMsg)
	if err != nil {
		return err
	}
	return n.Broadcast(newTransportMsg)
}

// ExecPaxosPromiseMessage - proposer
func (n *node) ExecPaxosPromiseMessage(msg types.Message, _ transport.Packet) error {
	promiseMsg, _ := msg.(*types.PaxosPromiseMessage)

	// check validity
	if promiseMsg.ID != n.conf.PaxosID {
		return nil
	}
	// the whole exec is a transaction, must be executed in the same phase
	// to use RLock: enable multi-Msg processing
	n.paxos.phase.RLock()
	defer n.paxos.phase.RUnlock()
	n.paxos.step.RLock()
	defer n.paxos.step.RUnlock()
	if promiseMsg.Step != n.paxos.step.val || n.paxos.phase.val != 1 {
		return nil
	}

	// did I receive PROMISE responses from a majority of acceptors?
	//if yes
	//    do any responses contain accepted values (from other proposals)?
	//    if yes
	//        val = accepted_VALUE    // value from PROMISE message with the highest accepted ID
	//    if no
	//        val = VALUE     // we can use our proposed value
	//    send PROPOSE(ID, val) to at least a majority of acceptors --> go into phase 2
	votes := n.paxos.collectedPromise.IncrementAndGet()
	n.paxos.highestAccepted.CompareAndSet(promiseMsg.AcceptedID, promiseMsg.AcceptedValue)

	if votes >= uint(n.conf.PaxosThreshold(n.conf.TotalPeers)) && n.paxos.phase.val == 1 {
		select {
		case n.paxos.majorityPromiseReceived <- true:
		default:
			log.Debug().Msgf("[ExecPaxosPromiseMessage] majorityPromiseReceived is not empty ")
		}
	}
	return nil
}

// ExecPaxosProposeMessage - for acceptor
func (n *node) ExecPaxosProposeMessage(msg types.Message, _ transport.Packet) error {
	proposeMsg, _ := msg.(*types.PaxosProposeMessage)
	// only if (ID == max_id) // is the ID the largest I have seen so far?
	//    proposal_accepted = true     // note that we accepted a proposal
	//    accepted_ID = ID             // save the accepted proposal number
	//    accepted_VALUE = VALUE       // save the accepted proposal data
	//    respond: ACCEPTED(ID, VALUE) to the proposer and all learners

	n.paxos.step.RLock()
	step := n.paxos.step.val
	if proposeMsg.Step != step || proposeMsg.ID != n.paxos.acceptorMaxSeenID.Get() {
		n.paxos.step.RUnlock()
		return nil
	}

	n.paxos.acceptorAccepted.Set(proposeMsg.ID, &proposeMsg.Value)
	acceptMsg := types.PaxosAcceptMessage{
		Step:  step,
		ID:    proposeMsg.ID,
		Value: proposeMsg.Value,
	}
	n.paxos.step.RUnlock()

	transportAccept, err := n.conf.MessageRegistry.MarshalMessage(acceptMsg)
	if err != nil {
		return err
	}
	return n.Broadcast(transportAccept)
}

// ExecPaxosAcceptMessage - for proposer && learner
func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	acceptMsg, _ := msg.(*types.PaxosAcceptMessage)
	// If a majority of acceptors accept ID, value then consensus is reached.

	n.paxos.step.RLock()
	step := n.paxos.step.val
	if n.paxos.isConsensusReached.Get() == 1 || acceptMsg.Step != step {
		n.paxos.step.RUnlock()
		return nil
	}

	if !n.paxos.acceptedValueSet.Contains(acceptMsg.Value.UniqID, pkt.Header.Source) {
		n.paxos.acceptedValueSet.Set(acceptMsg.Value.UniqID, pkt.Header.Source)
	}

	if n.paxos.acceptedValueSet.GetSizeByKey(acceptMsg.Value.UniqID) >= n.conf.PaxosThreshold(n.conf.TotalPeers) {
		n.paxos.isConsensusReached.Set(1)
		n.paxos.step.RUnlock()

		// for learner
		err := n.BroadcastTLC(acceptMsg.Step, acceptMsg.Value)
		if err != nil {
			log.Error().Msgf("[BroadcastTLC] error: " + err.Error())
		}
		// for proposer
		if n.paxos.phase.Get() == 2 {
			select {
			case n.paxos.majorityAcceptReceived <- acceptMsg:
			default:
				log.Debug().Msgf("[ExecPaxosAcceptMessage] proposer is not listening on majorityAcceptReceived")
			}
		}
	} else {
		n.paxos.step.RUnlock()
	}
	return nil
}

func (n *node) ExecTLCMessage(msg types.Message, _ transport.Packet) error {
	tlcMsg, _ := msg.(*types.TLCMessage)

	// ignore past steps
	if tlcMsg.Step < n.paxos.step.Get() {
		return nil
	}

	// store for now and future
	count := n.paxos.tlcStore.Add(tlcMsg.Step, tlcMsg)
	if count > n.conf.PaxosThreshold(n.conf.TotalPeers) {
		return nil
	}
	step := n.paxos.step.Get()
	tclMsgToProcess := tlcMsg
	for {
		if tclMsgToProcess.Step != step || count < n.conf.PaxosThreshold(n.conf.TotalPeers) {
			break
		}
		// process tlc message which reached threshold
		// operations to be idempotent (possible duplicated operations)
		buf, err := tclMsgToProcess.Block.Marshal()
		if err != nil {
			return err
		}

		n.conf.Storage.GetBlockchainStore().Set(hex.EncodeToString(tclMsgToProcess.Block.Hash), buf)
		n.conf.Storage.GetBlockchainStore().Set(storage.LastBlockKey, tclMsgToProcess.Block.Hash)
		n.conf.Storage.GetNamingStore().Set(tclMsgToProcess.Block.Value.Filename,
			[]byte(tclMsgToProcess.Block.Value.Metahash))

		if step == tlcMsg.Step {
			n.BroadcastTLCWithMsg(tlcMsg)
		}

		step = n.paxos.step.IncrementAndGet()
		count, tclMsgToProcess = n.paxos.tlcStore.Get(step)
		//log.Debug().Msgf("[ExecTLCMessage] src: %s, new step: %d", n.conf.Socket.GetAddress(), step)
	}

	if step > tlcMsg.Step { // has progressed
		// end of a step for acceptor, reinitialise
		n.paxos.step.Lock()
		n.paxos.acceptorMaxSeenID.Set(0)
		n.paxos.acceptorAccepted.Set(0, nil)
		n.paxos.isConsensusReached.Set(0)
		n.paxos.isTlcBroadcasted.Set(0)
		n.paxos.highestAccepted.Set(0, nil)
		n.paxos.step.Unlock()
		// end of a step for proposer
		n.paxos.ProposerExit()
	}
	return nil
}

func (n *node) BroadcastTLCWithMsg(tlcMsg *types.TLCMessage) {
	n.paxos.isTlcBroadcasted.Lock()
	defer n.paxos.isTlcBroadcasted.Unlock()
	if n.paxos.isTlcBroadcasted.val == 0 {
		transportTlcMsg, err := n.conf.MessageRegistry.MarshalMessage(tlcMsg)
		if err != nil {
			log.Error().Msgf("[TlcBroadcast] err: %s", err.Error())
		}
		err = n.Broadcast(transportTlcMsg)
		if err != nil {
			log.Error().Msgf("[TlcBroadcast] err: %s", err.Error())
		}
		n.paxos.isTlcBroadcasted.val = 1 // will go to next step and reset to 0
	}
}
