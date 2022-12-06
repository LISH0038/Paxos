package udp

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	con, err := net.ListenPacket("udp", address)
	if err != nil {
		return &Socket{}, err
	}

	return &Socket{conn: con}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	conn    net.PacketConn
	inLock  sync.RWMutex
	outLock sync.RWMutex
	ins     []transport.Packet
	outs    []transport.Packet
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.conn.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	if dest == "" {
		return errors.New("[transport.udp.Socket.Send]: dest is empty")
	}

	dstAddr, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return err
	}

	bytes, err := pkt.Marshal()
	if err != nil {
		return err
	}

	t := time.Time{}
	if timeout != 0 {
		t = time.Now().Add(timeout)
	}

	err = s.conn.SetWriteDeadline(t)
	if err != nil {
		return err
	}

	writtenBytes, err := s.conn.WriteTo(bytes, dstAddr)
	if err != nil {
		if os.IsTimeout(err) {
			return transport.TimeoutError(timeout)
		}
		return err
	}

	if writtenBytes < len(bytes) {
		return errors.New("[transport.udp.Socket.Send]: bytes to write exceeds buffer size ")
	}

	s.outLock.Lock()
	defer s.outLock.Unlock()
	s.outs = append(s.outs, pkt.Copy())

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	t := time.Time{}
	if timeout != 0 {
		t = time.Now().Add(timeout)
	}

	err := s.conn.SetReadDeadline(t)
	if err != nil {
		return transport.Packet{}, err
	}

	buffer := make([]byte, bufSize)

	bytesRead, _, err := s.conn.ReadFrom(buffer)
	if err != nil {
		if os.IsTimeout(err) {
			return transport.Packet{}, transport.TimeoutError(timeout)
		}
		return transport.Packet{}, err
	}

	var pkt transport.Packet
	err = pkt.Unmarshal(buffer[0:bytesRead])
	if err != nil {
		return transport.Packet{}, err
	}

	s.inLock.Lock()
	defer s.inLock.Unlock()
	s.ins = append(s.ins, pkt.Copy())

	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.conn.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	s.inLock.RLock()
	defer s.inLock.RUnlock()
	return s.getAll(s.ins)
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	s.outLock.RLock()
	defer s.outLock.RUnlock()
	return s.getAll(s.outs)
}

func (s *Socket) getAll(data []transport.Packet) []transport.Packet {
	res := make([]transport.Packet, len(data))

	for i, pkt := range data {
		res[i] = pkt.Copy()
	}

	return res
}
