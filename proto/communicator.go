/* singularity - communicator.go

   This provides helper methods for sending and receiving protobufs on the
   ZMQ sockets.

*/

package singularity

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"time"
)

// ReadPb sends any protobuf along a ZMQ socket. This makes sure to bundle our
// type identifier at the beginning of the message.
func ReadPb(sock *zmq.Socket, timeout int) ([]byte, interface{}, error) {
	if timeout > 0 {
		if !WaitForRecv(sock, timeout) {
			return nil, nil, errors.New("recv timeout")
		}
	}

	rresp, err := sock.RecvMultipart(0)
	if err != nil {
		return nil, nil, err
	}

	// If we got a remote address, keep it.
	var remote []byte
	if len(rresp) > 1 {
		remote = rresp[0] // Remote address.
	}
	resp := rresp[len(rresp)-1]

	var pb interface{}
	switch resp[0] {
	case 1:
		pb = &Command{}
	case 2:
		pb = &StillAlive{}
	case 3:
		pb = &CommandFinished{}
	case 4:
		pb = &CommandOutput{}
	default:
		return nil, nil, errors.New(fmt.Sprintf("unknown packet type: %d", resp[0]))
	}

	err = proto.Unmarshal(resp[1:], pb.(proto.Message))
	if err != nil {
		return nil, nil, err
	}
	return remote, pb, nil
}

// WritePb sends any protobuf along a ZMQ socket. This makes sure to bundle our
// type identifier at the beginning of the message.
func WritePb(sock *zmq.Socket, remote []byte, pb interface{}) error {
	// TODO(mark): What happens if two goroutines end up in here at the same
	// time? Will ZMQ get angry; interleaved messages?
	var ptype byte = 0
	switch pb.(type) {
	case *Command:
		ptype = 1
	case *StillAlive:
		ptype = 2
	case *CommandFinished:
		ptype = 3
	case *CommandOutput:
		ptype = 4
	}
	if ptype == 0 {
		return errors.New(fmt.Sprintf("attempted to send unknown object: %v", pb))
	}

	buf, err := proto.Marshal(pb.(proto.Message))
	if err != nil {
		return err
	}

	// TODO: This probably copies the entire message again. It's totally a
	// premature optimization to fix that now... maybe later.
	tbuf := make([]byte, len(buf)+1)
	tbuf[0] = ptype
	copy(tbuf[1:], buf)

	if remote != nil {
		err = sock.Send(remote, zmq.SNDMORE)
		if err != nil {
			return err
		}

		err = sock.Send(make([]byte, 0), zmq.SNDMORE)
		if err != nil {
			return err
		}
	}

	err = sock.Send(tbuf, 0)
	if err != nil {
		return err
	}
	return nil
}

// WaitForRecv polls a ZMQ socket for a certain amount of time to see when it's
// ready for us to receive data. This returns when there is data (or, I think,
// when the socket is dead).
func WaitForRecv(sock *zmq.Socket, timeout int) bool {
	pi := make([]zmq.PollItem, 1)
	pi[0] = zmq.PollItem{Socket: sock, Events: zmq.POLLIN}
	zmq.Poll(pi, time.Duration(timeout)*time.Second)
	if pi[0].REvents == zmq.POLLIN {
		return true
	}
	return false
}

// WaitForSend polls a ZMQ socket until it's writable. After this returns true,
// you should be able to write to the socket immediately. Note that this often
// returns true while a socket is still being connected -- ZMQ likes to buffer.
func WaitForSend(sock *zmq.Socket, timeout int) bool {
	pi := make([]zmq.PollItem, 1)
	pi[0] = zmq.PollItem{Socket: sock, Events: zmq.POLLOUT}
	zmq.Poll(pi, time.Duration(timeout)*time.Second)
	if pi[0].REvents == zmq.POLLOUT {
		return true
	}
	return false
}
