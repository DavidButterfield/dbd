package storage_node

import "errors"
import "fmt"
import "net"
import "net/rpc"

import .	"junk"

//XXX should have an init routine here to fill this in
var My_hostname string = ""	    // filled in by main()

/* Nodes that may appear in a storage routing tree, which distributes logical
 * blocks among mirrors, hosts, etc.
 */
type Node interface {
    Probe(Node_probe_args, *Node_probe_reply) (error)
    Read(Node_read_args, *Node_read_reply) (error)
    Write(Node_write_args, *Node_write_reply) (error)
    Sync(Node_sync_args, *Node_sync_reply) (error)
    Getsync(Node_getsync_args, *Node_getsync_reply) (error)
    String() (string)
}

/* RPC Client node */
type Rpcc_node struct {
    Name		string
    Down		Node	    // storage tree node being communicated to across RPC
    client		*rpc.Client
}

/* RPC Server node */
type Rpcs_node struct {
    Name		string
    Host		string	    // host name
    Port		string	    // TCP port number
    Rpc_name		string	    // RPC export name
    Down		Node	    // storage tree node being communicated to across RPC
    listener		net.Listener
}

/* NOP node */
type Nop_node struct {
    Name		string
    Host		string
    Nbyte		int
    Down		Node
}

/***** Ops *****/

type Node_probe_args struct {
    Host		string	    // current host in the config tree
}
type Node_probe_reply struct {
    Nbyte		int
    Ltime		int
}

func Node_probe(N Node, args Node_probe_args, reply *Node_probe_reply) (err error) {
    Chat("PROBE Node %v: %v", N, args)
    err = N.Probe(args, reply)
    if err != nil {
	if err != EHOSTMISMATCH {
	    Warn("probed: Node %v failed probe %v", N, err)
	} else {
	    Chat("probed: Node %v: EHOSTMISMATCH", N)
	}
    } else {
	Chat("PROBED Node %v: nbyte=%v ltime=%v", N, reply.Nbyte, reply.Ltime)
    }
    return
}

type Node_read_args struct {
    Ofs		    int
    Len		    int
    Slotnum	    int
}
type Node_read_reply struct {
    Data	    []byte
    Sync	    uint32
    Flags	    uint32
    Ltime	    int
}

type Node_write_args struct {
    Ofs		    int
    Slotnum	    int
    Data	    []byte
    Sync	    uint32
    Flag_bits_to_clr	uint32
    Flag_bits_to_set	uint32
}
type Node_write_reply struct {
}

// If ltime != 0, the other fields are ignored
type Node_sync_args struct {
    Slotnum	    int
    Sync            uint32
    Flag_bits_to_clr	uint32
    Flag_bits_to_set	uint32
    Ltime	    int
}
type Node_sync_reply struct {
}

type Node_getsync_args struct {
    Slotnum	    int
}
type Node_getsync_reply struct {
    Sync            uint32
    Flags	    uint32
    Ltime	    int
}

var EHOSTMISMATCH = errors.New("Host Mismatch")
var ERANGE = errors.New("Data Device Offset Out Of Range")
var ENOMIRROR = errors.New("No Mirrors Available")
var EMIRRORMISMATCH = errors.New("Mirrors Do Not Match")
var ENORPCSERVER = errors.New("No RPC Server")
var EBADSTORECONFIG = errors.New("Bad Store Configuration")

/********** NOP **********/

func (N *Nop_node) String() string {
    size := N.Nbyte
    return fmt.Sprintf("Nop_node %v: size=%v (%vM, %vG)",
		    N.Name, size, size/1024/1024, size/1024/1024/1024)
}

func (N *Nop_node) Probe(args Node_probe_args, reply *Node_probe_reply) error {
    err := Node_probe(N.Down, args, reply)
    N.Nbyte = reply.Nbyte
    return err
}

func (N *Nop_node) Read(args Node_read_args, reply *Node_read_reply) error {
    return N.Down.Read(args, reply)
}

func (N *Nop_node) Write(args Node_write_args, reply *Node_write_reply) error {
    return N.Down.Write(args, reply)
}

func (N *Nop_node) Sync(args Node_sync_args, reply *Node_sync_reply) error {
    return N.Down.Sync(args, reply)
}

func (N *Nop_node) Getsync(args Node_getsync_args, reply *Node_getsync_reply) error {
    return N.Down.Getsync(args, reply)
}

/********** RPC Server **********/

func (N *Rpcs_node) String() string {
    return fmt.Sprintf("Rpcs_node %v remote=%s:%s:%s listening=%v", N.Name, N.Host, N.Port, N.Rpc_name, N.listener!=nil)
}

func (N *Rpcs_node) Probe(args Node_probe_args, reply *Node_probe_reply) error {
    below_args := args

    // Replace blank hostnames in configuration with current hostname
    if N.Host == "" {
	below_args.Host = My_hostname
    } else {
	below_args.Host = N.Host
    }

    err := Node_probe(N.Down, below_args, reply)
    if err != nil {
	return err
    }

    if below_args.Host != My_hostname {
	Chat("Rpcs_node.Probe() SKIPS listening for node %v: hostname %v does not match My_hostname %v",
		    N, below_args.Host, My_hostname)
	return EHOSTMISMATCH
    }

    if N.listener != nil {
	return nil	// already listening
    }

    // Register the RPC server
    rpc.RegisterName(N.Rpc_name, N)
    N.listener, err = net.Listen("tcp", /* below_args.Host + */ ":" + N.Port)
    if err != nil {
	Ewarn(err, "net.Listen %v fails:", N)
	return err
    }

    Chat("RPC server on host %v port %v listens", below_args.Host, N.Port)
    go func () {
	Chat("Listener N={%v}", N)
	rpc.Accept(N.listener)	    // does not return until exiting
	N.listener = nil
	Chat("RPC server on host %v port %v ends", My_hostname, N.Port)
    }()

    return nil
}

func (N *Rpcs_node) Read(args Node_read_args, reply *Node_read_reply) error {
    return N.Down.Read(args, reply)
}

func (N *Rpcs_node) Write(args Node_write_args, reply *Node_write_reply) error {
    return N.Down.Write(args, reply)
}

func (N *Rpcs_node) Sync(args Node_sync_args, reply *Node_sync_reply) error {
    return N.Down.Sync(args, reply)
}

func (N *Rpcs_node) Getsync(args Node_getsync_args, reply *Node_getsync_reply) error {
    return N.Down.Getsync(args, reply)
}

/********** RPC Client **********/

func (N *Rpcc_node) String() string {
    return fmt.Sprintf("Rpcc_node %v (connects to %v) connected=%v", N.Name, N.Down.(*Rpcs_node).String(), N.client!=nil)
}

/* Connect to the RPC server */
func (N *Rpcc_node) start(args Node_probe_args, reply *Node_probe_reply) (err error) {
    N.client, err = rpc.Dial("tcp", N.Down.(*Rpcs_node).Host + ":" + N.Down.(*Rpcs_node).Port)
    if err != nil {
	Ewarn(err, "rpc.Dial could not connect from client %v to server %v: %v", N, N.Down, err)
    } else {
	Chat("rpc.Dial CONNECTED from to RPC server from client %v", N)
    }
    return err
}

func (N *Rpcc_node) Probe(args Node_probe_args, reply *Node_probe_reply) error {
    err := Node_probe(N.Down, args, reply)
    if err != nil && err != EHOSTMISMATCH {
	return err
    }

    if !IsLocalHost(args.Host) {
	Chat("Rpcc_node.Probe() SKIPS connecting for node %v because hostname %v does not match My_hostname %v",
		    N, args.Host, My_hostname)
	return EHOSTMISMATCH
    }

    if N.client == nil {
	err = N.start(args, reply)	// Connect to the RPC server
	if err != nil {
	    return err
	}
    } else {
	Chat("Rpcc_node %v already has a client", N)
    }

    return N.client.Call(N.Down.(*Rpcs_node).Rpc_name + ".Probe", args, reply)
}

func (N *Rpcc_node) callwrap(closure func () error) error {
    var err error
    if N.client == nil {
	var args Node_probe_args
	var reply Node_probe_reply
	args.Host = My_hostname
	err = N.start(args, &reply)
	if err != nil {
	    return err
	}
	Assert(N.client != nil)
    }

    err = closure()
    if err != nil {
	Warn("callwrap(%v) failed: %v", closure, err)
    }

    if err != nil && err.Error() == "connection is shut down" {	// RPC error
	Warn("callwrap(%v) clears client", closure)
	N.client = nil
    }

    return err
}

func (N *Rpcc_node) Read(args Node_read_args, reply *Node_read_reply) (err error) {
    return N.callwrap(func () error {
	return N.client.Call(N.Down.(*Rpcs_node).Rpc_name + ".Read", args, reply) } )
}

func (N *Rpcc_node) Write(args Node_write_args, reply *Node_write_reply) error {
    return N.callwrap(func () error {
	return N.client.Call(N.Down.(*Rpcs_node).Rpc_name + ".Write", args, reply) } )
}

func (N *Rpcc_node) Sync(args Node_sync_args, reply *Node_sync_reply) error {
    return N.callwrap(func () error {
	return N.client.Call(N.Down.(*Rpcs_node).Rpc_name + ".Sync", args, reply) } )
}

func (N *Rpcc_node) Getsync(args Node_getsync_args, reply *Node_getsync_reply) error {
    return N.callwrap(func () error {
	return N.client.Call(N.Down.(*Rpcs_node).Rpc_name + ".Getsync", args, reply) } )
}
