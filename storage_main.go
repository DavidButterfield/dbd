package main	    // Mirrored Distributed Block Device

import "fmt"
import "os"
import "os/signal"
import "strings"
import "time"
import "unsafe"

import mirror	"storage_node_mirror"	// mirror node impl
import store	"storage_node_store"	// store node impl
import node	"storage_node"		// generic impl
import logger	"pa_logger"		// persistent array logger
import .	"junk"			// misc helper functions

// #cgo CFLAGS: -I./../tcmu-runner -I./../tcmu-runner/ccan
//
// #cgo LDFLAGS: ./../tcmu-runner/libtcmulib_static.a
// #cgo LDFLAGS: ./../tcmu-runner/libtcmu_static.a
// #cgo LDFLAGS: -L/usr/lib/x86_64-linux-gnu -lkmod
// #cgo LDFLAGS: -lgio-2.0 -lgobject-2.0 -lglib-2.0
// #cgo LDFLAGS: -lnl-3 -lnl-genl-3 -lpthread -ldl
//
// #include <sys/types.h>
// #include "tcmu-runner.h"
// int tcmu_main_thunk(void);
import "C"

const SECTOR_SIZE = 4096	// min I/O alignment (offset and length)
const BLOCK_SIZE = 256*1024	// mirror metadata entry granularity
const MD_FILE_OFS = 4096	// offset of metadata in metadata os.File

/* Static storage routing configuration tree */

/* Left side of mirrored volume */
var store_0 = store.Store_node {
    Store_sync:		0x0001,			//XXX
    Name:		"store_0",
    Data_name:		"/tmp/GB_0",
    Meta_name:		"/tmp/MD_0",
    Bytes_per_slot:	BLOCK_SIZE,		//XXX
    Md_ofs:		MD_FILE_OFS	}

/* Right side of mirrored volume */
var store_1 = store.Store_node {
    Store_sync:		0x0002,			//XXX
    Name:		"store_1",
    Data_name:		"/tmp/GB_1",
    Meta_name:		"/tmp/MD_1",
    Bytes_per_slot:	BLOCK_SIZE,		//XXX
    Md_ofs:		MD_FILE_OFS	}

/* For --meta-init */
var store_list = []*store.Store_node { &store_0, &store_1 }

/* RPC Server for Mirror Right */
var rpcs_1 = node.Rpcs_node {
    Name:		"rpcs_1",
    Rpc_name:		"Store",
    Host:		"",
    Port:		"1234",
    Down:		node.Node(&store_1)	}

/* RPC Client for Mirror Right */
var rpcc_1 = node.Rpcc_node {
    Name:		"rpcc_1",
    Down:		node.Node(&rpcs_1)	}

/* Mirror Splitter (Duplicator) */
var dup_ = mirror.Dup_node {
    Name:		"dup_",
    Bytes_per_slot:	BLOCK_SIZE,
    Down:		[]node.Node { &store_0, &rpcc_1 } }

/* Interface for external requests */
var entry_node = node.Nop_node {
    Name:		"entry",
    Host:		"",
    Down:		node.Node(&dup_)	}

/* Return the list of nodes below the given node */
func nodes_below(N node.Node) []node.Node {
    switch T := N.(type) {
    case *node.Rpcs_node:
	return []node.Node{T.Down}
    case *mirror.Dup_node:
	return T.Down
    case *node.Rpcc_node:
	return []node.Node{T.Down}
    case *store.Store_node:
	return make([]node.Node, 0)
    case *node.Nop_node:
	return []node.Node{T.Down}
    default:
	fmt.Printf("\n\n%v\n", N)
	panic("****** BAD node.NODE TYPE for node_below()")
    }
}

/* Return a string representing the storage routing configuration tree */
const SPACES = "                                                "
const INDENT = 4	// indent spaces for levels of node tree
func Tree_string(N node.Node, indent int) string {
    var str string
    for _, n := range nodes_below(N) {
	if str == "" {
	    str = fmt.Sprintf("%s", Tree_string(n, indent+INDENT))
	} else {
	    str = fmt.Sprintf("%s\n%s", str, Tree_string(n, indent+INDENT))
	}
    }
    if str == "" {
	return fmt.Sprintf("%s%v", SPACES[0:indent], N)
    } else {
	return fmt.Sprintf("%s%v\n%s", SPACES[0:indent], N, str)
    }
}

/********** Interface for Requests from dbd tcmu handler **********/

//extern ssize_t go_dbd_probe(void *);
//export go_dbd_probe
func go_dbd_probe(cfg *byte) int {
    Notify("go_dbd_probe start")
    N := &entry_node
    var args node.Node_probe_args
    var reply node.Node_probe_reply
    args.Host = node.My_hostname
    err := node.Node_probe(N, args, &reply)
    if err != nil {
	return -1
    }
    N.Nbyte = reply.Nbyte
    Notify("go_dbd_probe returns %v", N.Nbyte)
    return N.Nbyte
}

//extern ssize_t go_dbd_read(struct iovec *, int, size_t, off_t);
//export go_dbd_read
func go_dbd_read(iov *C.struct_iovec, niov C.size_t, size C.size_t, ofs C.off_t) C.ssize_t {
    N := &entry_node
    Assert_eq(int(size)%SECTOR_SIZE, 0)
    Assert_eq(int(ofs)%SECTOR_SIZE, 0)

    var args node.Node_read_args
    var reply node.Node_read_reply
    args.Ofs = int(ofs)
    args.Len = int(size)

    err := N.Read(args, &reply)
    if err != nil {
	Ewarn(err, "go_dbd_read size=%v ofs=%v FAILS EIO", size, ofs)
	return -5
    }

    len := len(reply.Data)
    buf := unsafe.Pointer(&reply.Data[0])
    C.tcmu_memcpy_into_iovec(iov, C.size_t(niov), buf, C.size_t(len))

    Chat("go_dbd_read size=%v/%v ofs=%v OK", len, size, ofs)
    return C.ssize_t(len)
}

//extern ssize_t go_dbd_write(struct iovec *, int, size_t, off_t);
//export go_dbd_write
func go_dbd_write(iov *C.struct_iovec, niov C.size_t, size C.size_t, ofs C.off_t) C.ssize_t {
    N := &entry_node
    Assert_eq(int(size)%SECTOR_SIZE, 0)
    Assert_eq(int(ofs)%SECTOR_SIZE, 0)

    var buf = make([]byte, size)
    C.tcmu_memcpy_from_iovec(unsafe.Pointer(&buf[0]), C.size_t(size), iov, C.size_t(niov));

    var args node.Node_write_args
    var reply node.Node_write_reply
    args.Ofs = int(ofs)

    buf_ofs := 0
    count := int((ofs/BLOCK_SIZE + 1) * BLOCK_SIZE - ofs)
    for rem := int(size); rem > 0; {
	if count > rem {
	    count = rem
	}
	args.Data = buf[buf_ofs:buf_ofs+count]
	err := N.Write(args, &reply)
	if err != nil {
	    Ewarn(err, "go_dbd_write size=%v ofs=%v FAILS EIO", size, ofs)
	    return -5
	}
	rem -= count
	args.Ofs += count
	buf_ofs += count
	count = BLOCK_SIZE
    }

    Chat("go_dbd_write size=%v ofs=%v OK", size, ofs)
    return C.ssize_t(size)
}

/* Parse arguments
 * Optionally initialize the metadata files
 * Set up the storage routing tree
 * Call tcmu_main() to begin handling I/O
 */
func main() {
    var do_init_meta, do_dump, do_frob, do_config bool
    var ok bool = true

    for i := 1; i < len(os.Args); i++ {
	arg := os.Args[i]
	if arg == "-h" || arg == "--help" {
	    ok = false
	} else if arg == "-v" || arg == "--verbose" {
	    logger.CHATTY = true
	    CHATTY = true
	} else if arg == "-vv" {
	    logger.CHATTY = true
	    CHATTY = true
	    VERBOSE = true
	} else if arg == "-V" || arg == "--Verbose" {
	    logger.CHATTY = true
	    logger.VERBOSE = true

	} else if arg == "--SKIP_FLUSH_DATA" {
	    store.SKIP_FLUSH_DATA = true	// Skip os.File.Sync() of data device
	} else if arg == "--SKIP_FLUSH_META" {
	    logger.SKIP_FLUSH_META = true	// Skip os.File.Sync() of metadata file
	} else if arg == "--SKIP_COPY_DATA" {
	    mirror.SKIP_COPY_DATA = true	// Skip copy of data when updating mirror
	} else if arg == "--SKIP_SYNC" {
	    mirror.SKIP_SYNC = true		// Mirrors skip setting sync on writes
	} else if arg == "--SKIP_WAIT" {
	    store.SKIP_WAIT = true		// Skip waiting for MD write completion and sync

	} else if arg == "--config" {
	    do_config = true
	} else if arg == "--frob" {
	    do_frob = true
	} else if arg == "--dump" {
	    do_dump = true
	} else if arg == "--meta-init" {
	    do_init_meta = true

	} else if arg == "--front" {
	    if i+1 >= len(os.Args) || os.Args[i+1][0] == '-' {
		fmt.Printf("Missing hostname argument for --front\n")
		ok = false
		continue
	    }
	    entry_node.Host = os.Args[i+1]
	    i++
	} else if arg == "--back" {
	    if i+1 >= len(os.Args) || os.Args[i+1][0] == '-' {
		fmt.Printf("Missing hostname argument for --back\n")
		ok = false
		continue
	    }
	    rpcs_1.Host = os.Args[i+1]
	    i++
	} else {
	    ok = false
	    fmt.Printf("Unknown argument '%s'\n", arg)
	}
    }

    if !ok {
	//XXX
	fmt.Printf("Usage: %s [ --front host ] [ --back host ] [ --meta-init ] [ -v ] [ -V ]\n", os.Args[0])
	os.Exit(1)
    }

    // Get the local hostname to compare with args.host in Probe() functions
    var err error
    node.My_hostname, err = os.Hostname()
    if err != nil {
	fmt.Printf("Hostname(): %v\n", err)
	panic("Cannot get Hostname()")
    }

    if len(node.My_hostname) == 0 {
	node.My_hostname = "localhost"
    } else if !strings.ContainsAny(node.My_hostname, ".") {
	// If it's just a machine name, add ".local" to the end of it
	node.My_hostname += ".local"
    }

    Notify("node.My_hostname='%v'", node.My_hostname)

    if do_init_meta {
	Verbose("\n*** --meta-init begins")
	for _, S := range store_list {
	    _, err = os.Stat(S.Data_name)
	    if err != nil {
		fmt.Printf("do_init_meta skipped because %v %v\n", S.Data_name, err)
		continue
	    }

	    err = store.Store_init_meta(S.Data_name, S.Meta_name, S.Bytes_per_slot, S.Md_ofs, &store.My_pa_cfg, S.Store_sync)
	    if err != nil {
		Warn("Failed to init store %v metadata: %v", S, err)
		continue
	    }

	    Chat("Init done store metadata %v", S)
	}
	Verbose("\n*** --meta-init done")

	if do_frob || do_dump || do_config {
	    Warn("Other actions ignored with --meta-init")
	}
	os.Exit(0)
    }

    var probe_args node.Node_probe_args
    var probe_reply node.Node_probe_reply
    probe_args.Host = entry_node.Host

    Chat("*** node_probe(&entry_node) begins")

    err = node.Node_probe(&entry_node, probe_args, &probe_reply)
    if err != nil && err != node.EHOSTMISMATCH {
	Ewarn(err, "main: node_probe failed")
	os.Exit(4)
    }

    Chat("*** node_probe(&entry_node) done")

    Notify("Sleep(1)")
    time.Sleep(1*time.Second)
    Notify("Awake(1)")

    if do_config {
	Notify("Configuration:\n%s\n", Tree_string(&entry_node, 4))
    }

    if do_frob {
	Notify("frob take down mirror 1")
	dup_.Mirror_down(1)
	Notify("frob done")
    }

    Notify("Sleep(1)")
    time.Sleep(1*time.Second)
    Notify("Awake(1)")

    // Catch SIGINT
    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, os.Interrupt)
    go func () {
	<-sigchan
	//XXXXX PA.flush_wait()
	//XXXXX PA.close()
	Chat("\n\n*** SIGNAL FIRED ***\n\n")
	// os.Exit(0)
    }()

    Notify("Front-end volume nbyte: %v", probe_reply.Nbyte)

    C.tcmu_main_thunk()  // call tcmu_main()

    fmt.Printf("tcmu_main returned!\n")

    if do_dump {
	Notify("Configuration:\n%s\n", Tree_string(&entry_node, 4))
	store_0.Pa.Dump()
    }
}
