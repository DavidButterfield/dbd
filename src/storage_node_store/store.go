package storage_node_store

import "fmt"
import "os"
import "unsafe"

import node	"storage_node"
import logger	"pa_logger"
import .	"junk"

// Behavior breaks recovery in case of failure
var SKIP_FLUSH_DATA = false	// skip os.File.Sync() data after write
var SKIP_WAIT = false		// skip waiting for metadata writes/sync to complete

/********** Storage Device with Persistent Metadata **********/

/* Vitals for this example Persistent Array */
const my_pa_magic = 0x4DABC0DE
const my_pa_version = 1

var My_pa_cfg = logger.Pa_log_config_t{
    Magic:		    my_pa_magic,	// persistent array magic number
    Version:		    my_pa_version,	// persistent array structure version number
    Bytes_per_sector:	    512,		// e.g. 512 or 4096
    Slot_start_ofs:	    4096,		// ofs of slots from start of pa hdr in md_file
    Bytes_hdr_per_sector:   32,			// header at start of each slot sector
    Bytes_per_slot:	    32,			// size of each array slot on disk
    Rslots_max_per_sector:  8,			// refresh slots per sector
    Xslots_max_per_sector:  7,			// transaction slots per sector
    // below are set in code
    Slot_max:		    0,			// slot numbers must be less than this
    Bytes_in_slot_area:	    0,			// length of ONDISK slots area (not hdr)
}

type Store_node struct {
    Name		string
    Data_name		string
    Meta_name		string
    Md_ofs		int
    Bytes_per_slot	int
    Pa			*Pa_array
    nbyte		int
    dev			*os.File
    ltime		int
    Store_sync		uint32
}

func Store_init_meta(dev_name, md_name string, block_size int, md_ofs int, cfg *logger.Pa_log_config_t, my_sync uint32) error {
    // Open the data device
    dev_file, err := os.OpenFile(dev_name, os.O_RDONLY, 0600)
    if err != nil {
	Ewarn(err, "Store_init_meta: os.Open of %v failed", dev_name)
	return err
    }

    // Calculate the number of blocks to be covered with metadata
    st, err := dev_file.Stat()
    if err != nil {
	Ewarn(err, "Stat of device %v failed", dev_name)
	return err
    }
    Chat("\nDevice %v: Name=%v Size=%v Mode=%v",
		dev_name, st.Name(), st.Size(), st.Mode())

    cfg.Slot_max = uint64(Div_ceil(st.Size(), block_size))

    cfg.Bytes_in_slot_area = uint64(logger.Pa_log_write_bytes +
	Div_ceil(cfg.Slot_max, cfg.Rslots_max_per_sector) * int(cfg.Bytes_per_sector))

    Chat("Device %v: size=%v Slot_max=%v Bytes_in_slot_area=%v",
		dev_name, st.Size(), cfg.Slot_max, cfg.Bytes_in_slot_area)

    // Create the metadata file
    md_file, err := os.OpenFile(md_name, os.O_RDWR /* XXX |os.O_CREATE*/, 0600)
    if err != nil {
	Ewarn(err, "os.OpenFile of persistent array file %v failed", md_name)
	return err
    }

    // Create a Persistent Array to hold the metadata
    PA := new(Pa_array)
    PA.Slot = make([]My_pa_data, cfg.Slot_max)

    // Initialize the INCORE array with sync clear for every block
    for i := range PA.Slot {
	PA.Slot[i].Sync = uint16(my_sync)
	PA.Slot[i].Flags = uint16(0xeeee)
    }

    // Create and initialize the persistent array ONDISK --
    // The INCORE slots must be initialized first (above).
    err = PA.create(md_file, md_ofs, cfg)
    if err != nil {
	Ewarn(err, "PA.create() failed: config={%v]", cfg)
	return err
    }

    dev_file.Close()
    Chat("PA.create() succeeded")
    return nil
}

func store_start(S *Store_node, flags int, perm os.FileMode) error {
    // Open the device for the data
    var err error
    S.dev, err = os.OpenFile(S.Data_name, flags, perm)
    if err != nil {
	Ewarn(err, "os.OpenFile of data device %v failed", S.Data_name)
	return err
    }

    // Open the file for the metadata
    md_openflags := os.O_RDWR /* | os.O_CREATE */
    md_file, err := os.OpenFile(S.Meta_name, md_openflags, 0600)
    if err != nil {
	Ewarn(err, "os.OpenFile of metadata persistent array file %v failed", S.Meta_name)
	return err
    }

    // Open the logger and read the persistent array header.
    // Read array contents from ONDISK slot area into the INCORE array.
    Chat("store_start(%v) opens persistent array logger", S)
    Assert(S.Pa == nil, fmt.Sprintf("%v", S))
    S.Pa = new(Pa_array)
    config, err := S.Pa.open(md_file, S.Md_ofs)
    if err != nil {
	Ewarn(err, "PA.open(%v) failed", S.Meta_name)
	return err
    }
    Chat("PA.open() succeeded")

    My_pa_cfg.Slot_max = config.Slot_max
    My_pa_cfg.Bytes_in_slot_area = config.Bytes_in_slot_area

    Assert_eq(len(S.Pa.Slot), int(config.Slot_max))
    Chat(">>>>>>>>>> slot_max=%v", config.Slot_max)

    // Sanity checks
    if config.Magic != my_pa_magic {
	Ewarn(nil, "Mismatched Metadata Magic: expect=%v ONDISK=%v", my_pa_magic, config.Magic)
	return node.EBADSTORECONFIG
    }
    if config.Version != my_pa_version {
	Ewarn(nil, "Mismatched Metadata version: expect=%v ONDISK=%v", my_pa_version, config.Version)
	return node.EBADSTORECONFIG
    }
    if *config != My_pa_cfg {
	fmt.Printf("%v\n\n%v\n\n", *config, My_pa_cfg)
	Ewarn(nil, "PA.Open returned wrong config")
	return node.EBADSTORECONFIG
    }

    // Figure the expected metadata size from the device size
    st, err := S.dev.Stat()
    if err != nil {
	Ewarn(err, "Stat of device %s failed", S.Data_name)
	return err
    }
    Chat("Device %v: Name=%v Size=%v Mode=%v", S, st.Name(), st.Size(), st.Mode())

    S.nbyte = int(st.Size())
    nblock := Div_ceil(S.nbyte, S.Bytes_per_slot)
    if nblock != int(S.Pa.log.Cfg.Slot_max) {
	Warn("MISMATCH device blocks=%v metadata nslot=%v", nblock, S.Pa.log.Cfg.Slot_max)
    }

    S.ltime = S.Pa.ltime()
    return nil
}

func (N *Store_node) String() string {
    return fmt.Sprintf("Store_node %v data=%s meta=%s nbyte=%v ltime=%v", N.Name, N.Data_name, N.Meta_name, N.nbyte, N.ltime)
}

func (S *Store_node) Probe(args node.Node_probe_args, reply *node.Node_probe_reply) (err error) {
    if args.Host != node.My_hostname {
	Chat("store.Probe() node %v is for hostname %v not %v", S, args.Host, node.My_hostname)
	return node.EHOSTMISMATCH
    }

    if S.Pa == nil {
	err = store_start(S, os.O_RDWR, 0600)
	if err != nil {
	    Ewarn(err, "Store_node.Probe(%v) failed:", S)
	    return err
	}
    }

    reply.Nbyte = S.nbyte
    reply.Ltime = S.ltime

    return
}

/* Read from composite device -- straightforward to underlying data device */
func (S *Store_node) Read(args node.Node_read_args, reply *node.Node_read_reply) error {
    Assertz(args.Ofs % 4096)
    Assertz(args.Len % 4096)
    if args.Ofs >= S.nbyte {
	return node.ERANGE
    }
    if args.Ofs + args.Len > S.nbyte {
	return node.ERANGE
    }

    buf := make([]byte, args.Len)
    ofs := int64(args.Ofs)

    // Read the data from the device
    count, err := S.dev.ReadAt(buf, ofs)
    if err != nil || count != args.Len {
	Ewarn(err, "S.dev.ReadAt(%v, len=%v/%v, ofs=%v) failed", S.dev.Name, count, len(buf), ofs)
	return err
    }

    reply.Data = buf[0:count]

    md, err := S.Pa.get(args.Slotnum)
    if err != nil {
	Ewarn(err, "Failed to get metadata for slot %v", args.Slotnum)
    } else {
	reply.Sync = uint32(md.Sync)
	reply.Flags = uint32(md.Flags)
	reply.Ltime = S.ltime
    }

    Verbose("STORE {%v} READS %v bytes at offset %v (%v)", S, count, ofs, err)
    return err
}

/* Write to composite device --
 * update metadata to empty sync;
 * then write to data device;
 * then update metadata to specified sync
 */
func (S *Store_node) Write(args node.Node_write_args, reply *node.Node_write_reply) error {
    Assertz(args.Ofs % 4096)
    Assertz(len(args.Data) % 4096)
    if args.Ofs >= S.nbyte {
	return node.ERANGE
    }
    if args.Ofs + len(args.Data) > S.nbyte {
	return node.ERANGE
    }

    // Fetch the metadata for the block we are about to write
    md, err := S.Pa.get(args.Slotnum)
    if err != nil {
	Ewarn(err, "Could not get metadata for slot %v being written", args.Slotnum)
	return err
    }

    // Clear the sync and record it in the log
    if md.Sync != 0 {
	md.Sync = 0
	if !SKIP_WAIT {
	    err = S.Pa.set_wait(args.Slotnum, md)
	} else {
	    err = S.Pa.set(args.Slotnum, md, nil)
	}
	if err != nil {
	    Ewarn(err, "could not clear persistent array sync for S.Write block %v", args.Slotnum)
	}
    }

    // Write the block to the data file
    ofs := args.Ofs
    count, err := S.dev.WriteAt(args.Data, int64(ofs))
    if err != nil {
	Ewarn(err, "S.dev.WriteAt(%v, len=%v/%v, ofs=%v) failed", S.dev.Name, count, len(args.Data), ofs)
	return err
    }
    Verbose("STORE {%v} WRITES %v bytes at offset %v", S, len(args.Data), ofs)

    if !SKIP_FLUSH_DATA {
	// Sync the data file
	err = S.dev.Sync()
	if err != nil {
	    Ewarn(err, "S.dev.Sync(%v) failed", S.dev.Name)
	    return err
	}
    }

    // Record the new sync
    md.Sync = uint16(args.Sync)
    md.Flags &^= uint16(args.Flag_bits_to_clr)
    md.Flags |= uint16(args.Flag_bits_to_set)

    if !SKIP_WAIT {
	err = S.Pa.set_wait(args.Slotnum, md)
    } else {
	err = S.Pa.set(args.Slotnum, md, nil)
    }

    if err != nil {
	Ewarn(err, "could not set persistent array for S.Write Slotnum=%v to md={%v}", args.Slotnum, md)
    }

    return err
}

/* Phase II: Update the sync metadata to reflect successful write replies from the mirrors.
 *
 * This function is also used to update the ltime --
 *	if args.Ltime is nonzero then the other args are ignored
 */
func (S *Store_node) Sync(args node.Node_sync_args, reply *node.Node_sync_reply) error {
    if args.Ltime > 0 {
	if S.ltime > args.Ltime {
	    panic(fmt.Sprintf("attempt to decrease store ltime from %v to %v", S.ltime, args.Ltime))
	}
	if S.ltime == args.Ltime {
	    fmt.Printf("unexpected ltime sync to current ltime %v\n", S.ltime)
	}
	S.ltime = args.Ltime
	S.Pa.ltime_set(S.ltime)
	return nil
    }

    // Fetch the current metadata
    md, err := S.Pa.get(args.Slotnum)
    if err != nil {
	Ewarn(err, "could not get current metadata for store %v block %v\n", S, args.Slotnum)
    }

    xmd := md

    // Update the sync in the persistent array
    md.Sync = uint16(args.Sync)
    md.Flags &^= uint16(args.Flag_bits_to_clr)
    md.Flags |= uint16(args.Flag_bits_to_set)

    if !SKIP_WAIT {
	err = S.Pa.set_wait(args.Slotnum, md)
    } else {
	err = S.Pa.set(args.Slotnum, md, nil)
    }

    if err != nil {
	Ewarn(err, "could not set persistent array for S.Meta_sync_update Slotnum=%v to md={%v}", args.Slotnum, md)
    }

    Verbose("STORE %v block %v updated md from %#v to %#v\n", S, args.Slotnum, xmd, md)

    return err
}

/* Fetch the sync metadata for a specified block number */
func (S *Store_node) Getsync(args node.Node_getsync_args, reply *node.Node_getsync_reply) error {
    md, err := S.Pa.get(args.Slotnum)
    Verbose("getsync: STORE %v block %v MD={%#v} err=%v\n", S, args.Slotnum, md, err)
    if err == nil {
	reply.Sync = uint32(md.Sync)
	reply.Flags = uint32(md.Flags)
	reply.Ltime = S.ltime
    }
    return err
}

/******************************************************************************/
/************* Interface to Persistent Array for Mirror Metadata **************/

/* INCORE persistent array slot representation */
type My_pa_data struct {
    Sync                    uint16
    Flags                   uint16
}

/* Persistent Array structure */
type Pa_array struct {
    log                     *logger.Pa_log_t		// logger instance
    cfg			    *logger.Pa_log_config_t
    Slot                    []My_pa_data		// INCORE persistent array slots
}

/* Create a fresh ONDISK Persistent Array, destroying anything in the specified PA file/offset */
func (PA *Pa_array) create(md_file *os.File, file_md_ofs int, cfg *logger.Pa_log_config_t) (err error) {
    /***********************************************************/
    /* callback function for logger to get INCORE slot entries */
    cb := func (slotnum int) (slot_bytes []byte, err error) {
	    if slotnum >= int(cfg.Slot_max) {
		Ewarn(nil, "PA.Create callback() slotnum out of bounds: %v >= %v", slotnum, cfg.Slot_max)
		err = node.ERANGE
		return
	    }
	    return pa_to_bytes(slotnum, PA.Slot[slotnum]), nil
	  }
    /***********************************************************/

    pa_comment := "Mirror Metadata"

    err = logger.Disk_init(md_file, file_md_ofs, cb, cfg, pa_comment)
    if err != nil {
	Ewarn(err, "logger.Disk_init %v failed", md_file)
	return err
    }

    Chat("Created fresh ONDISK Persistent Array: %v", pa_comment)

    return
}

/* Open an existing ONDISK Persistent Array;
 * Initialize INCORE array from ONDISK data;
 * Start the logger;
 * Return the configuration.
 */
func (PA *Pa_array) open(md_file *os.File, file_md_ofs int) (*logger.Pa_log_config_t, error) {
    // open the logger and read the persistent array header
    var err error
    PA.log, err = logger.Open(md_file, file_md_ofs,
	    /*******************************************************/
	    /* callback function for logger to get refresh entries */
	    func (slotnum int) (slot_bytes []byte, err error) {
		slot_data, err := PA.get(slotnum)
		if err != nil {
		    return
		}
		return pa_to_bytes(slotnum, slot_data), nil
	    })
	    /*******************************************************/

    if err != nil {
	Warn("Pa_log_open failed %v", err)
	return nil, err
    }

    if PA.Slot == nil {
	PA.Slot = make([]My_pa_data, PA.log.Cfg.Slot_max)
    } else {
	if len(PA.Slot) != int(PA.log.Cfg.Slot_max) {
	    Ewarn(nil, "MISMATCH len(PA.Slot)=%v slot_max=%v", len(PA.Slot), PA.log.Cfg.Slot_max)
	    return nil, node.EBADSTORECONFIG
	}
    }

    // read array contents from ONDISK slot area into the INCORE array
    err = PA.log.Read(
	/**********************************************************/
	/* closure called for slot records oldest to newest seqno */
	func (slot_bytes []byte) {
	    slot, data := pa_of_bytes(slot_bytes)

	    if slot >= int(PA.log.Cfg.Slot_max) {
		Warn("log.read callback out of range: slot %v > %v",
			slot, PA.log.Cfg.Slot_max)
	    } else {
		PA.Slot[slot] = data
		Verbose("read into slot=%v: sync=%x flags=%x", slot, data.Sync, data.Flags)
	    }
	})
	/**********************************************************/

    if err != nil {
	Warn("pa_log_read failed %v", err)
	return nil, err
    }

    // start the log buffer accumulator
    go PA.log.Logger()

    return &PA.log.Cfg, err
}

/* Modifications to elements of the persistent array must occur through the
 * "set" function, which, in addition to setting the new array slot content
 * INCORE, enqueues a log of the change to be written to persistent storage
 * ONDISK.
 */

/* Retrieve a pointer to the data in the specified array slot */
func (PA *Pa_array) get(slotnum int) (ret My_pa_data, err error) {
    if slotnum >= len(PA.Slot) {
	Ewarn(nil, "PA.get(): slotnum out of bounds: %v/%v", slotnum, len(PA.Slot))
	err = node.ERANGE
	return
    }
    return PA.Slot[slotnum], nil;
}

/* Set data into the specified array slot --
 * Data is set in the INCORE array immediately.
 * Closure will be called after the data has completed ONDISK recording.
 */
func (PA *Pa_array) set(slotnum int, my_elem My_pa_data, closure func (error)) (err error) {
    if slotnum >= len(PA.Slot) {
	Ewarn(nil, "PA.set(%v): slotnum out of bounds: %v/%v", PA, slotnum, len(PA.Slot))
	return node.ERANGE
    }

    // Set the INCORE slot with the new content
    PA.Slot[slotnum] = my_elem;

    // Encode the INCORE slot into bytes
    txbytes := pa_to_bytes(slotnum, my_elem)

    // Enqueue request to the persistent logger --
    // The closure will be called when the record is written and sync'd ONDISK
    return PA.log.Tx(txbytes, closure)
}

/* Set data into the specified array slot, and wait for ONDISK recording to complete. */
func (PA *Pa_array) set_wait(slotnum int, my_elem My_pa_data) error {
    // When the write completes, send ourselves a message with the result
    Q_write_complete := make(chan error, 1)

    /*************************************/
    /* Closure to be called after completion of metadata write and sync, receives error */
    closure := func (err error) {
		    Q_write_complete <- err
		    close(Q_write_complete)
	       }
    /*************************************/

    // Issue the write, along with the completion closure to return the result
    // through the Q_write_complete channel.
    err := PA.set(slotnum, my_elem, closure)
    if err != nil {
	return err
    }

    // Flush all requests (including ours just above) from Q_tx into Q_ser
    PA.log.Flush()

    // Wait for result of the write, and return it
    return <- Q_write_complete
}

func (PA *Pa_array) ltime() int {
    return PA.log.Ltime()
}

func (PA *Pa_array) ltime_set(ltime int) error {
    return PA.log.Ltime_set(ltime)
}

func (PA *Pa_array) flush() {
    PA.log.Flush()
}

func (PA *Pa_array) flush_wait() {
    PA.log.Flush_wait()
}

func (PA *Pa_array) close() error {
    // Add a flush/close request to the logger's input queue
    PA.log.Close()
    return nil
}

func (PA *Pa_array) Dump() {
    if PA == nil {
	panic("PA == nil")
    }
    fmt.Printf("%v\n\n", PA.log)
    for i, data := range PA.Slot {
	Verbose("%d: sync=%x flags=%x\n", i, data.Sync, data.Flags)
    }
    Verbose("\n")
}

type pa_slot_layout_t struct {
    slotnum		    uint64
    sync		    uint32
    reserved_sync	    uint32
    reserved		    uint64
    flags		    uint32
    reserved_flags	    uint32
}			// 32 bytes

/* Encode INCORE representation to ONDISK representation */
func pa_to_bytes(slotnum int, my_elem My_pa_data) []byte {
    var ret [32]byte
    layout := (*pa_slot_layout_t)(unsafe.Pointer(&ret))
    layout.slotnum = uint64(slotnum)
    layout.sync = uint32(my_elem.Sync)
    layout.flags = uint32(my_elem.Flags)
    return ret[:]
}

/* Decode ONDISK representation to INCORE representation */
func pa_of_bytes(pa_bytes []byte) (slotnum int, my_elem My_pa_data) {
    layout := (*pa_slot_layout_t)(unsafe.Pointer(&pa_bytes[0]))
    slotnum = int(layout.slotnum)
    my_elem.Sync = uint16(layout.sync)
    my_elem.Flags = uint16(layout.flags)
    return
}
