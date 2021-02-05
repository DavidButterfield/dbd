package storage_node_mirror

import "fmt"
import "math/bits"
import "sync/atomic"
import "time"

import node	"storage_node"
import .	"junk"

import "C"

// Broken behavior even without a failure -- for testing only
var SKIP_COPY_DATA = false	// when updating a mirror during recovery
var SKIP_SYNC = false		// skip mirror sync-setting calls

/********** Duplicator (Mirror Splitter) **********/

type Dup_node struct {
    Name		string
    Down		[]node.Node // underlying mirrors
    Bytes_per_slot	int	    // number of storage bytes per metadata slot
    sync_active		uint32	    // mirrors in current active communication
    sync_current	uint32	    // mirrors active and up-to-date data
    probe_active	uint32	    // mirrors with probe active
    ltime		int	    // mirror current logical time
    nbyte		int	    // number of blocks exposed by mirrored device
    blocker		chan bool   // blocks writes during mirror recovery
}

/* Increment the logical time on all the "current" mirrors */
func (N *Dup_node) Ltime_bump() {
    var args node.Node_sync_args
    var reply node.Node_sync_reply
    N.ltime++
    args.Ltime = N.ltime
    for m, mirror := range N.Down {
	if !Atomic_bit_tst_32(&N.sync_current, 1<<m) {
	    continue
	}
	err := mirror.Sync(args, &reply)
	if err != nil {
	    Warn("Failed to Ltime_bump on mirror(%v)=%v: %v", m, mirror, err)
	    N.Mirror_down(m)
	    continue
	}
	Chat("Ltime_bump mirror(%v)=%v", m, mirror)
    }
    return
}

/* Choose a mirror to read from */
func (N *Dup_node) read_mirrornum() (int, error) {
    sync := Atomic_bit_fetch_32(&N.sync_current)
    m := bits.TrailingZeros32(sync)
    if m > 31 {
	Chat("No mirrors (sync_current=0x%04x, sync_active=0x%04x): %v", N.sync_current, N.sync_active, Stackline())
    }

    if !Atomic_bit_tst_32(&N.sync_current, 0xffffffff) {
	return -1, node.ENOMIRROR
    }

    //XXXX prefer local store if up-to-date

    return bits.TrailingZeros32(sync), nil
}

func (N *Dup_node) String() string {
    return fmt.Sprintf("Dup_node %v nbyte=%v active=0x%04x current=0x%04x ltime=%v",
			    N.Name, N.nbyte, N.sync_active, N.sync_current, N.ltime)
}

func (N *Dup_node) Probe(args node.Node_probe_args, reply *node.Node_probe_reply) error {
    var below_reply node.Node_probe_reply

    Assert(len(N.Down) > 0)
    for m, mirror := range N.Down {
	if args.Host == node.My_hostname {
	    // Start a probe for each mirror
	    N.mirror_probe(m)
	} else {
	    Chat("Dup_node.Probe() node %v (nbyte=%v) is for hostname %v", N, below_reply.Nbyte, args.Host)
	    err := node.Node_probe(mirror, args, &below_reply)
	    if err != nil {
		Warn("Dup_node.Probe() mirror=%v: %v", m, err)
	    }
	}
    }

    if args.Host != node.My_hostname {
	Ewarn(node.EHOSTMISMATCH, "Mirror Splitter %v is off-host at '%v' (I am '%v'):", N, args.Host, node.My_hostname)
	return node.EHOSTMISMATCH
    }

    // put a closed channel into place, which will not block readers
    N.blocker = make(chan bool)
    close(N.blocker)

    reply.Nbyte = N.nbyte
    reply.Ltime	= N.ltime

    Chat("Dup_node %v", N)
    return nil
}

func (N *Dup_node) Read(args node.Node_read_args, reply *node.Node_read_reply) error {
    Assert(args.Ofs < N.nbyte)
    Assert(args.Ofs + args.Len <= N.nbyte)
    Assertz(args.Slotnum)   // no one above us should be setting this
    args.Slotnum = args.Ofs / N.Bytes_per_slot

    for {
	m, err := N.read_mirrornum()
	if err != nil {
	    Ewarn(err, "No mirrors available at Read")
	    return err
	}

	err = N.Down[m].Read(args, reply)
	if err != nil {
	    Ewarn(nil, "mirror responded with error %v", err)
	    N.Mirror_down(m)
	    continue	    // try another mirror
	}

	if reply.Ltime < N.ltime {
	    Ewarn(nil, "Out-of-date mirror %m with ltime %v/%v responded!", m, reply.Ltime, N.ltime)
	    N.Mirror_down(m)
	    continue
	}

	if (reply.Sync & (1<<m)) == 0 {
	    fmt.Printf("CLR: Mirror %v chosen but unsync'd 0x%04x on ofs=%v\n", m, reply.Sync, args.Ofs)
	    Atomic_bit_clr_32(&N.sync_current, 1<<m)
	    go N.mirror_update(m)
	    continue
	}

	return nil
    }
}

func (N *Dup_node) Write(args node.Node_write_args, reply *node.Node_write_reply) error {
    // Assertz(int(args.Sync))	// no one above us should be setting this!
    Assert(args.Ofs < N.nbyte)
    Assert(args.Ofs + len(args.Data) <= N.nbyte)
    Assertz(args.Slotnum)   // no one above us should be setting this
    args.Slotnum = args.Ofs / N.Bytes_per_slot

    // Wait until writes are not blocked
    <-N.blocker

    //XXXX run concurrently
    var writes_try, writes_did uint32
    var below_reply node.Node_write_reply
    below_args := args
    below_args.Slotnum = below_args.Ofs/N.Bytes_per_slot
    for m, mirror := range N.Down {
	var mirror_bit uint32 = 1<<m
	if !Atomic_bit_tst_32(&N.sync_active, mirror_bit) {
	    continue
	}
	sync_bit := mirror_bit
	if SKIP_SYNC {
	    sync_bit = N.sync_active
	}
	writes_try |= mirror_bit
	below_args.Sync = sync_bit
	err := mirror.Write(below_args, &below_reply)
	if err != nil {
	    Ewarn(err, "(Mirror.Write): mirror %v failed", m)
	    N.Mirror_down(m)
	    continue
	}
	writes_did |= mirror_bit
    }

    if writes_try == 0 {
	Ewarn(nil, "WRITE tried NO MIRRORS!")
	return node.ENOMIRROR
    } else if writes_did == 0 {
	Ewarn(nil, "WRITE could write NO MIRRORS!")
	return node.ENOMIRROR
    }

    if writes_did != writes_try {
	Warn("WRITE bn=%v mirrors tried=0x%04x did=0x%04x", args.Slotnum, writes_try, writes_did)
    } else {
	Verbose("WRITE bn=%v mirrors tried=0x%04x did=0x%04x", args.Slotnum, writes_try, writes_did)
    }

    if SKIP_SYNC {
	return nil
    }

    // Phase II -- propagate sync bits to all confirmed writers for all the confirmed writes
    //XXX This should piggyback with a regular request if available
    var s_args node.Node_sync_args
    var s_reply node.Node_sync_reply
    s_args.Slotnum = args.Ofs/N.Bytes_per_slot
    s_args.Sync = writes_did
    s_args.Flag_bits_to_clr = 0xff00
    s_args.Flag_bits_to_set = 0xbb00

    //XXXX run concurrently
    for m, mirror := range N.Down {
	var mirror_bit uint32 = 1<<m
	if (writes_did & mirror_bit) == 0 {
	    Verbose("SKIP SYNC mirror %v", m)
	    continue
	}

	// This obscure line of code should have a meaningful comment
	if writes_did == mirror_bit {
	    break
	}

	err := mirror.Sync(s_args, &s_reply)
	if err != nil {
	    Ewarn(err, "node_sync() could not sync mirror %v to %v:", m, s_args.Sync)
	    N.Mirror_down(m)
	    continue
	}
    }

    return nil
}

/* Only Dup_nodes issue these calls, so we should never receive one */

func (N *Dup_node) Sync(args node.Node_sync_args, reply *node.Node_sync_reply) (ret error) {
    panic("Do not configure a mirror below a mirror!")
}

func (N *Dup_node) Getsync(args node.Node_getsync_args, reply *node.Node_getsync_reply) (ret error) {
    panic("Do not configure a mirror below a mirror!")
}

/* Declare a mirror inaccessable */
func (N *Dup_node) Mirror_down(m int) error {
    if Atomic_bit_tst_32(&N.sync_active, 1<<m) {
	Warn("M>>> CLR Mirror_down(%v)", m)
	Atomic_bit_clr_32(&N.sync_active, 1<<m)
	Atomic_bit_clr_32(&N.sync_current, 1<<m)
	N.Ltime_bump()
	N.mirror_probe(m)
    } else {
	Warn("M>>> mirror still down(%v)", m)
    }
    return nil
}

func (N *Dup_node) mirror_try(m int, reply *node.Node_probe_reply) error {
    var args node.Node_probe_args
    args.Host = node.My_hostname
    mirror := N.Down[m]
    err := node.Node_probe(mirror, args, reply)
    Chat("M>>> Try mirror %v mirror_try(%v): %#v (%v)", time.Now(), m, *reply, err)
    return err
}

/* Start a periodic probe attempt to connect to the mirror */
func (N *Dup_node) mirror_probe(m int) error {
    Chat("M>>>>>> mirror_probe(%v)", m)
    var reply node.Node_probe_reply

    // First try to do it synchronously
    err := N.mirror_try(m, &reply)
    if err == nil {
	return N.mirror_activate(m, &reply)
    }

    if (N.probe_active & (1<<m)) != 0 {
	return err
    }

    N.probe_active |= 1<<m   //XXX use atomic

    // Start a periodic probe
    go func () {
	for {
	    Notify("M>>> probe(%v) sleep(45)", m)
	    time.Sleep(45*time.Second)
	    Notify("M>>> probe(%v) awake(45)", m)
	    err := N.mirror_try(m, &reply)
	    if err != nil {
		continue
	    }
	    N.probe_active &^= 1<<m
	    // probe succeeded, activate the mirror
	    N.mirror_activate(m, &reply)
	    return
	}
    }()

    // Return the error that kept us from starting it synchronously
    return err
}

/* Activate a mirror that has been successfully probed */
func (N *Dup_node) mirror_activate(m int, reply *node.Node_probe_reply) error {
    Chat("M>>> mirror_activate(%v)", m)
    if Atomic_bit_tst_32(&N.sync_active, 1<<m) {
	Ewarn(nil, "mirror_activate(%v) already active", m)
	return nil
    }

    if N.nbyte == 0 {
	N.nbyte = reply.Nbyte
    } else if reply.Nbyte != N.nbyte {
	Ewarn(nil, "Mirror %v nbyte=%v MISMATCH with nbyte=%v", m, reply.Nbyte, N.nbyte)
	return node.EMIRRORMISMATCH
    }

    if reply.Ltime == N.ltime {
	Chat("mirror_activate: mirror %v nbyte=%v ltime equals node ltime, no recovery necessary", m, reply.Nbyte)
	N.mirror_up(m)
	Atomic_bit_set_32(&N.sync_current, 1<<m)
	return nil
    }

    if reply.Ltime < N.ltime {
	Chat("mirror_activate: recovering mirror %v nbyte=%v from ltime=%v to ltime=%v", m, reply.Nbyte, reply.Ltime, N.ltime)
	err := N.mirror_recover(m)
	if err == nil {
	    N.mirror_up(m)
	}
	return nil
    }

    Chat("mirror_activate: CLR new mirror %v nbyte=%v ltime=%v > ltime=%v", m, reply.Nbyte, reply.Ltime, N.ltime)
    N.ltime = reply.Ltime

    // the ones we already have are now seen to show out-of-date ltime
    stale := atomic.LoadUint32(&N.sync_active)
    atomic.StoreUint32(&N.sync_active, 0)

    Atomic_bit_clr_32(&N.sync_current, 0xffff)
    N.mirror_up(m)
    Atomic_bit_set_32(&N.sync_current, 1<<m)

    for mm, _ := range N.Down {
	if (stale & (1<<mm)) != 0 {
	    err := N.mirror_recover(mm)
	    if err == nil {
		N.mirror_up(mm)
	    }
	}
    }

    return nil
}

/* During activation, a mirror with stale ltime gets an updated set of metadata */
func (N *Dup_node) mirror_recover(dst int) error {
    Notify("M>>> mirror_recover(%v)", dst)
    Assert(!Atomic_bit_tst_32(&N.sync_active, 1<<dst))
    src, err := N.read_mirrornum()
    if err != nil {
	return err
    }
    Assert(src != dst)

    var get_args node.Node_getsync_args
    var src_getreply node.Node_getsync_reply
    var dst_getreply node.Node_getsync_reply
    var dst_args node.Node_sync_args
    var dst_reply node.Node_sync_reply

    t_start := time.Now()
    t_update := t_start
    N.blocker = make(chan bool)		////////// BLOCK WRITES
    {
	// Copy the syncs from an up-to-date mirror to the stale mirror
	// XXXX PERF
	var n_done int
	var ofs int
	for ofs = 0; ofs < N.nbyte; ofs += N.Bytes_per_slot {
	    get_args.Slotnum = ofs / N.Bytes_per_slot
	    err := N.Down[src].Getsync(get_args, &src_getreply)
	    Verbose("XXXXXXXXXX SRC getsync(%v) reply(%v) %v\n", get_args, src_getreply, err)

	    // It takes much longer to write than read, so check unequal first
	    N.Down[dst].Getsync(get_args, &dst_getreply)
	    Verbose("XXXXXXXXXX DST getsync(%v) reply(%v) %v\n", get_args, dst_getreply, err)
	    if dst_getreply.Sync != src_getreply.Sync {
		dst_args.Slotnum = get_args.Slotnum
		dst_args.Sync = src_getreply.Sync
		Verbose("XXXXXXXXXX Update sync of mirror %v offset %v from 0x%04x to 0x%04x\n",
			    dst, ofs, dst_getreply.Sync, dst_args.Sync)
		err := N.Down[dst].Sync(dst_args, &dst_reply)
		if err != nil {
		    break
		}
		n_done++
	    }

	    if time.Now().Sub(t_update) > time.Second {
		t_update = time.Now()
		fmt.Printf("META: %v/%v/%v\r", n_done, 1+ofs/N.Bytes_per_slot, N.nbyte/N.Bytes_per_slot)
	    }
	}
	fmt.Printf("META: %v/%v/%v\n", n_done, ofs/N.Bytes_per_slot, N.nbyte/N.Bytes_per_slot)
    }
    close(N.blocker)			////////// UNBLOCK WRITES
    Notify("mirror_recover(%v) blocked writes for %v", dst, time.Now().Sub(t_start))

    if err != nil {
	return err
    }

    /* dst now has current sync information, which includes knowing
     * when it does not have a current copy of a page.
     */

    // Update ltime on recovered mirror
    dst_args.Ltime = N.ltime
    err = N.Down[dst].Sync(dst_args, &dst_reply)
    if err != nil {
	Warn("Failed to update ltime to %v on mirror=%v: %v", N.ltime, dst, err)
	N.Mirror_down(dst)
	return err
    }

    go N.mirror_update(dst)

    return nil
}

/* Declare a mirror to be up and "active" (not necessarily up-to-date) */
func (N *Dup_node) mirror_up(m int) error {
    Chat("M>>> mirror_up(%v)", m)
    Atomic_bit_set_32(&N.sync_active, 1<<m)
    return nil
}

func (N *Dup_node) mirror_update_sched(m int) {
    go func () {
	Notify("M>>> mirror_update_sched(%v) sleep(6)", m)
	time.Sleep(6*time.Second)
	Notify("M>>> mirror_update_sched(%v) awake(6)", m)
	N.mirror_update(m)
    }()
}

/* Update the out-of-date blocks on a mirror */
func (N *Dup_node) mirror_update(m int) {
    Assert(!Atomic_bit_tst_32(&N.sync_current, 1<<m))
    Notify("M>>> mirror_update(%v)", m)
    t_start := time.Now()
    t_update := t_start
    var blocknum int
    var n_done int
    for blocknum = 0; blocknum < N.nbyte/N.Bytes_per_slot; blocknum++ {
	done, err := N.mirror_block_check_update(m, blocknum)
	if err != nil {
	    N.mirror_update_sched(m)
	    break
	}
	if done {
	    n_done++
	}
	if time.Now().Sub(t_update) > time.Second {
	    t_update = time.Now()
	    fmt.Printf("DATA: %v/%v/%v\r", n_done, 1+blocknum, N.nbyte/N.Bytes_per_slot)
	}
    }
    fmt.Printf("DATA: %v/%v/%v\n", n_done, blocknum, N.nbyte/N.Bytes_per_slot)
    elapsed := time.Now().Sub(t_start)
    Notify("M>>> mirror_update(%v) done, time=%v", m, elapsed)
}

/* Decide whether the specified block needs updating; if so, update it */
func (N *Dup_node) mirror_block_check_update(dst int, blocknum int) (bool, error) {
    // Verbose("M>>> mirror_block_check_update(%v) block=%v", dst, blocknum)
    var args node.Node_getsync_args
    var reply node.Node_getsync_reply
    args.Slotnum = blocknum
    err := N.Down[dst].Getsync(args, &reply)
    if err != nil {
	return false, err
    }

    // If the sync bit is set then the block is already up-to-date
    if (reply.Sync & (1<<dst)) != 0 {
	return false, nil
    }

    /* Update the block */
    err = N.mirror_block_update(dst, blocknum)
    return err == nil, err
}

/* Update the specified block on the mirror */
func (N *Dup_node) mirror_block_update(dst int, blocknum int) error {
    Verbose("M>>> mirror_block_update(%v) block=%v", dst, blocknum)

    // Find an up-to-date mirror
    src, err := N.read_mirrornum()
    if err != nil {
	return err	// no mirror available for read
    }
    Assert(src != dst)

    var new_sync, mirrors_to_update uint32

    if !SKIP_COPY_DATA {
	// Read the block off the up-to-date mirror
	var src_args node.Node_read_args
	var src_reply node.Node_read_reply
	src_args.Ofs = blocknum * N.Bytes_per_slot
	src_args.Len = N.Bytes_per_slot
	err = N.Down[src].Read(src_args, &src_reply)    // issue the read
	if err != nil {
	    return err
	}

	new_sync = src_reply.Sync | (1<<dst)    // Add dst into the current sync cohort
	mirrors_to_update = src_reply.Sync	// EXcluding dst

	// Write the block to the stale mirror
	// Since the other copies in sync are already written, we can sync during the Write
	var dst_args node.Node_write_args
	var dst_reply node.Node_write_reply
	dst_args.Ofs = blocknum * N.Bytes_per_slot
	dst_args.Data = src_reply.Data	// write the data we read above
	dst_args.Sync = new_sync	// include the new_sync with the write
	dst_args.Flag_bits_to_clr = 0xf000
	dst_args.Flag_bits_to_set = 0xf000
	err = N.Down[dst].Write(dst_args, &dst_reply)   // issue the write
	if err != nil {
	    return err
	}

    } else {
	// Just add dst to the sync cohort without actually copying the data block
	var args node.Node_getsync_args
	var reply node.Node_getsync_reply
	args.Slotnum = blocknum
	err = N.Down[src].Getsync(args, &reply)
	if err != nil {
	    return err
	}

	new_sync = reply.Sync | (1<<dst)    // Add dst into the current sync cohort
	mirrors_to_update = new_sync	    // INcluding dst
    }

    // Send new_sync to the other mirrors, so they know dst is up-to-date
    var sync_args node.Node_sync_args
    var sync_reply node.Node_sync_reply
    sync_args.Slotnum = blocknum
    sync_args.Sync = new_sync
    sync_args.Flag_bits_to_clr = 0xf000
    sync_args.Flag_bits_to_set = 0xb000

    //XXXX run concurrently
    for m, mirror := range N.Down {
	if (mirrors_to_update & (1<<m)) == 0 {
	    continue
	}
	err := mirror.Sync(sync_args, &sync_reply)
	if err != nil {
	    Ewarn(err, "block_update node_sync(%v):", N)
	    N.Mirror_down(m)
	    continue
	}
	Verbose("Sync'd mirror %v to 0x%04x\n", m, new_sync)
    }

    Verbose("Mirror %d updated block %v from mirror %v (%v)", dst, blocknum, src, err)
    return nil
}
