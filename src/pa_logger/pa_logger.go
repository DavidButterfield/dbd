package pa_logger

import "errors"
import "fmt"
import .    "golang.org/x/sys/unix"
import "hash/crc32"
import "os"
import "runtime"
import "syscall"
import "unsafe"

import .    "junk"

var SKIP_FLUSH_META = false

var CHATTY = false
var VERBOSE = false
var VVERBOSE = false

const pa_serializer_cpu_mask = 0x0001
const pa_logger_cpu_mask = 0x0002

const Pa_log_write_bytes = 1024*1024	// size of persistent array writes in bytes
const pa_log_Q_ser_cap = 64		// capacity of write serialization queue
const pa_log_Q_tx_cap = 1024*1024	// capacity of pa transaction queue

var EMETAREAD = errors.New("Metadata Read Failure")
var ESTALELTIME = errors.New("New Ltime less than previous!")

func chat(str string, args... interface{}) {
    if CHATTY {
        fmt.Printf("  " + str + "\n", args...)
    }
}

/* Persistent Array
 *
 * ("ONDISK" means in persistent storage; "INCORE" means in volatile storage)
 *
 * The persistent array resides INCORE, with a backing log ONDISK.
 * Modifications to the INCORE array are recorded in a log ONDISK.  In case the
 * INCORE array is lost (e.g. crash), it can be reconstructed from the log.
 *
 * The ONDISK representation of the persistent array looks nothing like the
 * INCORE representation.  The persistent data is maintained INCORE as some
 * opaque user type indexed zero to slot_max-1.
 *
 * ONDISK the information is kept in a "slot area" as a circular log recording
 * changes made to slots in the INCORE array The log is designed so that this
 * ONDISK slot area always contains at least one record of the persisted data
 * for every array slot at all times.
 *
 * The slot area is divided into "sectors" writable in a single I/O operation
 * (for example the hardware sector size or some multiple of it).
 * 
 * Each sector in the slot area has a pa_sector_hdr_t followed by some number
 * of slots, each of which can record an encoding of one persisted array INCORE
 * slot number and its content.
 *
 * When an array slot is changed, the the new slot content is encoded into an
 * opaque vector of bytes that represents the slot number itself, in addition
 * to the slot data.  It presents this opaque slotnum/content pair to be
 * recorded into one of the slots in a sector to be written to the ONDISK slot
 * area, along with a closure to be called when the updated slot information
 * has completed recording to persistent media.
 *
 * ONDISK LAYOUT
 *
 * The size of the slots in the ONDISK array is configurable.  (Guess: it seems
 * likely to be more optimal in the range of five to fifteen slots per sector)
 *
 * The ONDISK persistent array area begins with a pa_header_block_t describing
 * the size of the slot area and the geometry of the persistent-array sectors
 * within it.  The pa_header_block_t is at offset PA.log.File_pa_ofs into
 * PA.log.file.  The pa_header_block_t contains a Pa_log_config_t describing
 * the ONDISK layout.  That includes:
 *
 *   Slot_max		    INCORE slot numbers range from zero to slot_max-1
 *   Slot_start_ofs	    byte offset of start of slots from start of hdr block
 *   Bytes_in_slot_area	    length of ONDISK slot area (from slot_start_ofs)
 *
 *   Bytes_per_sector	    e.g. 512 or 4096 (or multiples of HW sector)
 *   Bytes_hdr_per_sector   persistent-array header at start of each slot sector
 *   Bytes_per_slot	    size of each array slot in sectors ONDISK
 *   Rslots_max_per_sector  number of refresh slots per sector (before xslots)
 *   Xslots_max_per_sector  number of transaction slots per sector (after rslots)
 *
 * The length of a "sector" (bytes_per_sector) can be a multiple of the
 * hardware sector length that can be written in one I/O operation.
 *
 * Recommendation: Unless the persistent array elements are larger than
 * (bytes_per_sector - bytes_hdr_per_sector) / 5, use hardware sector size.
 *
 * Each sector begins with a sector header, followed by refresh slots (rslots),
 * followed by transaction slots (xslots).  The number of rslots and xslots
 * present in any particular sector is recorded in that sector's sector_header.
 * All slots are of length bytes_per_slot; xslots appear in the sector
 * immediately after the last rslot (as indicated by hdr.rslots).
 *
 * So bytes_per_sector must be at least as large as
 * bytes_hdr_per_sector + bytes_per_slot * (rslots_max_per_sector + xslots_max_per_sector)
 */

/* ONDISK Representation
 *                 Offset into pa_file (backing device or file)
 *   --------- <-- 0
 *   |       |
 *   |ignored|
 *   |       |
 *   |-------| <-- File_pa_ofs
 *   |pa_hdr |
 *   |-------| <-- File_pa_ofs + cfg.Slot_start_ofs
 *   |slot   |	    ^
 *   | sector|	    |
 *   |-------|	    |
 *   |  ...  |	SLOT AREA
 *   |       |	    |
 *   |-------|	    |
 *   |slot   |	    |
 *   | sector|	    v
 *   |-------| <-- File_pa_ofs + cfg.Slot_start_ofs + cfg.Bytes_in_slot_area
 *   |       |
 *   |ignored|
 *   |       |
 *   ---------
 */

/* The number of xslots used in any particular sector can vary from zero to
 * xslots_max_per_sector, with the number of xslots in a sector denoted in
 * the sector header by hdr.xslots.
 *
 * The same should be considered true for hdr.rslots, the number of rslots
 * present in a sector.  Log readers should not assume that every sector was
 * written with sector_hdr.rslots equal to pa_hdr.rslots_max_per_sector, even
 * if that happens to be the case in the present implementation.
 *
 * REFRESH SLOTS
 *
 * Each ONDISK sector reserves some of its slots as "refresh" slots; the other
 * slots are "transaction" slots.  Each sector is (normally) written with all
 * REFRESH slots filled (from sequential INCORE array slots); but some (or even
 * all) TRANSACTION slots may be unused in any particular sector.
 *
 * Transaction slots record dynamic changes made to the array during operation
 * of the server.  This drives the writing of sectors to the ONDISK circular
 * log.
 *
 * As each log sector is prepared for writing onto disk (in service of one or
 * more transactions) a number (rslots_max_per_sector) of REFRESH slots are also
 * filled into those same sectors.  The refresh slots are filled by cycling
 * through INCORE array slots from zero to slot_max-1 and around again
 * indefinitely.
 *
 * If the size of the ONDISK slot area is sufficiently large, these refresh
 * entries ensure that every INCORE slot gets rewritten ONDISK before its
 * previous record is overwritten by the circular logging.  Thus the ONDISK log
 * will always contain at least one record for each INCORE array slot
 * describing its persisted content, whether or not the INCORE slot has been
 * recently modified.
 *
 * If the log is read and found to contain more than one record for a slot, the
 * one with the newest sequence number is used.  Within the same sector, xslot
 * changes appear in order oldest to newest; later is preferred over earlier.
 * 
 * For the log to contain all the INCORE array slots, it is required that
 * bytes_in_slot_area > bytes_per_sector * (slot_max / rslots_max_per_sector)
 *
 * Also, the queue-depth of bytes being written to the log must be limited to
 * bytes_in_slot_area - bytes_per_sector * (slot_max / rslots_max_per_sector)
 *
 * That is necessary to guarantee the prior ONDISK record for a slot is not
 * overwritten until after successful completion of a new write of that slot.
 *
 * LOGGER OPERATION
 *
 * The logger issues one write at a time, issuing fdatasync(2) after each write
 * completion before starting the next write.  This is to ensure at all times a
 * clean boundary in the ONDISK circular log between where new entries end and
 * old entries begin.
 *
 * When a write completes, the next write is issued with new transactions
 * accumulated since the previous write was issued (up to a maximum I/O size of
 * max_write_bytes).  Each write has an associated list of closures, also
 * called by the write completion, to notify the upstream requestor when the
 * slots have been persisted ONDISK.
 */

/* Header at start of ONDISK Persistent Array area */
type Pa_log_config_t struct {
    Magic		    uint32	// magic number for this persistent array
    Version		    uint16	// persistent array structure version number
    Bytes_per_sector	    uint16	// e.g. 512 or 4096
    Bytes_in_slot_area	    uint64	// length of ONDISK slots area (not hdr)
    Slot_max		    uint64	// slot numbers must be less than this
    Slot_start_ofs	    uint32	// ofs of slot area from start of pa hdr
    reserved		    uint32
    Bytes_hdr_per_sector    uint16	// header at start of each slot sector
    Bytes_per_slot	    uint16	// size of each array slot on disk
    Rslots_max_per_sector   uint16	// refresh slots per sector
    Xslots_max_per_sector   uint16	// transaction slots per sector
};
const sizeof_Pa_log_config_t = int(unsafe.Sizeof(*new(Pa_log_config_t)))

/* Header block at start of ONDISK Persistent Array area contains the config */
type pa_header_block_t struct {		// discourage changing from 4096 bytes
    ltime		    int
    cfg			    Pa_log_config_t
    reserved_pa_config	    [2048-4-sizeof_Pa_log_config_t]byte
    checksum		    uint32
    pa_freetext		    [2048]byte
}
const sizeof_pa_header_block = int(unsafe.Sizeof(*new(pa_header_block_t)))
const offsetof_pa_header_block_checksum = int(unsafe.Offsetof((*new(pa_header_block_t)).checksum))

/* Header at start of each sector in the slot area */
type pa_sector_hdr_t struct {
    sector_checksum	    uint32	// checksum over entire sector (must be first)
    version		    uint16	// persistent array structure version number
    reserved1		    uint16
    rslots		    uint16	// number of rslots in this sector
    xslots		    uint16	// number of xslots in this sector
    reserved2		    uint32
    last_rslotnum	    uint64	// slotnum last rslot in this sector
    seqno		    uint64	// log sequence number per sector write
}			// 32 bytes
const sizeof_pa_sector_hdr_t = int(unsafe.Sizeof(*new(pa_sector_hdr_t)))

func hdr_of_sector(sector_bytes []byte) *pa_sector_hdr_t {
    return (*pa_sector_hdr_t)(unsafe.Pointer(&sector_bytes[0]))
}

func slot_of_sector(sector_bytes []byte, slotnum int, cfg *Pa_log_config_t) []byte {
    Assert_eq(len(sector_bytes), int(cfg.Bytes_per_sector))
    ofs := sizeof_pa_sector_hdr_t + slotnum * int(cfg.Bytes_per_slot)
    return sector_bytes[ofs:ofs+int(cfg.Bytes_per_slot)]
}

func sector_dump(secnum int, sector_bytes []byte) {
    hdr := hdr_of_sector(sector_bytes)
    fmt.Printf("Sector=%v seqno=%v rslots=%v xslots=%v last_rslot=%v version=%v checksum=%v\n",
	    secnum, hdr.seqno, hdr.rslots, hdr.xslots, hdr.last_rslotnum, hdr.version, hdr.sector_checksum)
}

func (L *Pa_log_t) sector_check(sector_bytes []byte) error {
    hdr := hdr_of_sector(sector_bytes)
    sum := crc32.ChecksumIEEE(sector_bytes[4:L.Cfg.Bytes_hdr_per_sector])
    if sum != hdr.sector_checksum {
	Ewarn(nil, "checksum error on sector")
	return EMETAREAD
    }
    if hdr.version != L.Cfg.Version {
	Ewarn(nil, "version mismatch on sector")
	return EMETAREAD
    }
    return nil
}

/* Slot op comprises binary slot data to be logged, and closure called when complete */
/* This is the type that travels through channel Q_tx */
type pa_slot_op struct {
    data		    []byte
    closure		    func (error)
}

/* logger state */
type Pa_log_t struct {
    /* provided by upstream persistent array service, client of this pa_log */
    File		    *os.File	// open persistent array device
    File_pa_ofs		    int		// start of pa header in pa_file
					// callback closure for refresh entries
    comment		    string
    pa_get_bytes	    func (int) ([]byte, error)

    /* logical time */
    ltime		    int		// stored on behalf of mirror splitter

    /* stored ONDISK in persistent array header */
    Cfg			    Pa_log_config_t

    /* created by open() */
    Q_ser		    chan func ()    // log write serialization queue
    Q_tx		    chan pa_slot_op // logger log requests arrive here
    Q_flush		    chan byte	    // logger flush requests arrive here

    /* critical logger state */
    refresh_slot	    int		// next refresh slot number
    sector_seqno	    uint64	// next sector seqno
    file_writeofs	    int		// next write offset in pa_file ONDISK
    max_write_bytes	    int		// max pa slot byte write queue depth
    logging_active	    bool	// L.Logger() has been called
    starving		    bool	// no work to do

    /* persistent array sector accumulation buffer */
    buffer		    []byte	// accumulated log write data
    buf_xslot_ofs	    int		// next slot offset to write in buffer[]
    buf_sector_ofs	    int		// current sector offset in buffer[]

    /* persistent array write completion closures accumulated with buffer */
    closure		    []func(error)   // accumulating completion closures

    /* statistics */
    hQ_ser		    int		// high-watermark of write closures in Q_ser
    nQ_ser_write	    int		// number of write closures to Q_ser
    nQ_ser_flushwait	    int		// number of flushwaits
    nQ_ser_flush	    int		// number of flush closures to Q_ser
    ser_flush_size_cum	    int		// cumulative sizes of partial flush-writes
    nStarve		    int		// times Q_ser empty and cannot be supplied
    nAccum		    int		// times a tx was accumulated for recording
    nFlush_0		    int		// times logger flush was called flag 0
    nFlush_1		    int		// times logger flush was called flag 1
    nFlush_x		    int		// times logger flush was called flag other
}

func (L *Pa_log_t) Buf_empty() bool {
    return L.buffer == nil
}

func (L *Pa_log_t) Stat_reset() {
    L.hQ_ser = 0
    L.nQ_ser_write = 0
    L.nQ_ser_flushwait = 0
    L.nQ_ser_flush = 0
    L.ser_flush_size_cum = 0
    L.nStarve = 0
    L.nAccum = 0
    L.nFlush_0 = 0
    L.nFlush_1 = 0
    L.nFlush_x = 0
}

func (L *Pa_log_t) StatString() string {
    var avg int
    if L.nQ_ser_flush == 0 {
	avg = 0
    } else {
	avg = L.ser_flush_size_cum / L.nQ_ser_flush
    }
    return fmt.Sprintf("hQ_ser=%v nQ_ser_write=%v nQ_ser_flushwait=%v nQ_ser_flush=%v ser_flush_size_cum=%v nStarve=%v" +
				" nAccum=%v nFlush_0=%v nFlush_1=%v nFlush_x=%v average_bytes_per_flushwrite=%v",
			L.hQ_ser, L.nQ_ser_write, L.nQ_ser_flushwait, L.nQ_ser_flush, L.ser_flush_size_cum, L.nStarve,
			L.nAccum, L.nFlush_0, L.nFlush_1, L.nFlush_x, avg)
}

/* Check the configuration for self-consistency */
func cfg_check(cfg *Pa_log_config_t) error {
    max_slots := int(cfg.Bytes_in_slot_area) / int(cfg.Bytes_per_sector) * int(cfg.Rslots_max_per_sector)
    if max_slots < int(cfg.Slot_max) {
	fmt.Printf("pa_log config: bytes_in_slot_area %v can handle %v slots; " +
		   " not big enough for slot_max %v\n",
		    cfg.Bytes_in_slot_area, max_slots, cfg.Slot_max)
	return errors.New("bytes_in_slot_area too small for slot_max")
    }

    slots_per_sector := int(cfg.Rslots_max_per_sector) + int(cfg.Xslots_max_per_sector)
    if int(cfg.Bytes_hdr_per_sector) + int(cfg.Bytes_per_slot) * slots_per_sector > int(cfg.Bytes_per_sector) {
	fmt.Printf("pa_log config: bytes_per_sector %v cannot handle %v slots of size %v with header size %v\n",
		cfg.Bytes_per_sector, slots_per_sector, cfg.Bytes_per_slot, cfg.Bytes_hdr_per_sector)

	return errors.New("bytes_per_sector too small for slots_per_sector")
    }

    return nil
}

func (L *Pa_log_t) Cfg_check() error {
    return cfg_check(&L.Cfg)
}

func (L *Pa_log_t) String() string {
    return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n",
	fmt.Sprintf("Magic=%v version=%v File_pa_ofs=%v slot_start_ofs=%v bytes_in_slot_area=%v slot_max=%v",
	    L.Cfg.Magic, L.Cfg.Version, L.File_pa_ofs, L.Cfg.Slot_start_ofs, L.Cfg.Bytes_in_slot_area, L.Cfg.Slot_max),
	fmt.Sprintf("bytes_per_sector=%v bytes_hdr_per_sector=%v bytes_per_slot=%v rslots_max_per_sector=%v xslots_max_per_sector=%v",
	    L.Cfg.Bytes_per_sector, L.Cfg.Bytes_hdr_per_sector, L.Cfg.Bytes_per_slot, L.Cfg.Rslots_max_per_sector, L.Cfg.Xslots_max_per_sector),
	fmt.Sprintf("max_write_bytes=%v len(L.Q_ser)=%v len(L.Q_tx)=%v len(L.Q_flush)=%v logging_active=%v",
	    L.max_write_bytes, len(L.Q_ser), len(L.Q_tx), len(L.Q_flush), L.logging_active),
	fmt.Sprintf("file_writeofs=%v buf_sector_ofs=%v buf_xslot_ofs=%v refresh_slot=%v sector_seqno=%v starving=%v",
	    L.file_writeofs, L.buf_sector_ofs, L.buf_xslot_ofs, L.refresh_slot, L.sector_seqno, L.starving),
	L.StatString())
}

func log_header_write(file *os.File, pa_start_ofs int, ltime int,
			cfg *Pa_log_config_t, freetext string) (error) {
    var log_hdr pa_header_block_t
    log_hdr_bytes := (*[sizeof_pa_header_block]byte)(unsafe.Pointer(&log_hdr))

    /* Write the header */
    log_hdr.ltime = ltime
    log_hdr.cfg = *cfg
    copy(log_hdr.pa_freetext[:], freetext)
    log_hdr.checksum = crc32.ChecksumIEEE((*log_hdr_bytes)[0:offsetof_pa_header_block_checksum])

    _, err := file.WriteAt(log_hdr_bytes[:], int64(pa_start_ofs))
    if err != nil {
	Ewarn(err, "disk_init failed header WriteAt")
    }

    return err
}

/* This destroys whatever is in the ONDISK persistent array area and re-initializes it fresh */
func Disk_init(file *os.File,
		      pa_start_ofs int,
		      pa_get_bytes func (int) ([]byte, error),
		      cfg *Pa_log_config_t, freetext string) (error) {
    err := cfg_check(cfg)
    if err != nil {
	fmt.Printf("pa_log_check failed: %e\n", err)
	return err
    }

    slot_start_ofs := pa_start_ofs + int(cfg.Slot_start_ofs)
    slot_end_ofs := slot_start_ofs + int(cfg.Bytes_in_slot_area)

    err = log_header_write(file, pa_start_ofs, 0, cfg, freetext)
    if err != nil {
	return err
    }

    /* Write the slot sectors */
    sector_bytes := make([]byte, int(cfg.Bytes_per_sector))
    var seqno uint64
    var refresh_slot int

    for file_ofs := slot_start_ofs; file_ofs < slot_end_ofs; file_ofs += int(cfg.Bytes_per_sector) {
	/* Fill in sector fields and checksum */
	err := sector_finish(sector_bytes, pa_get_bytes, seqno, refresh_slot, cfg)

	/* Write the sector out to the ONDISK persistent array area */
	_, err = file.WriteAt(sector_bytes, int64(file_ofs))
	if err != nil {
	    Ewarn(err, "disk_init error on write")
	    return err
	}

	seqno++
	refresh_slot += int(cfg.Rslots_max_per_sector)
	if refresh_slot >= int(cfg.Slot_max) {
	    refresh_slot -= int(cfg.Slot_max)
	}
    }

    /* Make sure it's ONDISK before we return from here */
    file.Sync()

    return nil
}

/* Analyze static ONDISK persistent array to determine boundary between new log
 * records and old log records in the circular log.  Each sector has a write
 * sequence number embedded in it, used to find the dividing line.
 */
func (L *Pa_log_t) analyze() error {
    var oldest_sector_ofs, newest_sector_ofs int	// offset in PA.file
    var newest_last_rslot int		// last refresh slotnum in newest sector
    var newest_seqno uint64 = 0		// start at zero, adjust up
    oldest_seqno := newest_seqno - 1	// start at max64, adjust down

    /* Offset of start and (just past) the end of persistent array slots in PA.file */
    slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
    slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)

    if CHATTY {
	fmt.Printf("\nPersistent Array:\n")
	fmt.Printf("   Magic=%v version=%v bytes_per_sector=%v bytes_in_slot_area=%v\n",
		L.Cfg.Magic, L.Cfg.Version, L.Cfg.Bytes_per_sector, L.Cfg.Bytes_in_slot_area)
	fmt.Printf("   slot_max=%v slot_start_ofs=%v bytes_hdr_per_sector=%v bytes_per_slot=%v\n",
		L.Cfg.Slot_max, L.Cfg.Slot_start_ofs, L.Cfg.Bytes_hdr_per_sector, L.Cfg.Bytes_per_slot)
	fmt.Printf("   rslots_max_per_sector=%v xslots_max_per_sector=%v\n\n",
		L.Cfg.Rslots_max_per_sector, L.Cfg.Xslots_max_per_sector)
    }

    /* Find the sectors with the oldest and newest seqnos */
    sector_bytes := make([]byte, int(L.Cfg.Bytes_per_sector))

    for file_ofs := slot_start_ofs; file_ofs < slot_end_ofs;
			    file_ofs += int(L.Cfg.Bytes_per_sector) {
	/* Read the next sector */
	count, err := L.File.ReadAt(sector_bytes, int64(file_ofs))
	if err != nil /* XXX && err != EOF */ {
	    Ewarn(err, "ReadAt failed")
	    return err
	}
	if count != len(sector_bytes) {
	    Ewarn(nil, fmt.Sprintf("ReadAt short read %d/%d", count, len(sector_bytes)))
	    return EMETAREAD
	}

	err = L.sector_check(sector_bytes)
	if err != nil {
	    Ewarn(err, "sector_check failed")
	    return err
	}
	hdr := hdr_of_sector(sector_bytes)

	/* Track file sector_ofs of oldest and newest seqno sectors */
	if oldest_seqno > hdr.seqno {
	    oldest_seqno = hdr.seqno
	    oldest_sector_ofs = file_ofs
	}
	if newest_seqno < hdr.seqno {
	    newest_seqno = hdr.seqno
	    newest_sector_ofs = file_ofs
	    newest_last_rslot = int(hdr.last_rslotnum)
	}
    }

    /* Expect newest sector to occur just before oldest in log... */
    expected_oldest_sector_ofs := newest_sector_ofs + int(L.Cfg.Bytes_per_sector)
    if expected_oldest_sector_ofs >= slot_end_ofs {
	Assert_eq(expected_oldest_sector_ofs, slot_end_ofs, "expected_oldest_sector_ofs == slot_end_ofs")
	expected_oldest_sector_ofs = slot_start_ofs
    }
    if expected_oldest_sector_ofs != oldest_sector_ofs {
	fmt.Printf("expected_oldest_sector_ofs=%v oldest_sector_ofs=%v newest_sector_ofs=%v oldest_seqno=%v newest_seqno=%v\n",
		expected_oldest_sector_ofs, oldest_sector_ofs, newest_sector_ofs, oldest_seqno, newest_seqno)
	Ewarn(nil, "Uh oh... log doesn't look good")
	return EMETAREAD
    }

    /* Critical computations of current log position -- must be perfectly correct! */
    /* If these are wrong, no one will notice until it comes to a recovery attempt */
    L.sector_seqno = newest_seqno + 1

    L.file_writeofs = newest_sector_ofs + int(L.Cfg.Bytes_per_sector)
    if L.file_writeofs >= slot_end_ofs {
	L.file_writeofs = slot_start_ofs
    }

    L.refresh_slot = newest_last_rslot + 1
    if L.refresh_slot >= int(L.Cfg.Slot_max) {
	L.refresh_slot -= int(L.Cfg.Slot_max)
    }

    chat("log_analyze(): file_writeofs=%v sector_seqno=%v refresh_slot=%v", L.file_writeofs, L.sector_seqno, L.refresh_slot)

    return nil
}

/* Instantiate a persistent array logger, using persistent storage starting at
 * (file, pa_start_ofs).  The persistent array header is read here; the slots
 * themselves are read in L.analyze() and L.Read().
 */
func Open(file *os.File, pa_start_ofs int,
	    pa_get_bytes func (int) ([]byte, error)) (*Pa_log_t, error) {
    var log_hdr pa_header_block_t
    log_hdr_bytes := (*[sizeof_pa_header_block]byte)(unsafe.Pointer(&log_hdr))

    /* Create the Persistent Array Logger instance */
    L := new(Pa_log_t)
    L.File = file
    L.File_pa_ofs = pa_start_ofs
    L.pa_get_bytes = pa_get_bytes

    _, err := L.File.ReadAt(log_hdr_bytes[:], int64(pa_start_ofs))
    if err != nil {
	Ewarn(err, "open failed header ReadAt")
	return nil, err
    }

    /* Check header checksum */
    sum := crc32.ChecksumIEEE((*log_hdr_bytes)[0:offsetof_pa_header_block_checksum])
    if sum != log_hdr.checksum {
	Ewarn(nil, "open: checksum error on log_hdr")
	return nil, EMETAREAD
    }

    /* copy config from header into logger */
    L.ltime = log_hdr.ltime
    L.Cfg = log_hdr.cfg
    L.comment = string(log_hdr.pa_freetext[:])

    /* Compute maximum queue depth (in bytes) of persistent array writes, to avoid
     * overwriting the prior record for a slotnum before a new record has been
     * written for that slotnum.
     */
    L.max_write_bytes = int(L.Cfg.Bytes_per_sector) * int(L.Cfg.Bytes_in_slot_area)/int(L.Cfg.Bytes_per_sector) -
			    Div_ceil(int(L.Cfg.Slot_max), int(L.Cfg.Rslots_max_per_sector))

    if L.max_write_bytes < Pa_log_write_bytes {
	fmt.Printf("WARNING: open: small ONDISK area: %v/%v\n",
		    L.max_write_bytes, Pa_log_write_bytes)
    } else {
	L.max_write_bytes = Pa_log_write_bytes
    }

    err = L.Cfg_check()
    if err != nil {
	fmt.Printf("pa_log_check failed: %e\n", err)
	return nil, err
    }

    err = L.analyze()
    if err != nil {
	fmt.Printf("ONDISK log has errors: %e\n", err)
	return nil, err
    }

    L.Q_tx = make(chan pa_slot_op, pa_log_Q_tx_cap)
    L.Q_ser = make(chan func(), pa_log_Q_ser_cap)
    L.Q_flush = make(chan byte, 32)

    return L, nil
}

/* Read all persistent array records from disk and present them one-by-one to closure.
 * Persistent array sectors are processed in order of oldest sector sequence
 * number to newest; slots in each sector in the order they appear.
 *
 * L.analyze() already found the boundary between new and old log entries.
 * L.file_writeofs is the offset of the sector with the oldest sequence number.
 */
func (L *Pa_log_t) Read(closure func ([]byte)) error {
    if L.logging_active {
	return errors.New("Too late to call logger.Read() when logging active")
    }

    sector_bytes := make([]byte, int(L.Cfg.Bytes_per_sector))

    /********************************************************************/
    /* Process the slots present in one sector at file_ofs into PA.file */
    process_sector := func (file_ofs int) error {
	/* Read in the sector */
	count, err := L.File.ReadAt(sector_bytes[:], int64(file_ofs))
	if err != nil /* && err != EOF */ {
	    Ewarn(err, "ReadAt failed")
	    return err
	}
	if count != len(sector_bytes) {
	    Ewarn(nil, fmt.Sprintf("ReadAt short read %d/%d", count, len(sector_bytes)))
	    return EMETAREAD
	}
	if VVERBOSE {
	    fmt.Printf("ReadAt file_ofs=%v count=%v\n", file_ofs, count)
	}

	err = L.sector_check(sector_bytes)
	if err != nil {
	    Ewarn(err, "sector_check failed")
	    return err
	}
	hdr := hdr_of_sector(sector_bytes)

	/* Walk through all the slots in the sector, calling closure on each slot */
	var slot int
	for slot = 0; slot < int(hdr.rslots) + int(hdr.xslots); slot++ {
	    slot_bytes := slot_of_sector(sector_bytes, slot, &L.Cfg)
	    for i := range slot_bytes {
		if slot_bytes[i] != 0xff {  //XXXX Hacky to the Yuck
		    closure(slot_bytes)
		    break
		}
	    }
	}

	return nil
    }
    /********************************************************************/

    slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
    slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)

    /* Process pa sectors from oldest pa sector to end of the ONDISK slot area */
    for file_ofs := L.file_writeofs; file_ofs < slot_end_ofs;
		    file_ofs += int(L.Cfg.Bytes_per_sector) {
	if VVERBOSE {
	    fmt.Printf("process_sector file_ofs=%v\n", file_ofs)
	}
	err := process_sector(file_ofs)
	if err != nil {
	    return err
	}
    }

    /* Process from beginning of the ONDISK slot area to the newest pa sector */
    for file_ofs := slot_start_ofs; file_ofs < L.file_writeofs;
		    file_ofs += int(L.Cfg.Bytes_per_sector) {
	if VVERBOSE {
	    fmt.Printf("process_sector file_ofs=%v\n", file_ofs)
	}
	err := process_sector(file_ofs)
	if err != nil {
	    return err
	}
    }

    return nil
}

/********** Dynamic operation **********/

/* This serializer function simply sits here in a loop taking work request
 * closures one-by-one in order out of a channel (Q_ser) and calling them.  The
 * primary work is to write sectors of data to the ONDISK Persistent Array log.
 *
 * When Q_ser becomes empty, transmit zero on Q_flush to notify upstream.
 *
 * This function does not return until the logger is shutting down.  It should
 * be called on its own goroutine.
 */
func serializer(Q_ser chan func (), Q_flush chan byte) {
    runtime.LockOSThread()	// Take exclusive use of the OS thread
    SetThreadName("serializer")

    /* Lock the serializer thread onto one CPU */
    var new_cpu_set CPUSet
    new_cpu_set[0] = pa_serializer_cpu_mask
    err := SchedSetaffinity(0, &new_cpu_set)
    if err != nil {
	Ewarn(err, "serializer could not setaffinity()")
    }

    err = syscall.Setpriority(syscall.PRIO_PROCESS, 0, -3)
    if CHATTY && err != nil {
	Ewarn(err, "serializer could not Setpriority()")
    }

    /* Work serializer --
     * Consume req closures from Q_ser, running them one at a time in sequence.
     * If Q_ser becomes empty, send a flush request to our supplier to try to
     * provoke the arrival of more work; then sleep until more work arrives.
     */
    var req func ()
    var ok bool
    for {
	select {
	default:
	    /* Q_ser is empty */
	    Q_flush <- 0	// request our supplier to supply more input
	    req, ok = <-Q_ser	// block until another request arrives

	case req, ok = <-Q_ser:	// read a request from Q_ser
	    /* We got a request closure off of Q_ser */
	}

	if !ok {
	    if VERBOSE {
		Rusage_dump("serializer")
	    }
	    return  /* Q_ser is closed, time to go away */
	}

	/* Run the request closure */
	req()
    }
}

/* Return closure to be queued onto Q_ser for serial execution by service thread */
func write_closure(L *Pa_log_t, buf []byte, closure_list []func (error)) func () {
    file := L.File
    file_ofs := L.file_writeofs

    slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
    slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)

    if file_ofs + len(buf) > slot_end_ofs {
	fmt.Printf("************* file_ofs/buflen out of range %v + %v > %v\n", file_ofs, len(buf), slot_end_ofs)
    }

    /*******************************************************************/
    /* This function will be executed by the serializer service thread */
    return func () {
	/* Write the buffer containing pa slots to pa file at file_ofs */
	count, err := file.WriteAt(buf, int64(file_ofs))
	if err != nil {
	    Ewarn(err, "write_closure error on write")
	}
	if count != len(buf) {
	    Ewarn(nil, fmt.Sprintf("write_closure short WriteAt %d/%d", count, len(buf)))
	}
	if VVERBOSE {
	    fmt.Printf("Wrote %v to file at ofs %v %v %v %v %v\n", buf, file_ofs, buf[0], buf[1], buf[2], buf[3])
	}

	/* Ensure the pa has been actually written to disk */
	if err == nil {
	    // Without this sync the round-trip request to closure is under one microsecond.
	    // With the sync it is around 2.5 microseconds
	    if !SKIP_FLUSH_META {
		err = file.Sync()
	    }
	}

	/* Asynchronously start asynchronous execution of completion closures */
	/************************************/
	go func () {
	    for _, clo := range closure_list {
		clo(err)
	    }
	}()
	/************************************/
    }
    /*******************************************************************/
}

/* Fill in remaining sector fields and checksum */
func sector_finish(sector_bytes []byte,
			    pa_get_bytes func (int) ([]byte, error),
			    seqno uint64,
			    refresh_slot int,
			    cfg *Pa_log_config_t) (error) {
    hdr := hdr_of_sector(sector_bytes)
    hdr.version = cfg.Version
    hdr.seqno = seqno;

    /* Write all the refresh slots in the sector with data from pa table entries */
    for rslot := 0; rslot < int(cfg.Rslots_max_per_sector); rslot++ {
	slot_bytes, err := pa_get_bytes(refresh_slot);
	if err != nil {
	    Ewarn(err, "log_sector_finish failed pa_get_bytes()")
	    return err
	}
	hdr.last_rslotnum = uint64(refresh_slot)
	refresh_slot++
	if refresh_slot >= int(cfg.Slot_max) {
	    /* wrap refresh slotnum back to zero after reaching slot_max */
	    Assert_eq(refresh_slot, int(cfg.Slot_max))
	    refresh_slot = 0
	}
	copy(slot_of_sector(sector_bytes, rslot, cfg), slot_bytes)
    }
    hdr.rslots = cfg.Rslots_max_per_sector

    /* compute and set sector checksum */
    hdr.sector_checksum = crc32.ChecksumIEEE(sector_bytes[4:int(cfg.Bytes_hdr_per_sector)])

    return nil
}

/* If limited is false, remove every pending transaction from Q_tx and
 * accumulate it into buffer(s).  If limited is true, may stop after at least
 * one buffer has been released to the write queue (Q_ser).
 * Returns true if it got something off of Q_tx; otherwise false.
 */
func buf_fill(L *Pa_log_t, limited bool) (got_some bool) {
    for {
	select {
	case op := <-L.Q_tx:
	    got_some = true
	    buf_accum(L, op.data, op.closure)
	    if limited && L.Buf_empty() {
		return
	    }
	default:    // Q_tx is empty
	    return
	}
    }
}

/* Process a request to flush or close the logger --
 * When this function returns, transactions have been queued onto Q_ser, but
 * not necessarily processed from there.
 *
 * flag 0x00: downstream is starved, try to get something into Q_ser
 * flag 0x01: flush everything from Q_tx into Q_ser
 * flag 0xff: As (flag 0x01), then close Q_tx and Q_ser and goexit the logger
 */
func buf_flush(L *Pa_log_t, flag byte) {
    switch flag {
    case 0:
	L.nFlush_0++
    case 1:
	L.nFlush_1++
    default:
	L.nFlush_x++
    }

    /* Process requests from Q_tx into Q_ser */
    got_some := buf_fill(L, flag == 0)

    /* If serializer called flush but there is nothing to send, remember he is starving */
    if flag == 0 && !got_some && L.Buf_empty() {
	L.starving = true
	L.nStarve++
	return
    }

    /* If the buffer is non-empty then it is a partial buffer which we will now
     * process into Q_ser in satisfaction of the flush request.
     *
     * Note when flag == 0, buf_fill stopped as soon as it triggered a
     * write (resulting in an empty buffer).  In that case we have accomplished
     * the purpose of relieving starvation in the serializer.
     */

    /* Write any partial buffer remaining */
    if !L.Buf_empty() {
	/* Check the buffer for a partially-filled sector */
	sector_bytes := L.buffer[L.buf_sector_ofs:L.buf_sector_ofs+int(L.Cfg.Bytes_per_sector)]
	hdr := hdr_of_sector(sector_bytes)
	if hdr.xslots > 0 {
	    /* There are some transactions pending in this sector -- finish it */
	    sector_finish(sector_bytes, L.pa_get_bytes, L.sector_seqno, L.refresh_slot, &L.Cfg)
	    L.sector_seqno++
	    L.refresh_slot += int(L.Cfg.Rslots_max_per_sector)
	    if L.refresh_slot >= int(L.Cfg.Slot_max) {
		L.refresh_slot -= int(L.Cfg.Slot_max)
	    }
	    L.buf_sector_ofs += int(L.Cfg.Bytes_per_sector)
	}

	slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
	slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)

	/* Send the buffer and its associated closure list to the serializer */
	L.Q_ser <- write_closure(L, L.buffer[0:L.buf_sector_ofs], L.closure)
	L.nQ_ser_flush++

	/* Track high-watermark of Q_ser */
	l := len(L.Q_ser)
	if L.hQ_ser < l {
	    L.hQ_ser = l
	}

	L.ser_flush_size_cum += L.buf_sector_ofs
	L.file_writeofs += L.buf_sector_ofs
	if L.file_writeofs >= slot_end_ofs {
	    L.file_writeofs = slot_start_ofs
	}
	L.buffer, L.closure = nil, nil
	L.buf_sector_ofs = 0
	L.buf_xslot_ofs = 0
	L.starving = false
    }

    /* Check for close request */
    if flag == 0xff {
	close(L.Q_ser)
	close(L.Q_tx)
	L.logging_active = false
	if VERBOSE {
	    Rusage_dump("logger")
	}
	runtime.Goexit()
    }
}

/* Accumulate a transaction tx_bytes into the log write buffer.
 * If closure is non-nil, it closure will be called when the eventual write to
 * disk has completed (with sync).
 */
func buf_accum(L *Pa_log_t, tx_bytes []byte, closure func (error)) {
    Assert_eq(len(tx_bytes), int(L.Cfg.Bytes_per_slot))
    L.nAccum++

    /* Allocate a buffer, if none */
    if L.buffer == nil {
	Assert(L.closure == nil)
	slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
	slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)
	max_write := L.max_write_bytes
	if max_write > slot_end_ofs - L.file_writeofs {
	    max_write = slot_end_ofs - L.file_writeofs
	}

	L.buffer = make([]byte, max_write)
	L.closure = make([]func (error)(), 0, max_write/int(L.Cfg.Bytes_per_sector) * int(L.Cfg.Xslots_max_per_sector))
	L.buf_sector_ofs = 0	// offset of current sector in buffer
				// offset of current xslot in buffer
	L.buf_xslot_ofs = L.buf_sector_ofs + int(L.Cfg.Bytes_hdr_per_sector) +
				    int(L.Cfg.Rslots_max_per_sector) * int(L.Cfg.Bytes_per_slot)
    }

    /* Copy the incoming slot data into the current slot in L.buffer */
    Assert(len(L.buffer) >= L.buf_xslot_ofs + int(L.Cfg.Bytes_per_slot))
    copy(L.buffer[L.buf_xslot_ofs : L.buf_xslot_ofs + int(L.Cfg.Bytes_per_slot)], tx_bytes)
    L.buf_xslot_ofs += int(L.Cfg.Bytes_per_slot)	// advance to the next slot position

    /* append the associated incoming completion closure onto the L.closure list */
    if closure != nil {
	L.closure = append(L.closure, closure)
    }

    sector_bytes := L.buffer[L.buf_sector_ofs:L.buf_sector_ofs+int(L.Cfg.Bytes_per_sector)]
    hdr := hdr_of_sector(sector_bytes)

    /* Increment the count of xslots used in this sector */
    hdr.xslots++

    /* If all xslots are consumed in this sector, finish it */
    if int(hdr.xslots) >= int(L.Cfg.Xslots_max_per_sector) {
	/* Sector has all its xslots full */
	Assert_eq(hdr.xslots, int(L.Cfg.Xslots_max_per_sector), "too many xslots")
	/* Fill in remaining sector fields and checksum */
	sector_finish(sector_bytes, L.pa_get_bytes, L.sector_seqno, L.refresh_slot, &L.Cfg)

	/* Advance sector seqno and check for wrap of refresh_slot */
	L.sector_seqno++
	L.refresh_slot += int(L.Cfg.Rslots_max_per_sector)
	if L.refresh_slot >= int(L.Cfg.Slot_max) {
	    L.refresh_slot -= int(L.Cfg.Slot_max)
	}

	/* Advance to the next sector position in the buffer */
	L.buf_sector_ofs += int(L.Cfg.Bytes_per_sector)
	L.buf_xslot_ofs = L.buf_sector_ofs + int(L.Cfg.Bytes_hdr_per_sector) +
			int(L.Cfg.Rslots_max_per_sector) * int(L.Cfg.Bytes_per_slot)
    }

    slot_start_ofs := L.File_pa_ofs + int(L.Cfg.Slot_start_ofs)
    slot_end_ofs := slot_start_ofs + int(L.Cfg.Bytes_in_slot_area)

    /* Check if buffer is full */
    if L.buf_sector_ofs >= len(L.buffer) {
	Assert_eq(L.buf_sector_ofs, len(L.buffer))
	/* Send the buffer to the serializer to be written */
	L.Q_ser <- write_closure(L, L.buffer, L.closure)
	L.nQ_ser_write++

	/* Track high-watermark of Q_ser */
	if L.hQ_ser < len(L.Q_ser) {
	    L.hQ_ser = len(L.Q_ser)
	}

	/* Advance to the next write position in the file */
	L.file_writeofs += L.buf_sector_ofs
	if L.file_writeofs >= slot_end_ofs {
	    L.file_writeofs = slot_start_ofs
	}

	/* Release the buffer and closure list */
	L.buffer, L.closure = nil, nil
	L.buf_sector_ofs = 0
	L.buf_xslot_ofs = 0
	L.starving = false
    }
}

/* Make the logger active --
 * This function does not return.  It should be called on its own goroutine.
 */
func (L *Pa_log_t) Logger() error {
    runtime.LockOSThread()	// Take exclusive use of the OS thread
    SetThreadName("logger")
    L.logging_active = true

    /* Lock the logger thread onto one CPU */
    var new_cpu_set CPUSet
    new_cpu_set[0] = pa_logger_cpu_mask
    err := SchedSetaffinity(0, &new_cpu_set)
    if err != nil {
	Ewarn(err, "Logger could not setaffinity()")
    }

    err = syscall.Setpriority(syscall.PRIO_PROCESS, 0, -2)
    if CHATTY && err != nil {
	Ewarn(err, "logger could not Setpriority()")
    }

    /* Start the serializer server */
    go serializer(L.Q_ser, L.Q_flush)

    /* Read logging requests from Q_tx and flush/close requests from Q_flush,
     * and process accordingly.  buf_flush() will runtime.Goexit() if
     * flag is sufficiently high.
     */
    for {
	select {
	case flag := <-L.Q_flush:
	    buf_flush(L, flag)
	case op := <-L.Q_tx:
	    buf_accum(L, op.data, op.closure)
	    if (L.starving && len(L.Q_tx) == 0) {
		/* Get out whatever we can to the serializer */
		buf_flush(L, 0)
	    }
	}
    }
}

/* Send a request to log a change to the Persistent Array */
func (L *Pa_log_t) Tx(txbytes []byte, closure func (error)) error {
    var op pa_slot_op
    op.data = txbytes
    op.closure = closure
    L.Q_tx <- op
    return nil
}

/* Send a flush request to the logger -- there is no reply
 * The logger will process anything pending on Q_tx into Q_ser
 */
func (L *Pa_log_t) Flush() {
    L.Q_flush <- 0x01
}

/* Send a flush closure and wait for it to complete */
func (L *Pa_log_t) Flush_wait() {
    /* Fake transaction to flush through those before it */
    var flush_txbytes = make([]byte, int(L.Cfg.Bytes_per_slot))
    for i := range flush_txbytes {
	flush_txbytes[i] = 0xff
    }

    /* When this "plunger" closure reaches head of Q_ser, we know everything ahead of it has completed */
    Q_flush_done := make(chan bool, 1)
    closure := func (error) {
		    /* Tell our waiter we've reached head of queue and executed */
		    Q_flush_done <- true
	       }
    L.Tx(flush_txbytes, closure)

    /* Initiate processing of anything pending on Q_tx into Q_ser */
    L.Q_flush <- 0x01
    L.nQ_ser_flushwait++

    /* Now wait to receive notification that our plunger reached head of queue */
    <-Q_flush_done
}

/* Send a close request to the logger */
func (L *Pa_log_t) Close() {
    L.Q_flush <- 0xff
}

/* Fetch the current ltime */
func (L *Pa_log_t) Ltime() int {
    return L.ltime
}

/* Record a new ltime */
func (L *Pa_log_t) Ltime_set(new_ltime int) error {
    if new_ltime == L.ltime {
	return nil	    // already there
    }
    if new_ltime < L.ltime {
	Ewarn(ESTALELTIME, "%v $v", new_ltime, L.ltime)
	return ESTALELTIME  // can't go backwards
    }
    L.ltime = new_ltime
    return log_header_write(L.File, L.File_pa_ofs, L.ltime, &L.Cfg, L.comment)
}
