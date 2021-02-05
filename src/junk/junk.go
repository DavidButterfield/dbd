package junk

import "fmt"
import .    "golang.org/x/sys/unix"
import "os"
import "runtime"
import "runtime/debug"
import "strings"
import "sync/atomic"
import "time"
import "unsafe"

var CHATTY bool
var VERBOSE bool

func integer(a interface{}) uint64 {
    switch T := a.(type) {
    case uint64:
        return uint64(T)
    case int64:
        return uint64(T)
    case uint32:
        return uint64(T)
    case int32:
        return uint64(T)
    case uint16:
        return uint64(T)
    case int16:
        return uint64(T)
    case uint8:
        return uint64(T)
    case int8:
        return uint64(T)
    case int:
        return uint64(T)
    case uint:
        return uint64(T)
    default:
        panic(fmt.Sprintf("****** BAD integer TYPE %#v", a))
    }
}

func Div_ceil(numerator, denominator interface{}) int {
    return int((integer(numerator) + integer(denominator) - 1) / integer(denominator))
}

func Stackline() (ret string) {
    ret = "STACK: "
    for skip := 1 ; ; skip++ {
        _, file, line, ok := runtime.Caller(skip)
        if !ok {
            return
        }
        ret = fmt.Sprintf("%s %v:%v ", ret, file, line)
    }
}

func Catch(f func()) {
    defer func() {
	plight := recover()
	if plight != nil {
	    fmt.Printf("Caught panic: %v", plight)
	}
    }()
    f()
}

func assert_finish(str ... string) {
    var foo string
    for _, s := range str {
	foo = fmt.Sprintf("%s%s ", foo, s)
    }
    fmt.Printf("%s\n%v\n", foo, Stackline())
    debug.PrintStack()
    panic(foo)
}

func Assert(cond bool, str ... string) {
    if !cond {
	fmt.Printf("ASSERTION FAILED: ")
	assert_finish(str...)
    }
}

func Assertz(a interface{}, str ... string) {
    if integer(a) != 0 {
	fmt.Printf("ASSERTION FAILED: Assertz(%v): ", a)
	assert_finish(str...)
    }
}

func Assert_eq(a interface{}, b interface{}, str ... string) {
    if integer(a) != integer(b) {
	fmt.Printf("ASSERTION FAILED: assert_eq(%v, %v): ", a, b)
	assert_finish(str...)
    }
}

func Ewarn(err error, msg string, args... interface{}) {
    if err != nil {
	fmt.Printf("WARNING " + err.Error() + ": " + msg + "\n", args...)
    } else {
	fmt.Printf("WARNING: " + msg + "\n", args...)
    }
}

func emit(str string) {
    fmt.Printf("%v: %s", time.Now().Format("15:04:05.000000"), str)
}

func Verbose(str string, args... interface{}) {
    if VERBOSE {
	emit(fmt.Sprintf("    " + str + "\n", args...))
    }
}

func Chat(str string, args... interface{}) {
    if CHATTY {
	emit(fmt.Sprintf("  " + str + "\n", args...))
    }
}

func Notify(str string, args... interface{}) {
    emit(fmt.Sprintf("> " + str + "\n", args...))
}

func Warn(str string, args... interface{}) {
    emit(fmt.Sprintf("! " + str + "\n", args...))
}

func Rusage_dump(str string) {
    /* Dump out the rusage for this thread */
    var rusage Rusage
    err := Getrusage(RUSAGE_THREAD, &rusage)
    if err != nil {
	fmt.Printf("Could not get rusage: %v\n", err)
    } else {
	fmt.Printf("rusage (%s):\n%#v\n", str, rusage)
    }
    fmt.Printf("NumGoroutine=%v\n", runtime.NumGoroutine())
    fmt.Printf("\n")
}

/* Return true if the hostname refers to the local host */
func IsLocalHost(hostname string) bool {
    if hostname == "" {
	return true	// blank hostname means local host
    }
    my_hostname, err := os.Hostname()
    if err != nil {
	return false
    }
    if !strings.ContainsAny(my_hostname, ".") {
        // If it's just a machine name, add ".local" to the end of it
        my_hostname += ".local"
    }
    if hostname == my_hostname {
	return true
    }
    return false
}

func SetThreadName(name string) {
    bytes := ([]byte)(name)
    arg2 := uintptr(unsafe.Pointer(&bytes[0]))
    err := Prctl(PR_SET_NAME, arg2, 0, 0, 0)
    if err != nil {
	fmt.Printf("Could not set threadname to %v\n", name)
    }
    // fmt.Printf("Set threadname to %v\n", name)
}

func Atomic_bit_set_32(addr *uint32, bit_set_val uint32) {
    for {
	old_val := *addr
	new_val := old_val | bit_set_val
	if atomic.CompareAndSwapUint32(addr, old_val, new_val) {
	    break
	}
    }
}

func Atomic_bit_clr_32(addr *uint32, bit_clr_val uint32) {
    for {
	old_val := *addr
	new_val := old_val &^ bit_clr_val
	if atomic.CompareAndSwapUint32(addr, old_val, new_val) {
	    break
	}
    }
}

func Atomic_bit_tst_32(addr *uint32, bit_tst_val uint32) bool {
    return (atomic.LoadUint32(addr) & bit_tst_val) != 0
}

func Atomic_bit_fetch_32(addr *uint32) uint32 {
    return atomic.LoadUint32(addr)
}
