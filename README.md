# Distributed Block Device

**Distributed Block Device (dbd)**
A block device implementing one or more mirrors on remote hosts
*David A. Butterfield*

This project implements a replicated (mirrored) block device in about 3200
lines of code in the go programming language.

cgo is used to interface the tcmu-runner handler dbd.c to the dbd
implementation.  The device can then be managed using targetcli(8).

Components:
  storage_main.go -- main program initializes and then calls tcmu_main()
  storage_node.go -- generic storage routing, and RPC implementation
  mirror.go	  == mirroring storage routing module (duplicates I/O)
  store.go	  -- storage node issues actual I/O to the backend device
  pa_logger.go	  -- persistent array logger records metadata on disk
  junk.go	  -- misc trivial utility functions

Moving from C to Go reminds me of how it felt moving from assembly language to C.
