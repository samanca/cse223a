package triblab

const (
	LOG_KEY = "OPLOG"

	OP_SET = 0 // opCode for Get()
	OP_LIST_APPEND = 1 // opCode for ListAppend()
	OP_LIST_REMOVE = 2 // opCode for ListRemove()
)

/*
 * Our OpLog will be a list of encoded OpLogEntries, ordered by Primary Time
 */
type OpLogEntry struct {
	opCode uint // OP_SET | OP_LIST_APPEND | OP_LIST_REMOVE
	data KeyValue
}

// TODO helper functions (e.g. encoder and decoder)
