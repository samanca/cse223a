package triblab
import . "trib"
import "encoding/json"

func Sync(backend string, replicas []string, c *chan bool) {
	e := _sync(backend, replicas)
	*c<-(e == nil)
}

/*
 * TODO test basic operation (without any failures)
 * TODO handle keeper failures (log buffer at BackEnds)
 * TODO GC before return (close connections)
 * TODO we might be able to apply cached opLog on Primary Failure
 */
func _sync(backend string, replicas []string) error {

	// Backend RPC handler
	be := &client{ addr: backend }
	rs := make([]*client, len(replicas))
	for i := range replicas {
		rs[i] = &client{ addr: replicas[i] }
	}

	// 1 - Retrieve OpLog
	var opLog List
	e := be.ListGet(LOG_KEY, &opLog)
	if e != nil { return e }

	for i := range opLog.L {

		// 2 - Decode OpLog entry
		var op OpLogEntry
		e = json.Unmarshal([]byte(opLog.L[i]), &op)
		if e == nil {

			// 3 - Replicate
			for r := range rs {
				e = _doWhatISay(rs[r], &op)
				if e != nil { return e }
			}

			// 4 - Perform operation
			e = _doWhatISay(be, &op)
			if e != nil { return e }
		}

		// 5 - Remove log entry
		var n int
		e = be.ListRemove(&KeyValue{ Key: LOG_KEY, Value: opLog.L[i] }, &n)
		if e != nil { return e }
	}
	return nil
}

/*
 * TODO do we need to have &b and &n outside this method?
 */
func _doWhatISay(c *client, o *OpLogEntry) error {

	var b bool
	var n int

	switch {
	case o.opCode == OP_SET:
		return c.Set(&o.data, &b)
	case o.opCode == OP_LIST_APPEND:
		return c.ListAppend(&o.data, &b)
	case o.opCode == OP_LIST_REMOVE:
		return c.ListRemove(&o.data, &n)
	}

	return nil;
}
