package triblab
import . "trib"
import "encoding/json"
/*
 * TODO test basic operation (without any failures)
 * TODO test if the method returns on the event of failure
 * TODO handle keeper failures (log buffer at BackEnds)
 * TODO we need to have persistent connections to back-ends
 * TODO GC before return (close connections)
 */
func run(backend string, replicas []string) error {

	// Backend RPC handler
	be := &client{ addr: backend }
	rs := make([]*client, len(replicas))
	for i := range replicas {
		rs[i] = &client{ addr: replicas[i] }
	}

	for {
		// 1 - Retrieve OpLog
		var opLog List
		e := be.ListGet(LOG_KEY, &opLog)
		if e != nil { return e }

		for i := range opLog.L {

			// 2 - Decode OpLog entry
			var op OpLogEntry
			e = json.Unmarshal([]byte(opLog.L[i]), &op)
			if e != nil {

				// 3 - Replicate
				for r := range rs {
					e = doWhatISay(rs[r], &op)
					if e != nil { return e }
				}

				// 4 - Perform operation
				e = doWhatISay(be, &op)
				if e != nil { return e }
			}

			// 5 - Remove log entry
			var n int
			e = be.ListRemove(&KeyValue{ Key: LOG_KEY, Value: opLog.L[i] }, &n)
			if e != nil { return e }
		}
	}
}

/*
 * TODO do we need to have &b and &n outside this method?
 */
func doWhatISay(c *client, o *OpLogEntry) error {

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
