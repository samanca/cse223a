package triblab
import . "trib"
import "encoding/json"
import "log"

func Sync(backend string, replicas []string, chord *Chord, c *chan bool) {
	e := _sync(backend, replicas, chord)
	*c<-(e == nil)
}

/*
 * TODO test basic operation (without any failures)
 * TODO handle keeper failures (log buffer at BackEnds)
 * TODO GC before return (close connections)
 * TODO we might be able to apply cached opLog on Primary Failure
 */
func _sync(backend string, replicas []string, chord *Chord) error {

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

			// 2.5 - is it mine?
			owner, err := chord.getIPbyBinName(removeNS(op.data.Key))

			/*
			 * if the current node is the owner, I just need to replicate data to the next and next_next node
			 * otherwise, the previous node should be the owner, so I should write it to the previous and next node
			 */

			if err != nil {
				log.Print("error getting bin name: %s", err)
			}

			if owner == backend {

				// 3 - Replicate
				for r := range rs {
					e = _doWhatISay(rs[r], &op)
					if e != nil { return e }
				}

				// 4 - Perform operation (we already have the data on the backend)
				//e = _doWhatISay(be, &op)
				//if e != nil { return e }
			} else {

				// 2.75 - Move it to the right place
				var b bool
				t := &client{ addr: owner }
				e = t.ListAppend(&KeyValue{ Key: LOG_KEY, Value: opLog.L[i]}, &b)
				// TODO optimizing this by maintaining a connection to previous node
			}
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
