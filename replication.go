package triblab
import . "trib"
import "fmt"
import "log"
import "time"

const (
	PRIMARY = 0
	REPLICA_1 = 1
	REPLICA_2 = 2
)

/*
 * TODO check for race conditions while accessing Chord
 * TODO use locking to avoid data re-ordering on replica leave/join
 */
type ReplicationService struct {
	chord *Chord
}

// Background replication service (only for OpLog)
func (self *ReplicationService) run() error {

	var e error

	for {
		// sleep
		time.Sleep(1 * time.Second)

		// 1.0 - list of active back-ends
		live_back_ends, err := self.chord.listAllActiveNodes()
		if err != nil {
			log.Print("unable to get the list of active nodes: %s", err)
			continue
		}

		// 1.5 - don't replicate data to nodes in locked state (in progress replication)
		// TODO remove primary nodes from back-ends list

		// 2.0 - initialize channel
		c := make(chan bool)

		// 3.0 - create concurrent sync threads
		for i := range live_back_ends {

			replicas := make([]string, 2)

			replicas[0], e = self.chord.Succ_node_ip(live_back_ends[i])
			if e != nil { c<-false; continue }

			replicas[1], e = self.chord.Succ_node_ip(replicas[0])
			if e != nil { c<-false; continue }

			go Sync(live_back_ends[i], replicas, &c)
		}

		// 4.0 - wait for join
		var succ int = 0
		for i := 0; i < len(live_back_ends); i++ {
			if (<-c) { succ++ }
		}

		// 5.0 - log replication statistics
		log.Print("background replication: %d / %d", succ, len(live_back_ends))
	}
	return fmt.Errorf("unexcpected behavior in replication service!")
}

func (self *ReplicationService) garbageCollector() {
	// TODO
}

func (self *ReplicationService) _cpValues(c *chan bool, source, dest, reference string) {

	var keys List
	var err error
	var value string
	var b, anyFailure bool

	s_conn := &client{ addr: source }
	d_conn := &client{ addr: dest }
	p := &Pattern{ Prefix: "*", Suffix: "*" }

	err = s_conn.Keys(p, &keys)
	if err != nil { *c<-false; return }

	anyFailure = false
	for i := range keys.L {

		// filter
		primary_copy, e := self.chord.getIPbyBinName(keys.L[i])
		if e !=  nil {
			log.Print("error mapping user to bin: %s", e)
			continue
		}
		if primary_copy != reference {
			continue // TODO mark it for garbage collection if it's not a replica
		}

		err = s_conn.Get(keys.L[i], &value)
		if err != nil { anyFailure = true; continue }
		err = d_conn.Set(&KeyValue{ Key: keys.L[i], Value: value }, &b)
		if err !=  nil || b == false { anyFailure = true }
	}

	*c<-(!anyFailure)
}

func (self *ReplicationService) _cpLists(c *chan bool, source, dest, reference string) {

	var lists, buffer List
	var err error
	var b, anyFailure bool

	s_conn := &client{ addr: source }
	d_conn := &client{ addr: dest }
	p := &Pattern{ Prefix: "*", Suffix: "*" }

	err = s_conn.ListKeys(p, &lists)
	if err != nil { *c<-false; return }

	anyFailure = false
	for i := range lists.L {

		// filter
		primary_copy, e := self.chord.getIPbyBinName(lists.L[i])
		if e !=  nil {
			log.Print("error mapping user to bin: %s", e)
			continue
		}
		if primary_copy != reference {
			continue // TODO mark it for garbage collection if it's not a replica
		}

		// TODO avoid replicating other metadata information
		if lists.L[i] == LOG_KEY { continue }

		err = s_conn.ListGet(lists.L[i], &buffer)
		if err != nil { anyFailure = true; continue }
		for j := range buffer.L {
			err = d_conn.ListAppend(&KeyValue{ Key: lists.L[i], Value: buffer.L[j]}, &b)
			if err !=  nil || b == false { anyFailure = true }
		}
	}

	*c<-(!anyFailure)
}

func (self *ReplicationService) replicate(c *chan bool, source, dest string) {
	self.replicateThrough(c, source, dest, source)
}

/*
 * reading [source] data from [tp] and replicating it to [dest]
 */
func (self *ReplicationService) replicateThrough(c *chan bool, source, dest, tp string) {

	channel := make(chan bool)

	// TODO lock

	// concurrent copy
	go self._cpValues(&channel, tp, dest, source)
	go self._cpLists(&channel, tp, dest, source)

	// wait for join
	var succ int = 0
	if (<-channel) { succ++ }
	if (<-channel) { succ++ }

	// TODO unlock

	// TODO
	*c<-(succ == 2)
}

// TODO@Vineet Should be invoked after reflecting the change in Chord
func (self *ReplicationService) notifyJoin(node string) error {
	// TODO
	var err error
	var prev_prev, prev, next string

	// query Chord
	prev, err = self.chord.Prev_node_ip(node)
	if err != nil { return err }
	prev_prev, err = self.chord.Prev_node_ip(prev)
	if err != nil { return err }

	next, err = self.chord.Succ_node_ip(node)
	if err != nil { return err }

	if prev == EMPTY_STRING {
		return nil // nothing to do as |Chord| < 2
	}

	// init channel
	c := make(chan bool)

	go self.replicateThrough(&c, node, node, next)
	go self.replicate(&c, prev, node)
	if prev_prev != prev {
		go self.replicate(&c, prev_prev, node)
	} else {
		c<-true
	}

	// wait for join
	var succ int = 0
	for i := 0; i < 3; i++ {
		if (<-c) { succ++ }
	}

	// report garbage
	// TODO

	if succ == 3 {
		return nil
	} else {
		return fmt.Errorf("%d replication(s) failed!", (3 - succ))
	}
}

// TODO refactor (too much duplicated code)
// TODO@Vineet Should be invoked before reflecting node failure in Chord
func (self *ReplicationService) notifyLeave(node string) error {

	var err error
	var prev_prev, prev, next, next_next, next_next_next string

	// query Chord
	prev, err = self.chord.Prev_node_ip(node)
	if err != nil { return err }
	prev_prev, err = self.chord.Prev_node_ip(prev)
	if err != nil { return err }

	next, err = self.chord.Succ_node_ip(node)
	if err != nil { return err }
	next_next, err = self.chord.Succ_node_ip(next)
	if err != nil { return err }

	if prev == EMPTY_STRING || prev == next || prev_prev == next {
		return nil // nothing to do as |Chord| < 4
	}

	// init channel
	c := make(chan bool)

	// parallel replication
	go self.replicateThrough(&c, node, next_next_next, next)
	go self.replicate(&c, prev, next_next)
	go self.replicate(&c, prev_prev, next)

	// wait for join
	var succ int = 0
	for i := 0; i < 3; i++ {
		if (<-c) { succ++ }
	}

	// report garbage
	// TODO

	if succ == 3 {
		return nil
	} else {
		return fmt.Errorf("%d replication(s) failed!", (3 - succ))
	}
}
