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
 */
type ReplicationService struct {
	_chord *Chord
}

// Background replication service (only for OpLog)
func (self *ReplicationService) run() error {

	var e error

	for {
		// sleep
		time.Sleep(1 * time.Second)

		// 1 - list of active back-ends
		live_back_ends, err := self._chord.listAllActiveNodes()
		if err != nil {
			log.Printf("unable to get the list of active nodes: %s", err)
			continue
		}
		/*
		for i := range live_back_ends {
			log.Printf("--%s", live_back_ends[i])
		}
		*/

		// 2 - initialize channel
		c := make(chan bool, len(live_back_ends))

		// 3 - create concurrent sync threads
		for i := range live_back_ends {

			replicas := make([]string, 2)

			replicas[0], e = self._chord.Succ_node_ip(live_back_ends[i])
			if e != nil {
				log.Printf("error while getting next: %s", e)
				c<-false; continue
			}
			//log.Printf("next for %s is %s", live_back_ends[i], replicas[0])

			replicas[1], e = self._chord.Succ_node_ip(replicas[0])
			if e != nil {
				log.Printf("error while getting next_next: %s", e)
				c<-false; continue
			}
			//log.Printf("next_next for %s is %s", live_back_ends[i], replicas[1])

			go Sync(live_back_ends[i], replicas, self._chord, &c)
			//log.Printf("Sync thread created for %s", live_back_ends[i])
		}

		// 4 - wait for join
		var succ int = 0
		//log.Printf("waiting for Sync threads ...")
		for i := 0; i < len(live_back_ends); i++ {
			if (<-c) { succ++ }
			//log.Printf("one returned!")
		}

		// 5 - log replication statistics
		log.Printf("background replication: %d / %d", succ, len(live_back_ends))
	}
	return fmt.Errorf("unexcpected behavior in replication service!")
}

func (self *ReplicationService) garbageCollector() {
	// TODO
}

// TODO avoid copying keys with empty value to destination
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
		primary_copy, e := self._chord.getIPbyBinName(extractNS(keys.L[i]))
		if e !=  nil {
			log.Printf("error mapping user to bin: %s", e)
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

// TODO avoid copying empty lists to destination
func (self *ReplicationService) _cpLists(c *chan bool, source, dest, reference string) {

	var lists, buffer List
	var err error
	var b, anyFailure bool

	s_conn := &client{ addr: source }
	d_conn := &client{ addr: dest }
	p := &Pattern{ Prefix: "", Suffix: "" }

	err = s_conn.ListKeys(p, &lists)
	if err != nil { *c<-false; return }

	log.Printf("cpLists for %s of size %d from %s to %s", reference, len(lists.L), source, dest)

	anyFailure = false
	for i := range lists.L {

		// filter
		primary_copy, e := self._chord.getIPbyBinName(extractNS(lists.L[i]))
		if e !=  nil {
			log.Printf("error mapping user to bin: %s", e)
			continue
		}

		log.Printf("primary_copy = %s (%s)", primary_copy, lists.L[i])

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

	channel := make(chan bool, 2)

	// concurrent copy
	go self._cpValues(&channel, tp, dest, source)
	go self._cpLists(&channel, tp, dest, source)

	// wait for join
	var succ int = 0
	if (<-channel) { succ++ }
	if (<-channel) { succ++ }

	*c<-(succ == 2)
}

// TODO@Vineet Should be invoked after reflecting the change in Chord
func (self *ReplicationService) notifyJoin(chord *ChordMiniSnapshot) error {

	if chord.ofSizeOne() {
		log.Printf("notifyJoin for |chord| = 1")
		return nil // nothing to do as |Chord| < 2
	}

	// init channel
	c := make(chan bool, 3)

	log.Printf("starting background replicaiton for join ...")

	go self.replicateThrough(&c, chord.me, chord.me, chord.next)
	go self.replicate(&c, chord.prev, chord.me)
	if chord.ofSizeTwo() {
		c<-true
	} else {
		go self.replicate(&c, chord.prev_prev, chord.me)
	}

	// wait for join
	var succ int = 0
	for i := 0; i < 3; i++ {
		if (<-c) { succ++ }
	}

	log.Printf("finished with background replicaiton for join ...")

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
func (self *ReplicationService) notifyLeave(chord *ChordMiniSnapshot) error {

	if chord.smallerThanFour() {
		return nil // nothing to do as |Chord| < 4
	}

	// init channel
	c := make(chan bool, 3)

	// parallel replication
	go self.replicateThrough(&c, chord.me, chord.next_next_next, chord.next)
	go self.replicate(&c, chord.prev, chord.next_next)
	go self.replicate(&c, chord.prev_prev, chord.next)

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
