package triblab
//import . "trib"
import "fmt"
import "log"

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
		// 1.0 - list of active back-ends
		live_back_ends := make([]string, 0) // chord.live()

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

func (self *ReplicationService) x(bin string) {

}

func (self *ReplicationService) replicate(source, dest string) {

}

func (self *ReplicationService) replicateThrough(source, dest, x string) {

}

func (self *ReplicationService) garbageCollector() {

}

func (self *ReplicationService) notifyJoin(node string) {

}

func (self *ReplicationService) notifyLeave(add string) {
	/*
	var prev_prev, prev, next, next_next, next_next_next string

	// Query Chord

	if prev == next || prev_prev == next {
		return // nothing to do as |Chord| < 4
	}

	go self.replicateThrough(node, next_next_next, next)
	go self.replicate(prev, next_next)
	go self.replicate(prev_prev, next)
	*/
}
