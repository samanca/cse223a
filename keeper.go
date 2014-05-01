package triblab
import (
	. "trib"
	"time"
	"fmt"
	"log"
	"errors"
)

const (
	ALIVE = 1
	SILENT = 2
	DEAD = 3
)

type worker struct {
	address		string
	lastAck		uint64
	status		int
	silentDur	int
	handler		*client
}

func (w worker) String() string {
	return fmt.Sprintf("%s (%d, %d)", w.address, w.lastAck, w.status)
}

type keeper struct {
	config		*KeeperConfig
	workers		[]worker
}


//Maintain Node and Ring for the Chord Ring information
type node struct {
    succ string //succ ip
    prev string //prev ip
    ip string //its own ip
    start uint32 //start of its arc
    end uint32 //end of its arc onr ring
}

//Initially there is no node in the ring
var ring []node

//TODO-return ??
//TODO- is the hash value returned uitn32?
func addNodetoRing(ip string, ring []node, next *string, prev *string){
    var Node node
    Node.ip = ip
    val := getHash(ip)
    if (len(ring)==0){
        //TODO-Is this empty string?
        Node.succ = ""
        Node.prev = ""
        Node.start = 0
        Node.end = 2^(32) -1 //TODO-better way to write this
    } else{
    for i:=0;i<len(ring);i++{
        if val > ring[i].start && val < ring[i].end{
            Node.succ = ring[i].succ
            Node.prev = ring[i].ip
            Node.start = ring[i].end
            Node.end = val
            //Fix the predecessor
            ring[i].succ = Node.ip
            break
        }else {
            fmt.Errorf("some error, the node must be inserted somewhere")
        }
    }
    //Fix the successor's arc and prev value
    for j:=0;j<len(ring);j++{
        if ring[j].ip==Node.succ{
            ring[j].prev = Node.ip
            ring[j].start = val //TODO-or os this val + 1?
            break
        }
    }
}
    ring = append(ring, Node) //TODO-is this how you use append
}


func removeNodefromRing(ip string, ring []node, next *string, prev *string){
    //assert(len(ring)!=0)

    for i:=0;i<len(ring);i++{
        if ip==ring[i].ip{
            //we have found our node, need to modify the relevant succ and prev values in ring
            //and remove the node

            for j:=0;j<len(ring);j++{
            //Fix the successor node
                if ring[j].ip==ring[i].succ{
                    ring[j].prev = ring [i].prev
                }
            //Fix the predecessor node
                if ring[j].ip==ring[i].prev{
                    ring[j].succ=ring[i].prev
                }
            }
            //Remove the node
            ring = append(ring[:i],ring[i+1:]...)
            //TODO-AM i USING append correctly
        }
    }
}

//keeps a count mod 3. Everytime it is 0, we call the Clock().
//When it is 0,1, or 2, we do the node join/crash check
var count int = -1

//Table maintained by Keeper for all the nodes
//TODO-300 is static. Can we maintain this table dynamic while sharing it between keepers
var node_status []bool = make ([]bool,300)

//TODO-only works for one keeper. The range needs to be modified when working with multiple keepers
func (self *keeper) node_status() error{
    for i:= range self.config.Backs {
        node_status[i] = false
    }
    return nil
}

func (self *keeper) run() error {

	// initialize
	for i := range self.config.Backs {
		self.workers = append(self.workers, worker{
			address: self.config.Backs[i],
			lastAck: 0,
			status: ALIVE,
			handler: &client{ addr: self.config.Backs[i] },
		})
	}

	for {
		// Heartbeat period
		time.Sleep(1 * time.Second)
        count = (count+1)%3

    //Perform the following node check operation everytime
        key := "STATUS"
        value:= "TRUE"
        keyvalue := KeyValue{Key:key,Value:value}
        succ:= false

    for i:= range self.config.Backs {
        cur_node_status := false
        err_node_status := self.workers[i].handler.Set(&keyvalue, &succ)

        if err_node_status!=nil{
            if(succ==true){
                //the operation had succeeded, the node is up
                //TODO-is my understanding of succ correct
                cur_node_status=true
            }else{
                //node is down
                cur_node_status=false
            }
        }else{
            cur_node_status=true
        }

        var succ2,succ3 bool
        var next,prev string

        if node_status[i]==false{
            if cur_node_status==true{
            //new node has joined
            //TODO-modify ring
            addNodetoRing(self.config.Backs[i],ring,&next,&prev)

            //TODO-add successor/previous keys on the corresponding nodes
            self.workers[i].handler.Set(&KeyValue{
                Key: "NEXT",
                Value: next},&succ2)
            self.workers[i].handler.Set(&KeyValue{
                Key: "PREV",
                Value: prev}, &succ3)
                //TODO-Check for errors


            //TODO-call replication
            //ReplicaSetJoin(self.config.Backs[i])
            }else{
                //Nothing to do
            }
        }else {
            if cur_node_status==true{
                //Nothing to do
            }else{
                //Node has failed
                //TODO-Modify ring, remove node
                removeNodefromRing(self.config.Backs[i],ring,&next,&prev)

                //TODO-Modify successor/previous keys
                //MODIFY THE CORRECT NODES

                //TODO-call replication
                //ReplicaSetFail(self.config.Backs[i])
            }
        }
    }

    //Perform the following clock sync only every 3rd second
     if count==2{
		// Query workers
		var maxClock uint64
		for i := range self.workers {
			err := self.workers[i].handler.Clock(self.workers[i].lastAck, &self.workers[i].lastAck);
			if err == nil {
				if maxClock < self.workers[i].lastAck {
					maxClock = self.workers[i].lastAck
				}

				// Mark worker as ALIVE
				self.workers[i].status = ALIVE
			} else {
				// Print log message
				log.Printf("Error reading from %s\n", self.workers[i])

				if self.workers[i].status != DEAD {
					// Mark worker as SILENT
					if self.workers[i].status != SILENT {
						self.workers[i].status = SILENT
						self.workers[i].silentDur = 0
					}
					self.workers[i].silentDur++
				}
			}
		}

		// Sync clocks
		for i := range self.workers {
			if self.workers[i].status == ALIVE {
				if self.workers[i].lastAck < maxClock {
					// Sync clock with maxClock
					log.Printf("Shifting clock from %d to %d for %s\n", self.workers[i].lastAck, maxClock, self.workers[i].address)
					err := self.workers[i].handler.Clock(maxClock, &self.workers[i].lastAck)
					if err != nil {
						// Print log message
						log.Printf("Error updating clock for %s\n", self.workers[i])
					}
				}
			} else {
				if (self.workers[i].status == SILENT && self.workers[i].silentDur == 10) {
					self.workers[i].status = DEAD
					log.Printf("Potential failure of %s detected\n", self.workers[i].address)
				}
			}
		}
     }
	}
	return errors.New("Keeper terminated unexpectedly!");
}
