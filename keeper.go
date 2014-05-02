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

const MAXHASHVAL = 4294967296 - 1
//TODO-isthis correct MAXHASHVAL?

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
    ip string //its own ip
    prev string //prev ip
    succ string //succ ip
    start uint32 //start of its arc
    end uint32 //end of its arc onr ring
}

//Initially there is no node in the ring
//var ring []node

//create ring with zero nodes
func (self *Chord) initialize() error{
    self.ring = make([]node, 0)
    return nil
}

func (self *Chord) printRing() error{
    for i := range self.ring{
        fmt.Printf("%+v",self.ring[i])
    }
    return nil
}

func (self *Chord) lookupValinRing(val uint32) (string, error){
    if len(self.ring)==0{
        return "",fmt.Errorf("ring is of size 0 nodes. Cannot find a node.")
    }
    if len(self.ring)==1{
        return self.ring[0].ip,nil
    }
    if len(self.ring)>1{
        for j:=0;j<len(self.ring);j++{
            //Normal case, whent he start value is less than the end value
          if self.ring[j].start < self.ring[j].end {
            if (self.ring[j].start < val) && (self.ring[j].end > val){
                return self.ring[j].ip, nil
            }
        }
            //else check for the "0" key jump case i.e. the val is between end and start
            if (self.ring[j].start > self.ring[j].end){
                if((self.ring[j].end > val && val > 0) || (self.ring[j].start < val)){
                    return self.ring[j].ip, nil
                }
            }
        }
    }
    return "", fmt.Errorf("should never reach here. Check function")
}

func (self *Chord) getIPbyBinName(name string) (string,error){
    val := getHash(name)
    return self.lookupValinRing(val)
}

func (self *Chord) listAllActiveNodes() ([]string, error){
    list_of_active_nodes:=make([]string,0)

    for i:=0;i<len(self.ring);i++{
        list_of_active_nodes = append(list_of_active_nodes,self.ring[i].ip)
    }
    return list_of_active_nodes,nil
}

func (self *Chord) addNodetoRing(ip string) (string, string, error){
    var Node node
    val := getHash(ip)

    //var next,prev string

    //Folowing values are fixed regardless of node location in ring
    Node.ip = ip

    if len(self.ring)==0{
        Node.succ = ""
        Node.prev = ""
        Node.start = 0
        Node.end = MAXHASHVAL
    } //else{
        if len(self.ring)==1{
            Node.succ=self.ring[0].ip
            Node.prev=self.ring[0].ip
            Node.start=getHash(self.ring[0].ip)+1
            Node.end=val

            //Fix the other node - which is already existing
            self.ring[0].succ=ip
            self.ring[0].prev=ip
            self.ring[0].start=val+1
            self.ring[0].end=getHash(self.ring[0].ip)
            //So, now both the nodes are fixed. The initial node covers the "0" key
        }//else{
        //TODO-above. I think len=2 case is handled in loop. Confirm.
        //For rings with 2 nodes or more
        if len(self.ring)>1{
    for i:=0;i<len(self.ring);i++{
        //Normal case - when a node's start value is less than end
      if self.ring[i].start < self.ring[i].end{
        if (val > self.ring[i].start && val < self.ring[i].end){
            Node.succ = self.ring[i].ip
            Node.prev = self.ring[i].prev
            Node.start = self.ring[i].start
            Node.end = val
            //Fix the successor node
            self.ring[i].prev = Node.ip
            self.ring[i].start = val+1
            //Fix the predecessor node later - TODO
            break
        }
      }else{
          //Special case - jumping over the zero key
        if self.ring[i].start > self.ring[i].end{
            if (val < self.ring[i].start && val > 0) ||  val > self.ring[i].end{
                Node.succ=self.ring[i].ip
                Node.prev=self.ring[i].prev
                Node.start=self.ring[i].start
                Node.end=val
                //Fixing the successor
                self.ring[i].prev=Node.ip
                self.ring[i].start=val+1
                //Fix the predecessor node later - TODO
                break
        }
    }
            return "","",fmt.Errorf("some error, the node must be inserted somewhere")
    }
    }
    //Fix the predecessor's arc and prev value
    for j:=0;j<len(self.ring);j++{
        if self.ring[j].ip==Node.prev{
            self.ring[j].succ = Node.ip
            break
        }
    }
}
    self.ring = append(self.ring, Node) //TODO-is this how you use append
    return  Node.succ,Node.prev,nil
}

func (self *Chord) removeNodefromRing(ip string) (string, string, error){
    if len(self.ring)==0{
        return "","",fmt.Errorf("ring already empty, cannot remove node")
    }
    if len(self.ring)==1{
        if self.ring[1].ip==ip{
            //the only node in the ring is the node we want to delete
            //create ring of size 0
            self.ring = make ([]node, 0)
            return "","",nil
        }else{
            return "","",fmt.Errorf("the only node in ring does not share ip with the node being removed. Error")
        }
    }
    if len(self.ring)==2{
        var j uint32
        if self.ring[0].ip==ip{
            //j is the index of the node remaining
            j=1
        }else{
        if self.ring[1].ip==ip{
            j=0
        }else{
            return "","",fmt.Errorf("Ring has two nodes, but none of the nodes match the ip being deleted")
        }}

//Modify ring to contain only one node
        self.ring = make([]node,1)
        self.ring[0].succ=""
        self.ring[0].prev=""
        self.ring[0].ip=self.ring[j].ip
        self.ring[0].start=0
        self.ring[0].end= MAXHASHVAL

        //above should leave only one node in the ring
        if len(self.ring)!=1{
            return "","",fmt.Errorf("ring is not of size 1. Error")
        }
        return "","",nil
    }

    for i:=0;i<len(self.ring);i++{
        if ip==self.ring[i].ip{
            //we have found our node, need to modify the relevant succ and prev values in ring
            //and remove the node
            for j:=0;j<len(self.ring);j++{
            //Fix the successor node
                if self.ring[j].ip==self.ring[i].succ{
                    self.ring[j].prev = self.ring[i].prev
                    self.ring[j].start = self.ring[i].start
                }
            //Fix the predecessor node
                if self.ring[j].ip==self.ring[i].prev{
                    self.ring[j].succ=self.ring[i].succ
                }
            }
            //Remove the node
            self.ring = append(self.ring[:i],self.ring[i+1:]...)
            return "","",nil
        }
    }
    return "","",nil
}

/*
//TODO-only works for one keeper. The range needs to be modified when working with multiple keepers
func (self *keeper) node_status() error{
    for i:= range self.config.Backs {
        node_status[i] = false
    }
    return nil
}
*/

func (self *keeper) run() error {
    //keeps a count mod 3. Everytime it is 0, we call the Clock().
    //When it is 0,1, or 2, we do the node join/crash check
    var count int = -1
//Table maintained by Keeper for all the nodes
//TODO-300 is static. Can we maintain this table dynamic while sharing it between keepers
var node_status []bool = make ([]bool,300)

	// initialize
    var chord Chord
    chord.initialize()
log.Print(chord.ring)
    replication := &ReplicationService{ _chord: &chord }

log.Print(self.config.Backs)
	for i := range self.config.Backs {
		self.workers = append(self.workers, worker{
			address: self.config.Backs[i],
			lastAck: 0,
			status: ALIVE,
			handler: &client{ addr: self.config.Backs[i] },
		})
	}

	go replication.run()
log.Print(1)
	for {
		// Heartbeat period
		time.Sleep(1 * time.Second)
        count = (count+1)%3

    //Perform the following node check operation everytime
        key := "STATUS"
        value:= "TRUE"
        keyvalue := KeyValue{Key:key,Value:value}
        succ:= false

log.Print(2)
    for i:= range self.config.Backs {
        cur_node_status := false
        err_node_status := self.workers[i].handler.Set(&keyvalue, &succ)
        //If you are able to do above "Set" operation, then the node is up
log.Print(3)
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

log.Print(4)
        var succ2,succ3 bool
        var next,prev string

        if node_status[i]==false{
            if cur_node_status==true{
            //new node has joined
            //Call replication service
            //modify ring - add node
			var err1 error
log.Print(5)
log.Print("len of chord.ring=", len(chord.ring))
log.Print(chord.ring)
			next,prev,err1 = chord.addNodetoRing(self.config.Backs[i])
log.Print(6)
log.Print(chord.ring)
			if err1!=nil{
				fmt.Errorf("error in adding node")
			}
            chordminisnapshot,err1:=CreateMiniChord(self.config.Backs[i],&chord)
            if err1!=nil{
                fmt.Errorf("chordminisnapshot network error. check.")
            }
log.Print(7)
			go replication.notifyJoin(&chordminisnapshot)
log.Print(8)
            //add successor/previous keys on the corresponding nodes
            err2:=self.workers[i].handler.Set(&KeyValue{
                Key: "NEXT",
                Value: next},&succ2)
                if err2!=nil || succ2 !=true{
                    fmt.Errorf("Error with Set NEXT")
                }
log.Print(9)
            err3:=self.workers[i].handler.Set(&KeyValue{
                Key: "PREV",
                Value: prev}, &succ3)
                if err3!=nil || succ3!=true{
                    fmt.Errorf("Error with Set PREV")
                }
log.Print(10)
                //TODO-also modify these values on the other nodes
                var succ4,succ5 bool
                for j:=0;j<len(self.workers);j++{
                    if self.config.Backs[j]==prev{
                        err4:=self.workers[j].handler.Set(&KeyValue{Key:"NEXT",Value:self.config.Backs[i]},&succ4)
                        if err4!=nil || succ4!=true{
                            return fmt.Errorf("Error: with set NEXT in prev")
                        }
                    }
log.Print(11)
                    if self.config.Backs[j]==next{
                        err5:=self.workers[j].handler.Set(&KeyValue{Key:"PREV",Value:self.config.Backs[i]},&succ5)
                        if err5!=nil || succ5!=true{
                            return fmt.Errorf("Error: with set PREV in next")
                        }
                    }
log.Print(12)
                }
           node_status[i]=true
            }else{
                //Nothing to do
log.Print(13)
            }
        }else {
            if cur_node_status==true{
                //Nothing to do
log.Print(14)
            }else{
                //Node has failed
                //Call replication service
log.Print(15)
            chordminisnapshot1,errr:=CreateMiniChord(self.config.Backs[i],&chord)
            if errr!=nil{
                fmt.Errorf("chordminisnapshot network error. check.")
            }
			go replication.notifyLeave(&chordminisnapshot1)
log.Print(16)
            //Remove node - modify ring
                var err2 error
                next,prev,err2 = chord.removeNodefromRing(self.config.Backs[i])
log.Print(17)
                if err2!=nil{
                    fmt.Errorf("Error removing node.")
                }
                //TODO-Modify successor/previous keys
                //TODO for the other nodes
                var succ4,succ5 bool
                for j:=0;j<len(self.workers);j++{
                    if self.config.Backs[j]==prev{
                        err4:=self.workers[j].handler.Set(&KeyValue{Key:"NEXT",Value:next},&succ4)
                        if err4!=nil || succ4!=true{
                            return fmt.Errorf("Error: with set NEXT in prev")
                        }
                    }
                    if self.config.Backs[j]==next{
                        err5:=self.workers[j].handler.Set(&KeyValue{Key:"PREV",Value:prev},&succ5)
                        if err5!=nil || succ5!=true{
                            return fmt.Errorf("Error: with set PREV in next")
                        }
                    }
                }
            node_status[i]=false
        }
    }
    //Perform the following clock sync only every 3rd second
     if count==2{
log.Print(20)
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
}
	return errors.New("Keeper terminated unexpectedly!");
}

