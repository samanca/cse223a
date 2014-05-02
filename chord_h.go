package triblab
//import "trib"
import "hash/crc32"
import "log"
import "fmt"
//Maintain node1 and Ring for the Chord1 Ring information
type node1 struct {
    succ string //succ ip
    prev string //prev ip
    ip string //its own ip
    hash uint32
    start uint32 //start of its arc
    end uint32 //end of its arc on ring
}

//Initially there is no node1 in the ring
var ring []node1

func getHash1(name string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(name))
	return h.Sum32()
}

func (self *Chord1)locate_node(id uint32) uint32{
    var succ uint32
    var id_max int
    var id_min int
    var max uint32
    var min uint32
    var r uint32
    var found bool
    found=false
    max=ring[0].hash
    min=ring[0].hash
    r=0
    fmt.Printf("Ring size is %d\n",len(ring))

    if (len(ring)==1){
            if(ring[0].hash>id){
                succ=ring[0].hash
                ring[0].end=id
            }else{
                succ= ring[0].end
                ring[0].end=id
            }
    }
    if (len(ring)>=2){
    for i:=0;i<len(ring);i++{
        if(max < ring[i].hash){
            max=ring[i].hash
            id_max=i
        }

        if(min > ring[i].hash){
            min=ring[i].hash
            id_min=i
        }

            if (id > ring[i].hash && id < ring[i].end && found==false){
                succ=ring[i].end
                ring[i].end=id
                found=true
            }
          
        }
          if (id >max || id <min){
                succ=ring[id_min].hash
                ring[id_max].end=id
            }
        }
    

  /*          
        if (id > ring[i].hash && id < ring[i].end && found==false) || (ring[i].hash==ring[i].end && found==false){
            succ= ring[i].end
            ring[i].end=id
            found=true
            r=1
        }
    }  
        if (found=false && len(ring)>2) {
            ring[id_max].end=id
            succ=ring[id_min].hash
            //succ=max
            r=2
        }

        if (id < ring[id_min].hash && len(ring)>2) {
             ring[id_max].end=id
             succ=ring[id_min].hash
             r=3
        }
    */     
        fmt.Printf("Max :%d,Min :%d,R:%d\n",max,min,r)
              
return succ
}

func (self *Chord1)find_succ(id uint32) uint32{
    var succ uint32
    for i:=0;i<len(ring);i++{
        if id > ring[i].hash && id < ring[i].end{
            succ= ring[i].end
            break
        }else{
        fmt.Printf("Didnt find the id on the circle")
        }
    }
return succ
}

func (self *Chord1) createRing() node1{
    var Node node1
    Node.ip = ""
    Node.hash=0
    Node.end=4294967296 -1
    return Node
}
func (self *Chord1) addNode(ip string){
    var Node node1
    Node.ip = ip
    val := getHash1(ip)
    Node.hash=val
    if (len(ring)==0){
    //self.createRing()
    //ring = append(ring,self.createRing())
    Node.end=val
    }else{
    Node.end=self.locate_node(val)
    }
    ring = append(ring,Node)
    for i:=0;i<len(ring);i++{
    fmt.Printf("Node value:%d,Node Succ:%d\n",ring[i].hash,ring[i].end)
    }
}
//TODO-r}eturn ??
//TODO- is the hash value returned uitn32?
func (self *Chord1) addNodetoRing(ip string){
    var Node node1
    Node.ip = ip
    val := getHash1(ip)
    if (len(ring)==0){
        //TODO-Is this empty string?
        Node.succ = ""
        Node.prev = ""
        Node.start = 0
        Node.end = 4294967296 -1 //TODO-better way to write this
    }
    for i:=0;i<len(ring);i++{
        if val > ring[i].start && val < ring[i].end{
            Node.succ = ring[i].succ
            Node.prev = ring[i].ip
            Node.start = ring[i].end
            Node.end = val
            ring[i].succ = Node.ip
            break
        }else {
            fmt.Errorf("some error, the node must be inserted somewhere")
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
    ring = append(ring,Node) //TODO-is this how you use append
}

//keeps a count mod 3. Everytime it is 0, we call the Clock().
//When it is 0,1, or 2, we do the node join/crash check
var count1 int = -1

//comment by vineet
type Chord1 struct {
	back_ends[] string
}
func (self *Chord1) makeRing(){
for i := range self.back_ends{
	self.addNode(self.back_ends[i])
}
}

func (self *Chord1) printRing(){
	for i := range ring {
		//log.Printf("%s--%s--%s--%d--%d",ring[i].ip,ring[i].succ,ring[i].prev,ring[i].start,ring[i].end)
        log.Printf("%s--%d--%d",ring[i].ip,ring[i].hash,ring[i].end)
}
}
