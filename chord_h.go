package triblab
//import "trib"
import "hash/crc32"
//import "log"
import "fmt"
//Maintain node1 and Ring for the Chord1 Ring information
type node1 struct {
    succ string //succ ip
    prev string //prev ip
    ip string //its own ip
    hash uint32
    start uint32 //start of its arc
    end uint32 //end of its arc on self.ring
}


type Chord1 struct {
    back_ends[] string
    ring []node1
}


func getHash1(name string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(name))
	return h.Sum32()
}

func (self *Chord1) locate_node(id uint32) uint32{
    var succ uint32
    var id_max int
    var id_min int
    var max uint32
    var min uint32
//    var r uint32
    found:=false
    max=self.ring[0].hash//
    min=self.ring[0].hash
  //  r=0
    if (len(self.ring)==1){
            if(self.ring[0].hash>id){
                succ=self.ring[0].hash
                self.ring[0].end=id
            }else{
                succ= self.ring[0].end
                self.ring[0].end=id
            }
    }
    if (len(self.ring)>=2){

    for i:=0;i<len(self.ring);i++{

            if (self.ring[i].hash > max){
                id_max=i
                max=self.ring[i].hash
            }
            if (self.ring[i].hash<min){
                id_min=i
                min=self.ring[i].hash
            }


            if (id > self.ring[i].hash && id < self.ring[i].end){
                succ=self.ring[i].end
                self.ring[i].end=id
                found=true
                break
            }
          
        }
        if (id >max || id <min) && found==false{
                succ=self.ring[id_min].hash
                self.ring[id_max].end=id
        }

        }           
return succ
}

func (self *Chord1) find_succ(id uint32) uint32{
    var succ uint32
 //   var id_max int
 //   id_max:=1
    var id_min int
    var max uint32
    var min uint32
 //   var r uint32
    found:=false
    max=self.ring[0].hash
    min=self.ring[0].hash

    if len(self.ring)==1{
        return self.ring[0].hash
    }
    if len(self.ring)>1{
        for i:=0;i<len(self.ring);i++{

             if (self.ring[i].hash > max){
               // id_max=i
                max=self.ring[i].hash
            }

            if (self.ring[i].hash<min){
                id_min=i
                min=self.ring[i].hash
            }

            if id > self.ring[i].hash && id < self.ring[i].end{
                succ= self.ring[i].end
                found=true
                break
            }
        }
           if (id >max || id <min) && found==false{
                succ=self.ring[id_min].hash
        }
    }
return succ
}


func (self *Chord1) addNode(ip string){
    var Node node1
    Node.ip = ip
    val := getHash1(ip)
    Node.hash=val
    if (len(self.ring)==0){
    Node.end=val
    }else{
    Node.end=self.locate_node(val)
    }
    self.ring = append(self.ring,Node)
    for i:=0;i<len(self.ring);i++{
    fmt.Printf("Node value:%d,Node Succ:%d\n",self.ring[i].hash,self.ring[i].end)
    }
}

/***
func (self *Chord1) removeNode(ip string) (error){
    var ip_used string
    val := getHash1(ip)
    deleted := false
    var index int
    index=0
    if (len(self.ring)==0){
        fmt.Printf("No nodes to delete\n")
    }else{     
        if (len(self.ring)==1){
            if self.ring[0].hash=val{
            self.ring = make ([]node1, 0)
            }
        }
        deleted=true 
    }else{
        if (len(self.ring)==2){
            if self.ring[0].hash==val{
            ip_used=self.ring[1].ip
            }else{
            if self.ring[1].ip==ip{
            ip_used=self.ring[0].ip
            }
            }

            self.ring = make([]node,1)
            self.ring[0].ip=ip_used
            new_val := getHash1(ip_used)
            self.ring[0].end= new_val
            deleted=true
            }
    }else{
        for i:=0;i<len(self.ring);i++{
            if val==self.ring[i].hash{
                /**
                 for j:=0;j<len(self.ring);j++{
            //Fix the successor node
                if self.ring[j].hash==self.ring[i].end{
//log.Print("remove23")
                //    self.ring[j].prev = self.ring[i].prev
                    self.ring[j].start = self.ring[i].start
                }
            //Fix the predecessor node
//log.Print("remove24")
                if self.ring[j].ip==self.ring[i].prev{
//log.Print("remove25")
                    self.ring[j].succ=self.ring[i].succ
                }
            }
                self.ring = append(self.ring[:i],self.ring[i+1:]...)
                deleted=true
                index=i-1
            }
        }
    }

    if(deleted==false)
        return fmt.Errorf("Error while deleting node in Chord")
    else
        return nil
    
}***/






