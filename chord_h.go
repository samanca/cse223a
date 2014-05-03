package triblab
//import "trib"
import "hash/crc32"
import "log"
import "fmt"
//Maintain node1 and Ring for the Chord1 Ring information
type node1 struct {
    succ_ip string //succ ip
    prev_ip string //prev ip
    ip string //its own ip
    hash uint32
    prev uint32 //prev of its arc
    next uint32 //next of its arc on self.ring
}


type Chord1 struct {
    ring []node1
}


func (self *Chord1)getHash1(name string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(name))
	return h.Sum32()
}

func (self *Chord1) locate_node(id uint32,ip string) (ret_succ uint32,ret_succ_ip string){
    var succ uint32
    var succ_ip string
    var id_max int
    var id_min int
    var max uint32
    var min uint32
    found:=false
    max=self.ring[0].hash
    min=self.ring[0].hash
    log.Print("Enter locate node")
   // log.Print(self.ring)
    if (len(self.ring)==1){
            if(self.ring[0].hash>id){
                succ=self.ring[0].hash
                succ_ip=self.ring[0].ip
                self.ring[0].succ_ip=ip
                self.ring[0].next=id

            }else{
                succ= self.ring[0].next
                succ_ip=self.ring[0].succ_ip
                self.ring[0].succ_ip=ip
                self.ring[0].next=id
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


            if (id > self.ring[i].hash && id < self.ring[i].next){
                succ=self.ring[i].next
                succ_ip=self.ring[i].succ_ip
                self.ring[i].next=id
                self.ring[i].succ_ip=ip
                found=true
                break
            }
          
        }
        if (id >max || id <min) && found==false{
                succ=self.ring[id_min].hash
                succ_ip=self.ring[id_min].ip
                self.ring[id_max].next=id
                self.ring[id_max].succ_ip=ip
        }

        }     
        log.Print("Exit locate node")      
return succ,succ_ip
}

func (self *Chord1) find_succ(id uint32) (ret_succ uint32,ret_succ_ip string){
    var succ uint32
    var succ_ip string 
    var id_min int
    var max uint32
    var min uint32
    found:=false
    //max=self.ring[0].hash
    //min=self.ring[0].hash
    max=0
    min=0
    if len(self.ring)==1{
        return self.ring[0].hash,self.ring[0].ip
    }
    if len(self.ring)>1{
        for i:=0;i<len(self.ring);i++{

             if (self.ring[i].hash > max){
                max=self.ring[i].hash
            }

            if (self.ring[i].hash<min){
                id_min=i
                min=self.ring[i].hash
            }

            if id > self.ring[i].hash && id < self.ring[i].next{
                succ= self.ring[i].next
                succ_ip=self.ring[i].succ_ip
                found=true
                break
            }
        }
           if (id >max || id <min) && found==false{
                succ=self.ring[id_min].hash
                succ_ip=self.ring[id_min].ip
                found=true
        }
    }
return succ,succ_ip
}


func (self *Chord1) addNode(ip string){
    var Node node1
    Node.ip = ip
    Node.succ_ip=""
    Node.prev_ip=""
    Node.prev=0
    val := self.getHash1(ip)
    Node.hash=val
    if (len(self.ring)==0){
    Node.next=val
    Node.succ_ip=ip
    }else{
    Node.next,Node.succ_ip=self.locate_node(val,ip)
    }
    self.ring = append(self.ring,Node)
   
}


func (self *Chord1) removeNode(ip string) (error){
    var ip_used string
    val := self.getHash1(ip)
    deleted:=false

    if (len(self.ring)==0){
        fmt.Printf("No nodes to delete\n")
    }else if (len(self.ring)==1){
            if self.ring[0].hash==val{
            self.ring = make ([]node1, 0)
            }
            deleted=true
    }else if (len(self.ring)==2){
            if self.ring[0].hash==val{
            ip_used=self.ring[1].ip
            }else{
            if self.ring[1].ip==ip{
            ip_used=self.ring[0].ip
            }
            }

            self.ring = make([]node1,1)
            self.ring[0].ip=ip_used
            new_val := self.getHash1(ip_used)
            self.ring[0].next= new_val
            deleted=true
    }else{
        for i:=0;i<len(self.ring);i++{
            if val==self.ring[i].hash{
    
                for j:=0;j<len(self.ring);j++{
                    if self.ring[j].hash==self.ring[i].next{
                        self.ring[j].prev = self.ring[i].prev
                    }
                if self.ring[j].hash==self.ring[i].prev{
                    self.ring[j].next=self.ring[i].next
                    }
            }
                self.ring = append(self.ring[:i],self.ring[i+1:]...)
                deleted=true
            }
        }
    }

    if(deleted==false){
        return fmt.Errorf("Error while deleting node in Chord")
    }else{
        return nil
    }    
}

//func (self *Chord1) succ() {

//}


