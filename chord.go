
package triblab
//import "trib"
import "fmt"

//comment pushed by vineet to check push


//comment by vineet
type Chord struct {
    ring []node
    //check keeper.go for struct definitions
}

func (self *Chord) Succ_node_ip(ip string) (string,error){
    if len(self.ring)==0{
        return "",fmt.Errorf("Ring with no nodes. Please check")
    }

    if len(self.ring)==1{
        return "",fmt.Errorf("Ring with 1 node, cant find succ. Please check")
    }

    for i:=0;i<len(self.ring);i++{
        if self.ring[i].ip==ip{
            return self.ring[i].succ,nil
        }
        return "",fmt.Errorf("IP not found in ring. Error!")
    }
    return "",fmt.Errorf("Should not have reached here, check succ_node function")
}

func (self *Chord) Prev_node_ip(ip string) (string,error){
    if len(self.ring)==0{
        return "",fmt.Errorf("Ring with no nodes. Please check")
    }

    if len(self.ring)==1{
        return "",fmt.Errorf("Ring with 1 node, cant find succ. Please check")
    }

    for i:=0;i<len(self.ring);i++{
        if self.ring[i].ip==ip{
            return self.ring[i].prev,nil
        }
        return "",fmt.Errorf("IP not found in ring. Error!")
    }
    return "",fmt.Errorf("Should not have reached here, check prev_node function")
}

/**
 * @return Returns a list of IP addresses that store the primary copy and replicas of the provided bin
 */
func (self *Chord) ReplicaSetJoin(ip string) []string {
    //ip is the IP address of the node which joined
	return nil
}

func (self *Chord) ReplicaSetFail(ip string) []string {
    //ip is the IP address of the successor node of the node which failed
    //The failed node does not exist in the ring at all.
	return nil
}

//TODO-additional functions asked my Saman
