package triblab
//import "trib"


//comment by vineet
type Chord struct {
    ring []node
    //check keeper.go for struct definitions
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
