package triblab
import . "trib"
import "hash/crc32"
import "time"
import "fmt"

type BinStorageWrapper struct {
	back_ends[] string
	chord  Chord1
}

func getHash(name string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(name))
	return h.Sum32()
}

func (self *BinStorageWrapper) Map(name string) uint32 {
	h := getHash(name)
	c := uint32(len(self.back_ends))
	return h % c
}

func (self BinStorageWrapper) Bin(name string) Storage {
	var cli *client
	var ip string
	id:=self.chord.getHash1(name)
	_,succ_ip:=self.chord.find_succ(id)	
	cli=&client{ addr: succ_ip }
    _, err := cli.acquireConnection();
    if(err==nil){
    	ip=succ_ip
    }else{
    	for{
            succ_ip_val:=self.chord.getHash1(succ_ip)
    		_,succ_ip:=self.chord.find_succ(succ_ip_val)	
			cli=&client{ addr: succ_ip }
    		_, err := cli.acquireConnection();

    		if (err==nil){
    			ip=succ_ip
    			break
    		}
    	}
    }
    
	final_cli := &OpLogClient{ addr: ip, ns: name }
	final_cli.init()
	return final_cli
}


func (self BinStorageWrapper) bootStrapRing(){
	var cli *client
    for i:=0;i<len(self.back_ends);i++{
    	cli=&client{ addr: self.back_ends[i] }
    	_, err := cli.acquireConnection();
    	if err==nil{
    		self.chord.addNode(self.back_ends[i])
    	}
    }
}


func (self BinStorageWrapper) fixPreviousPointer(){
	var prev string
	var prev_val uint32
    var cli *client
	for i:= range self.chord.ring{
		cli=&client{ addr: self.chord.ring[i].ip }
    	_, err := cli.acquireConnection();
    	if err==nil{
    		err1:=cli.Get("PREV",&prev)
            if (err1!=nil){
                fmt.Errorf("Error with Get PREV")
            }else{
    		prev_val=self.chord.getHash1(prev)
    		self.chord.ring[i].prev_ip=prev
    		self.chord.ring[i].prev=prev_val
        }
    	}
	}
}


func (self BinStorageWrapper) updateRing(){
	var cli *client
	var next string
	var prev string
	var next_val uint32
	var prev_val uint32
//	var name string
	var ip string
	for {
		// Run every 15 seconds
		time.Sleep(15 * time.Second)
		for i:= range self.chord.ring{
			ip = self.chord.ring[i].ip

			cli=&client{ addr: ip }

			_,err:=cli.acquireConnection();
			if (err==nil){ // Node is alive
			// Read PREV and NEXT from the live node
				err1:=cli.Get("NEXT",&next)
            	err2:=cli.Get("PREV",&prev)

            	if err1!=nil{
                	fmt.Errorf("Error with Get NEXT")
            	}
            	if err2!=nil{
                	fmt.Errorf("Error with Get PREV")
            	}

            	next_val=self.chord.getHash1(next)
            	prev_val=self.chord.getHash1(prev)
            	if (self.chord.ring[i].next!=next_val || self.chord.ring[i].prev!=prev_val){
            		// New node was added or some node was deleted
            		found:= false
            		for j:= range self.chord.ring{
            			if (self.chord.ring[j].succ_ip==next){
            				found =true
            				break
            			}
            		}
            		if (found==true){
            			self.chord.removeNode(next)// This should remove the node as well as the fix succ and prev
            		}else{
            			self.chord.addNode(next)
            		}
            	}

			}else{ // if the connection was not successful then remove that node 
					self.chord.removeNode(next)
			}
		}
}

}
