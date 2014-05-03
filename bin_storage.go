package triblab
import . "trib"
import "hash/crc32"
import "time"
import "fmt"
import "log"

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

func (self *BinStorageWrapper) Bin(name string) Storage {
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


func (self *BinStorageWrapper) bootStrapRing(){
	var cli *client
    log.Print("Entered the ring")
    for i:=0;i<len(self.back_ends);i++{
    	cli=&client{ addr: self.back_ends[i] }
    	_, err := cli.acquireConnection();
    	if err==nil{
    		self.chord.addNode(self.back_ends[i])
    	}
    }
    log.Print("Exit the ring")
    log.Print("Ring Size")
    log.Print(len(self.chord.ring))
}


func (self *BinStorageWrapper) fixPreviousPointer(){
	var prev string
	var prev_val uint32
    var cli *client
    log.Print("Entered the fixPrevious")
	for i:= range self.chord.ring{
		cli=&client{ addr: self.chord.ring[i].ip }
    	_, err := cli.acquireConnection();
    	if err==nil{
    		err1:=cli.Get("PREV",&prev)
            if (err1!=nil){
                fmt.Errorf("Error with Get PREV")
                log.Print("Error while Get PREV")
            }else{
            log.Print("PREV-",prev)
    		prev_val=self.chord.getHash1(prev)
    		self.chord.ring[i].prev_ip=prev
    		self.chord.ring[i].prev=prev_val
        }
    	}
     log.Print("Exit fixPrevious")   
	}
}


func (self *BinStorageWrapper) updateRing(){
	var cli *client
	var next string
	var prev string
	var next_val uint32
	var prev_val uint32
//	var name string
    var incr uint32
	var ip string
    incr =0
    log.Print("Entering updateRing")
	for {
		// Run every 15 seconds
        log.Print("Running updateRing for:")
        log.Print(incr)
        incr=incr+1
		time.Sleep(15 * time.Second)
        log.Print(len(self.chord.ring))
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
                    log.Print("Error while getting Next")
            	}
            	if err2!=nil{
                	fmt.Errorf("Error with Get PREV")
                    log.Print("Error while getting PREV")
            	}

            	next_val=self.chord.getHash1(next)
            	prev_val=self.chord.getHash1(prev)

                log.Print("Got next and pre values")
                log.Print(self.chord.ring[i].next)
                log.Print(next_val)
            	if (self.chord.ring[i].next!=next_val || self.chord.ring[i].prev!=prev_val){
            		// New node was added or some node was deleted
            		found:= false
            		for j:= range self.chord.ring{
            			if (self.chord.ring[j].ip==next){
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
					self.chord.removeNode(ip)
			}
		}
}
                self.printRing()
                log.Print("Leaving updateRing")
}



func (self *BinStorageWrapper) printRing(){
    log.Print("Print the ring")
    log.Print("Size of the ring PrintLog")
    log.Print(len(self.chord.ring))
    for i:= range self.chord.ring {
        fmt.Printf("%d--%d--%d--%s--%s\n",self.chord.ring[i].hash,self.chord.ring[i].prev,self.chord.ring[i].next,self.chord.ring[i].ip,self.chord.ring[i].succ_ip)
    }
    log.Print("Getting out of the Printring")

}


func (self *BinStorageWrapper) testFindSucc(){
ls:=[]uint32{562156532,653006829,674734111,825012062,947739488,949674805,1058053961,1073162028,1177263824,1209511639,1223694018,1333422798}


for i:= range ls{
    succ,succ_ip:=self.chord.find_succ(ls[i])
    log.Print("SUCC--",succ,"--",succ_ip)
}

}