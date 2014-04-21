package triblab
import . "trib"
import "sync"
import "log"
import "fmt"
import "strconv"

type TServer struct {
	lock  sync.Mutex
	storage BinStorage
}

var _ *TServer = new(TServer)

// Utilities
func validateUsername(user string) error {
	if len(user) > MaxUsernameLen {
		return fmt.Errorf("username %q too long", user)
	}
	if !IsValidUsername(user) {
		return fmt.Errorf("invalid username %q", user)
	}
	return nil;
}

func inArray(needle string, haystack[] string) bool {
	for i := range haystack {
		if haystack[i] == needle { return true }
	}
	return false
}

//
func (self *TServer) acquireBin(user string) Storage {
	// TODO optimize by maintaining a list of recent connections (persistent)
	return self.storage.Bin(user)
}

func (self *TServer) userList(users *List) error {
	b := self.acquireBin("USERS")
	e := b.ListGet("USERS", users)
	if e != nil { return e }
	return nil
}

func (self *TServer) userExists(user string) bool {
	// TODO optimize by first looking at local cache
	users := new(List)
	e := self.userList(users)
	if e != nil {
		log.Printf("Error while looking for user %s: %s\n", user, e)
		return true;
	}
	return inArray(user, users.L)
}

func (self *TServer) getUsers() []string {
	users := new(List)
	e := self.userList(users)
	if e != nil {
		log.Printf("Error while trying to get the list of users: %s\n", e)
		return nil
	}
	return users.L
}

// Service Interfaces
func (self TServer) SignUp(user string) error {

	err := validateUsername(user)
	if err != nil { return err }

	if self.userExists(user) {
		return fmt.Errorf("user %q already exists!", user)
	}

	var ok bool
	b := self.acquireBin("USERS")
	kv := KeyValue{ Key: "USERS", Value: user }

	self.lock.Lock()
	defer self.lock.Unlock()
	e := b.ListAppend(&kv, &ok)

	if e == nil && !ok {
		return fmt.Errorf("failed creating new user!")
	}

	return e
}

func (self TServer) ListUsers() ([]string, error) {
	// TODO optimize by first looking at local cache
	return self.getUsers(), nil;
}

func (self TServer) Follow(who, whom string) error {

	t, e := self.IsFollowing(who, whom)

	if e != nil { return e }
	if t != false {
		return fmt.Errorf("%s is already following %s", who, whom)
	}

	b := self.acquireBin(who)
	var fq string
	e = b.Get(who + "::" + "FQ", &fq)
	var FQ int
	if e != nil { return e }
	if fq == "" {
		FQ = 0
	} else {
		FQ, e = strconv.Atoi(fq)
		if e != nil { return e }
		if FQ >= MaxFollowing {
			return fmt.Errorf("You have already reached the limit!")
		}
	}

	// TODO make it atomic using redo-logging
	var OK bool

	// TODO Store Clock
	e = b.Set(&KeyValue{ Key: who + "_" + whom, Value: "FOLLOWING" }, &OK)
	if e != nil { return e }
	if OK != true { return fmt.Errorf("Unable to create new follower!") }

	e = b.ListAppend(&KeyValue{ Key: who + "::FOLLOWING", Value: whom }, &OK)
	if e != nil { return e }
	if OK != true { return fmt.Errorf("Unable to update list of followers!") }

	FQ = FQ + 1
	e = b.Set(&KeyValue{ Key: who + "::" + "FQ", Value: strconv.Itoa(FQ + 1)}, &OK)
	if OK != true { return fmt.Errorf("Unable to update number of followers!") }

	return e
}

func (self TServer) IsFollowing(who, whom string) (bool, error) {

	if who == whom {
		return false, fmt.Errorf("You cannot become your follower!")
	}

	if !self.userExists(who) {
		return false, fmt.Errorf("Source user does not exist!")
	}

	if !self.userExists(whom) {
		return false, fmt.Errorf("Target user does not exist!")
	}

	b := self.acquireBin(who)

	// TODO make it atomic using redo-logging
	var temp string
	e := b.Get(who + "_" + whom, &temp)
	if temp != "" {
		return true, e
	} else {
		return false, e
	}
}

func (self TServer) Unfollow(who, whom string) error {
	/*
	if who == whom {
		return fmt.Errorf("cannot unfollow oneself")
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	uwho, e := self.findUser(who)
	if e != nil {
		return e
	}

	uwhom, e := self.findUser(whom)
	if e != nil {
		return e
	}

	if !uwho.isFollowing(whom) {
		return fmt.Errorf("user %q is not following %q", who, whom)
	}

	uwho.unfollow(whom)
	uwhom.removeFollower(who)
	return nil
	*/
	return nil
}

func (self TServer) Following(who string) ([]string, error) {
	/*
	self.lock.Lock()
	defer self.lock.Unlock()

	uwho, e := self.findUser(who)
	if e != nil {
		return nil, e
	}

	ret := uwho.listFollowing()
	return ret, nil
	*/
	return nil, nil
}

func (self TServer) Post(user, post string, c uint64) error {
	/*
	if len(post) > trib.MaxTribLen {
		return fmt.Errorf("trib too long")
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return e
	}

	if self.seq < c {
		self.seq = c
	}
	self.seq++
	if self.seq == math.MaxUint64 {
		panic("run out of seq number")
	}

	t := time.Now()
	u.post(user, post, self.seq, t)

	return nil
	*/
	return nil
}

func (self TServer) Home(user string) ([]*Trib, error) {
	/*
	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return nil, e
	}

	return u.listHome(), nil
	*/
	return nil, nil
}

func (self TServer) Tribs(user string) ([]*Trib, error) {
	/*
	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return nil, e
	}

	return u.listTribs(), nil
	*/
	return nil, nil
}
