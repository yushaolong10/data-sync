package ssh

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// sshController  --> sshPool  --> sshConnection

const (
	poolCount               = 3
	eachPoolConnectionCount = 3
)

//to keep stability of the ssh netWork
//this package for ssh pool
type sshController struct {
	ctx              context.Context
	sshTool          *SSHTool
	remoteHost       string
	totalPoolCount   int64
	eachPoolConnNums int64
	currentPoolId    int64
	poolMutex        *sync.Mutex
	poolMap          map[int64]*sshPool
	exitPoolChan     chan int64    //exit poolId
	sshConnChan      chan *sshConn //ssh all conn
}

func NewSshController(ctx context.Context, sshTool *SSHTool, remoteHost string) *sshController {
	ctl := &sshController{
		ctx:              ctx,
		sshTool:          sshTool,
		remoteHost:       remoteHost,
		totalPoolCount:   poolCount,
		eachPoolConnNums: eachPoolConnectionCount,
	}
	//init controller
	ctl.init()
	return ctl
}

func (ctl *sshController) init() error {
	ctl.poolMutex = new(sync.Mutex)
	ctl.poolMap = make(map[int64]*sshPool)
	//exitPoolChan must has buffer, or it will deadlock when exit
	ctl.exitPoolChan = make(chan int64, ctl.totalPoolCount)
	ctl.sshConnChan = make(chan *sshConn, ctl.totalPoolCount*ctl.eachPoolConnNums)
	return nil
}

func (ctl *sshController) Run() error {
	//checkConnect
	if err := ctl.checkConnect(); err != nil {
		return err
	}
	//create poolMap
	if err := ctl.createPools(); err != nil {
		ctl.close()
		return err
	}
	//loop
	go ctl.loop()
	return nil
}

func (ctl *sshController) GetNetConn() (netConn net.Conn, err error) {
	ticker := time.NewTicker(time.Second * 3)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case conn := <-ctl.sshConnChan:
			if conn.isActive() {
				//pool make new conn signal
				ctl.signalPoolNewConn(conn.poolId)
				//return
				return conn.netConn, nil
			} else {
				//reset pool conn
				err := ctl.resetPoolConn(conn)
				if err != nil {
					log.Printf("[error][sshController.GetNetConn] resetPoolConn err. poolId:%d,connId:%d,err:%s", conn.poolId, conn.connId, err.Error())
				}
			}
			//just continue get netConn
		case <-ticker.C:
			return nil, fmt.Errorf("get net conn timeout")
		case <-ctl.ctx.Done():
			return nil, fmt.Errorf("context canceled")
		}
	}
	return
}

func (ctl *sshController) signalPoolNewConn(poolId int64) {
	ctl.poolMutex.Lock()
	defer ctl.poolMutex.Unlock()
	if pool, ok := ctl.poolMap[poolId]; ok {
		<-pool.newConnChan
	} else {
		log.Printf("[error][sshController.signalPoolNewConn] pool(poolId:%d) not eixst", poolId)
	}
}

func (ctl *sshController) checkConnect() error {
	//open ssh tunnel
	sshClient, err := ctl.sshTool.Connect()
	if err != nil {
		return fmt.Errorf("[sshController.checkConnect] sshTool.Connect err:%s", err.Error())
	}
	//check ssh remote host
	testRemoteConn, err := sshClient.Dial("tcp", ctl.remoteHost)
	if err != nil {
		return fmt.Errorf("[sshController.checkConnect]  ssh dial remoteHost(%s) err:%s", ctl.remoteHost, err.Error())
	}
	testRemoteConn.Close()
	sshClient.Close()
	return nil
}

func (ctl *sshController) createPools() error {
	ctl.poolMutex.Lock()
	defer ctl.poolMutex.Unlock()
	var poolNum, failNum int64
	maxFailNum := ctl.totalPoolCount * 4
	for {
		//if exceed total count
		if poolNum >= ctl.totalPoolCount {
			break
		}
		pool, err := ctl.newSshPool()
		if err != nil {
			log.Printf("[error][sshController.createPools] newSshPool error. err:%s", err.Error())
			//increment failed num
			failNum++
			if failNum > maxFailNum {
				log.Printf("[error][sshController.createPools] newSshPool exceed maxFailNums(%d)", failNum)
				return fmt.Errorf("create poolMap exceed max fail num:%d", failNum)
			}
			continue
		}
		ctl.poolMap[pool.poolId] = pool
		poolNum++
	}
	return nil
}

func (ctl *sshController) loop() {
	//pool dispatcher
	ctl.poolMutex.Lock()
	for poolId, pool := range ctl.poolMap {
		go pool.Dispatcher()
		log.Printf("[info][sshController.loop] pool(%d) Dispatcher", poolId)
	}
	ctl.poolMutex.Unlock()
	//loop for
	for {
		select {
		//wait ctx cancel
		case <-ctl.ctx.Done():
			ctl.close()
			log.Printf("[info][sshController.loop] ctx done. controller close")
			goto end
		case poolId := <-ctl.exitPoolChan:
			//reload poolId
			ctl.reloadPool(poolId)
		}
	}
end:
}

func (ctl *sshController) reloadPool(oldPoolId int64) {
	//ctx done
	select {
	case <-ctl.ctx.Done():
		log.Printf("[info][sshController.reloadPool] context canceled. oldPoolId:%d", oldPoolId)
		return
	default:
	}
	ctl.poolMutex.Lock()
	defer ctl.poolMutex.Unlock()
	//close
	if oldPool, ok := ctl.poolMap[oldPoolId]; ok {
		if err := oldPool.close(); err != nil {
			log.Printf("[error][sshController.reloadPool] pool close error.poolId:%d,err:%s", oldPool.poolId, err.Error())
		}
	}
	//add new
	var failNum int64
	for {
		newPool, err := ctl.newSshPool()
		if err != nil {
			failNum++
			if failNum > 5 {
				log.Printf("[error][sshController.reloadPool] reload exceed max failNum(%d).oldPoolId:%d", failNum, oldPoolId)
				break
			}
			continue
		}
		go newPool.Dispatcher()
		//add new pool
		ctl.poolMap[newPool.poolId] = newPool
		log.Printf("[info][sshController.reloadPool] reload success.oldPoolId:%d,newPoolId:%d", oldPoolId, newPool.poolId)
		break
	}
	delete(ctl.poolMap, oldPoolId)
}

func (ctl *sshController) newSshPool() (*sshPool, error) {
	sshClient, err := ctl.sshTool.Connect()
	if err != nil {
		log.Printf("[error][sshController.newSshPool] ssh tool connect err:%s", err.Error())
		return nil, fmt.Errorf("sshTool Connect err:%s", err.Error())
	}
	pool := &sshPool{
		ctx:         ctl.ctx,
		sshClient:   sshClient,
		remoteHost:  ctl.remoteHost,
		maxConnNum:  ctl.eachPoolConnNums,
		poolId:      atomic.AddInt64(&ctl.currentPoolId, 1),
		exit:        ctl.exitPoolChan,
		sshConnChan: ctl.sshConnChan,
	}
	//init
	pool.init()
	return pool, nil
}

func (ctl *sshController) close() error {
	ctl.poolMutex.Lock()
	for poolId, pool := range ctl.poolMap {
		pool.close()
		log.Printf("[info][sshController.close] pool(%d) close", poolId)
	}
	ctl.poolMutex.Unlock()
	//close ssh conn chan
	for {
		select {
		case <-ctl.sshConnChan:
		default:
			goto end1sshConn
		}
	}
end1sshConn:
	close(ctl.sshConnChan)
	//close exit pool chan
	for {
		select {
		case <-ctl.exitPoolChan:
		default:
			goto end2ExitPool
		}
	}
end2ExitPool:
	close(ctl.exitPoolChan)
	log.Printf("[info][sshController.close] close success")
	return nil
}

func (ctl *sshController) resetPoolConn(conn *sshConn) error {
	ctl.poolMutex.Lock()
	defer ctl.poolMutex.Unlock()
	if pool, ok := ctl.poolMap[conn.poolId]; ok {
		pool.PutConn(conn)
	} else {
		return fmt.Errorf("conn not found pool(%d)", conn.poolId)
	}
	return nil
}

type sshPool struct {
	ctx           context.Context
	remoteHost    string
	poolId        int64
	maxConnNum    int64
	sshClient     *ssh.Client
	currentConnId int64
	//from sshController
	exit        chan int64
	sshConnChan chan *sshConn
	//self chan for async
	closeChan        chan struct{}
	ackClose         chan struct{}
	newConnChan      chan struct{}
	returnedConnChan chan *sshConn
}

func (pool *sshPool) init() {
	pool.closeChan = make(chan struct{})
	pool.ackClose = make(chan struct{})
	pool.newConnChan = make(chan struct{}, pool.maxConnNum)
	pool.returnedConnChan = make(chan *sshConn, pool.maxConnNum)
}

func (pool *sshPool) PutConn(conn *sshConn) {
	select {
	case <-pool.ctx.Done():
		log.Printf("[info][sshPool.PutConn] context canceled.")
		return
	case <-pool.closeChan:
		log.Printf("[info][sshPool.PutConn] pool closed.")
		return
	default:
	}
	pool.returnedConnChan <- conn
}

func (pool *sshPool) Dispatcher() {
	//monitor
	go pool.monitor()
	//watch
	go pool.watchConn()
	//loop
	for {
		//get signal
		select {
		case <-pool.closeChan:
			log.Printf("[info][sshPool.Dispatcher][poolId:%d] pool closed", pool.poolId)
			goto end
		case <-pool.ctx.Done(): //context canceled
			log.Printf("[info][sshPool.Dispatcher][poolId:%d] context canceled", pool.poolId)
			goto end
		case pool.newConnChan <- struct{}{}:
		}
		//new ssh
		conn, err := pool.newSshConn()
		if err != nil {
			log.Printf("[error][sshPool.Dispatcher][poolId:%d] newSshConn err:%s", pool.poolId, err.Error())
			//remove
			<-pool.newConnChan
			continue
		}
		if err := conn.ping(); err != nil {
			log.Printf("[error][sshPool.Dispatcher][poolId:%d] sshConn ping err:%s", pool.poolId, err.Error())
			//close
			conn.close()
			//remove
			<-pool.newConnChan
			continue
		}
		//put new conn
		pool.sshConnChan <- conn
	}
end:
	//ack close
	pool.ackClose <- struct{}{}
}

func (pool *sshPool) monitor() {
	for {
		err := pool.ping()
		if err != nil {
			log.Printf("[error][sshPool.monitor][poolId:%d] ping err:%s", pool.poolId, err.Error())
			//pool exit
			pool.exit <- pool.poolId
			break
		}
		//context is canceled
		if pool.isCanceled() {
			log.Printf("[info][sshPool.monitor][poolId:%d] context canceled", pool.poolId)
			break
		}
		//sleep 3s
		sleepAwhile(3)
	}
	log.Printf("[info][sshPool.monitor][poolId:%d] monitor pool exit", pool.poolId)
	//ack close
	pool.ackClose <- struct{}{}
}

func (pool *sshPool) watchConn() {
	for {
		select {
		case <-pool.ctx.Done():
			log.Printf("[info][sshPool.watchConn][poolId:%d] context canceled", pool.poolId)
			goto end
		case <-pool.closeChan:
			log.Printf("[info][sshPool.watchConn][poolId:%d] pool closed", pool.poolId)
			goto end
		case conn := <-pool.returnedConnChan:
			if conn == nil {
				continue
			}
			if conn.isExpired() { //conn expired
				pool.expireConn(conn)
			} else if conn.isNeedRefresh() { //conn need refresh
				pool.refreshConn(conn)
			} else if conn.isActive() { // conn active
				pool.sshConnChan <- conn
			} else {
				log.Printf("[error][sshPool.watchConn][poolId:%d] conn condition not in (expire,needRefresh,active),connId:%d,creatTime:%d,updateTime:%d", pool.poolId, conn.connId, conn.createTime, conn.updateTime)
			}
		}
	}
end:
	//ack close
	pool.ackClose <- struct{}{}
}

func (pool *sshPool) expireConn(conn *sshConn) {
	conn.close()
	<-pool.newConnChan
}

func (pool *sshPool) refreshConn(conn *sshConn) {
	if err := conn.ping(); err != nil {
		log.Printf("[error][sshPool.refreshConn][poolId:%d] conn ping error.connId:%d,err:%s", conn.poolId, conn.connId, err.Error())
		conn.close()
		<-pool.newConnChan
	} else {
		pool.sshConnChan <- conn
	}
}

func (pool *sshPool) ping() error {
	//space: []byte{'\x20'}
	_, _, err := pool.sshClient.SendRequest("keepalive", true, []byte{'\x20'})
	if err != nil {
		log.Printf("[error][sshPool.monitor][poolId:%d] sshClient SendRequest err:%s", pool.poolId, err.Error())
		return err
	}
	return nil
}

func (pool *sshPool) newSshConn() (*sshConn, error) {
	netConn, err := pool.sshClient.Dial("tcp", pool.remoteHost)
	if err != nil {
		log.Printf("[error][sshPool.newSshConn] ssh dial remote host(%s) err:%s", pool.remoteHost, err.Error())
		return nil, err
	}
	conn := sshConn{
		netConn:    netConn,
		poolId:     pool.poolId,
		connId:     atomic.AddInt64(&pool.currentConnId, 1),
		createTime: time.Now().Unix(),
		updateTime: time.Now().Unix(),
	}
	return &conn, nil
}

func (pool *sshPool) isCanceled() bool {
	return pool.ctx.Err() != nil
}

func (pool *sshPool) close() error {
	if err := pool.sshClient.Close(); err != nil {
		log.Printf("[error][sshPool.close][poolId:%d] sshClient Close error.err:%s", pool.poolId, err.Error())
		return fmt.Errorf("sshPool sshClient close err:%s", err.Error())
	}
	//close self chan
	close(pool.closeChan)
	//wait routine exit
	<-pool.ackClose
	<-pool.ackClose
	<-pool.ackClose
	//close ack
	close(pool.ackClose)

	//close new chan
	for {
		select {
		case <-pool.newConnChan:
		default:
			goto end1New
		}
	}
end1New:
	close(pool.newConnChan)
	//close returned chan
	for {
		select {
		case conn := <-pool.returnedConnChan:
			if conn != nil {
				conn.close()
			}
		default:
			goto end2Returned
		}
	}
end2Returned:
	close(pool.returnedConnChan)

	pool.sshClient = nil
	pool.sshConnChan = nil
	pool.exit = nil

	log.Printf("[info][sshPool.close][poolId:%d] pool close success", pool.poolId)
	return nil
}

type sshConn struct {
	netConn    net.Conn
	poolId     int64
	connId     int64
	createTime int64
	updateTime int64
}

func (conn *sshConn) isExpired() bool {
	return conn.createTime+30 < time.Now().Unix()
}

func (conn *sshConn) isNeedRefresh() bool {
	//greater than 3 second
	b1 := conn.updateTime+3 < time.Now().Unix()
	// less than 30 second
	b2 := conn.updateTime+30 > time.Now().Unix()
	return b1 && b2
}

func (conn *sshConn) isActive() bool {
	return conn.updateTime+3 > time.Now().Unix()
}

func (conn *sshConn) close() error {
	return conn.netConn.Close()
}

func (conn *sshConn) ping() error {
	//space: []byte{'\x20'}
	conn.netConn.SetWriteDeadline(afterAwhile(2))
	_, err := conn.netConn.Write([]byte{'\x20'})
	if err != nil {
		log.Printf("[error][sshConn.ping][poolId:%d][connId:%d] ssh write err:%s", conn.poolId, conn.connId, err.Error())
		return fmt.Errorf("ping err:%s", err.Error())
	}
	conn.netConn.SetWriteDeadline(time.Time{})
	//update time
	conn.updateTime = time.Now().Unix()
	return nil
}

//after seconds
func afterAwhile(duration time.Duration) time.Time {
	return time.Now().Add(time.Second * duration)
}

//sleep seconds
func sleepAwhile(duration time.Duration) {
	time.Sleep(time.Second * duration)
}
