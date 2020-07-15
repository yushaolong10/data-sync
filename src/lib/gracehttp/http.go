// Package gracehttp provides easy to use graceful restart
// functionality for HTTP server.
package gracehttp

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/facebookgo/grace/gracenet"
	"github.com/facebookgo/httpdown"
	"google.golang.org/grpc"
)

var (
	logger     *log.Logger
	didInherit = os.Getenv("LISTEN_FDS") != ""
	ppid       = os.Getppid()
)

type option func(*app)

// An app contains one or more servers and associated configuration.
type app struct {
	servers         []*http.Server
	gservers        []*GRPCServer
	http            *httpdown.HTTP
	net             *gracenet.Net
	listeners       []net.Listener
	sds             []httpdown.Server
	preStartProcess func() error
	errors          chan error
}

// GRPCServer grpc.Server packaging
type GRPCServer struct {
	Addr      string // address
	Server    *grpc.Server
	serveDone chan struct{}
}

// NewGRPCServer GRPCServer package factory
func NewGRPCServer(Addr string, Server *grpc.Server) *GRPCServer {
	return &GRPCServer{
		Addr:      Addr,
		Server:    Server,
		serveDone: make(chan struct{}),
	}
}

// Wait implement interface for httpdown.Server
func (s *GRPCServer) Wait() error {
	<-s.serveDone
	// fmt.Printf("gserver wait done, port:%s\n", s.Addr)
	return nil
}

// close implement interface for httpdown.Server
func (s *GRPCServer) Stop() error {
	s.Server.GracefulStop()
	close(s.serveDone)
	// fmt.Printf("gserver gracefull stop: port:%s\n", s.Addr)
	return nil
}

func newApp(servers []*http.Server, gservers []*GRPCServer) *app {
	return &app{
		servers:         servers,
		gservers:        gservers,
		http:            &httpdown.HTTP{},
		net:             &gracenet.Net{},
		listeners:       make([]net.Listener, 0, len(servers)+len(gservers)),
		sds:             make([]httpdown.Server, 0, len(servers)+len(gservers)),
		preStartProcess: func() error { return nil },
		// 2x num servers for possible Close or close errors + 1 for possible
		// StartProcess error.
		errors: make(chan error, 1+(len(servers)*2)+(len(gservers)*2)),
	}
}

func (a *app) listen() error {
	for _, s := range a.servers {
		// TODO: default addresses
		l, err := a.net.Listen("tcp", s.Addr)
		if err != nil {
			return err
		}
		if s.TLSConfig != nil {
			l = tls.NewListener(l, s.TLSConfig)
		}
		// fmt.Printf("http listener %s\n", s.Addr)
		a.listeners = append(a.listeners, l)
	}
	for _, s := range a.gservers {
		// TODO: default addresses
		l, err := a.net.Listen("tcp", s.Addr)
		if err != nil {
			return err
		}
		// fmt.Printf("grpc listener %s\n", s.Addr)
		a.listeners = append(a.listeners, l)
	}
	return nil
}

func (a *app) serve() {
	for i, s := range a.servers {
		a.sds = append(a.sds, a.http.Serve(s, a.listeners[i]))
	}
	for i, s := range a.gservers {
		gserver := s.Server
		gl := a.listeners[(i + len(a.servers))]

		var gerr error
		go func() {
			err := gserver.Serve(gl)
			if err != nil {
				gerr = err
				fmt.Printf("Start grpc server error,error msg=%s", err)
				return
			}
		}()
		time.Sleep(time.Millisecond * 100)
		if gerr != nil {
			fmt.Printf("Start grpc server error after sleep 100ms ,error msg=%s", gerr)
			return
		}
		a.sds = append(a.sds, s)

	}
}

func (a *app) wait() {
	var wg sync.WaitGroup
	wg.Add(len(a.sds) * 2) // Wait & close
	go a.signalHandler(&wg)
	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Wait(); err != nil {
				a.errors <- err
			}
		}(s)
	}
	wg.Wait()
}

func (a *app) term(wg *sync.WaitGroup) {
	for _, s := range a.sds {
		go func(s httpdown.Server) {
			defer wg.Done()
			if err := s.Stop(); err != nil {
				a.errors <- err
			}
		}(s)
	}
}

func (a *app) signalHandler(wg *sync.WaitGroup) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			// this ensures a subsequent INT/TERM will trigger standard go behaviour of
			// terminating.
			signal.Stop(ch)
			a.term(wg)
			return
		case syscall.SIGUSR2:
			err := a.preStartProcess()
			if err != nil {
				a.errors <- err
			}
			// we only return here if there's an error, otherwise the new process
			// will send us a TERM when it's ready to trigger the actual shutdown.
			if _, err := a.net.StartProcess(); err != nil {
				a.errors <- err
			}
		}
	}
}

func (a *app) run() error {
	// Acquire Listeners
	if err := a.listen(); err != nil {
		return err
	}

	// Some useful logging.
	// if logger != nil {
	if didInherit {
		if ppid == 1 {
			// logger.Printf("Listening on init activated %s", pprintAddr(a.listeners))
			fmt.Printf("Listening on init activated %s", pprintAddr(a.listeners))
		} else {
			const msg = "Graceful handoff of %s with new pid %d and old pid %d\n"
			// logger.Printf(msg, pprintAddr(a.listeners), os.Getpid(), ppid)
			fmt.Printf(msg, pprintAddr(a.listeners), os.Getpid(), ppid)
		}
	} else {
		const msg = "Serving %s with pid %d"
		// logger.Printf(msg, pprintAddr(a.listeners), os.Getpid())
		fmt.Printf(msg, pprintAddr(a.listeners), os.Getpid())
	}
	// }

	// Start serving.
	a.serve()

	// Close the parent if we inherited and it wasn't init that started us.
	if didInherit && ppid != 1 {
		if err := syscall.Kill(ppid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to close parent: %s", err)
		}
	}

	waitdone := make(chan struct{})
	go func() {
		defer close(waitdone)
		a.wait()
	}()

	select {
	case err := <-a.errors:
		if err == nil {
			panic("unexpected nil error")
		}
		return err
	case <-waitdone:
		// if logger != nil {
		// 	logger.Printf("Exiting pid %d.", os.Getpid())
		// }
		fmt.Printf("Exiting pid %d.\n", os.Getpid())
		return nil
	}
}

// ServeWithOptions does the same as Serve, but takes a set of options to
// configure the app struct.
func ServeWithOptions(servers []*http.Server, gservers []*GRPCServer, options ...option) error {
	a := newApp(servers, gservers)
	for _, opt := range options {
		opt(a)
	}
	return a.run()
}

// Serve will serve the given http.Servers and will monitor for signals
// allowing for graceful termination (SIGTERM) or restart (SIGUSR2).
func Serve(servers ...*http.Server) error {
	a := newApp(servers, []*GRPCServer{})
	return a.run()
}

// PreStartProcess configures a callback to trigger during graceful restart
// directly before starting the successor process. This allows the current
// process to release holds on resources that the new process will need.
func PreStartProcess(hook func() error) option {
	return func(a *app) {
		a.preStartProcess = hook
	}
}

// Used for pretty printing addresses.
func pprintAddr(listeners []net.Listener) []byte {
	var out bytes.Buffer
	for i, l := range listeners {
		if i != 0 {
			fmt.Fprint(&out, ", ")
		}
		fmt.Fprint(&out, l.Addr())
	}
	return out.Bytes()
}

// SetLogger sets logger to be able to grab some useful logs
func SetLogger(l *log.Logger) {
	logger = l
}
