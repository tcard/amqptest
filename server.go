// Package amqptest provides utilities for AMQP testing.
package amqptest

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/tcard/amqptest/internal"
)

// NewServer starts and returns a new Server.
// The caller should call Close when finished, to shut it down.
func NewServer(cfg *Config) *Server {
	s := NewUnstartedServer(cfg)
	s.Start()
	return s
}

// NewUnstartedServer returns a new Server but doesn't start it.
//
// After changing its configuration, the caller should call Start or
// StartTLS.
//
// The caller should call Close when finished, to shut it down.
func NewUnstartedServer(cfg *Config) *Server {
	return &Server{
		Config:   cfg,
		Listener: newLocalListener(),
		conns:    make(map[net.Conn]io.Writer),
	}
}

// A Server is an AMQP server listening on a system-chosen port on the
// local loopback interface, for use in end-to-end AMQP tests.
type Server struct {
	URL      string
	Listener net.Listener
	Config   *Config

	// TLS is the optional TLS configuration, populated with a new config
	// after TLS is started. If set on an unstarted server before StartTLS
	// is called, existing fields are copied into the new config.
	TLS *tls.Config

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]io.Writer // except terminal states
}

// A Config configures a Server. Hook functions can be set up that will be
// called when the server processes the corresponding operations.
type Config struct {
	OnConnectionOpen  func(conn net.Conn) error
	OnConnectionClose func(conn net.Conn) error

	OnChannelOpen  func(conn net.Conn, channelID uint16) error
	OnChannelClose func(conn net.Conn, channelID uint16) error

	OnQueueDeclare func(conn net.Conn, channelID uint16, args *QueueDeclare) error

	OnConsume func(conn net.Conn, channelID uint16, args *BasicConsume) error

	OnPublish func(conn net.Conn, channelID uint16, args *BasicPublish, confirm func(bool)) error
}

// Close shuts down the server and blocks until all outstanding
// requests on this server have completed.
func (s *Server) Close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.Listener.Close()
		for c, _ := range s.conns {
			c.Close()
		}
	}
	s.mu.Unlock()
}

// Start starts a server from NewUnstartedServer.
func (s *Server) Start() {
	if s.URL != "" {
		panic("Server already started")
	}
	s.URL = "amqp://" + s.Listener.Addr().String()
	s.goServe()
	if *serve != "" {
		fmt.Fprintln(os.Stderr, "amqptest: serving on", s.URL)
		select {}
	}
}

// StartTLS starts TLS on a server from NewUnstartedServer.
func (s *Server) StartTLS() {
	if s.URL != "" {
		panic("Server already started")
	}
	cert, err := tls.X509KeyPair(internal.LocalhostCert, internal.LocalhostKey)
	if err != nil {
		panic(fmt.Sprintf("amqptest: NewTLSServer: %v", err))
	}

	existingConfig := s.TLS
	s.TLS = new(tls.Config)
	if existingConfig != nil {
		*s.TLS = *existingConfig
	}
	if s.TLS.NextProtos == nil {
		s.TLS.NextProtos = []string{"amqp/0-9-1"}
	}
	if len(s.TLS.Certificates) == 0 {
		s.TLS.Certificates = []tls.Certificate{cert}
	}
	s.Listener = tls.NewListener(s.Listener, s.TLS)
	s.URL = "amqps://" + s.Listener.Addr().String()
	s.goServe()
}

func (s *Server) Deliver(conn net.Conn, channelID uint16, args *BasicDeliver) error {
	s.mu.Lock()
	w, ok := s.conns[conn]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("no active connection")
	}

	err := WriteFrame(w, &methodFrame{ChannelId: channelID, Method: args})
	if err != nil {
		return err
	}

	class, _ := args.id()
	err = WriteFrame(w, &headerFrame{
		ChannelId:  channelID,
		ClassId:    class,
		Size:       uint64(len(args.Body)),
		Properties: args.Properties,
	})
	if err != nil {
		return err
	}

	err = WriteFrame(w, &bodyFrame{ChannelId: channelID, Body: args.Body})
	if err != nil {
		return err
	}

	return nil
}

func newLocalListener() net.Listener {
	if *serve != "" {
		l, err := net.Listen("tcp", *serve)
		if err != nil {
			panic(fmt.Sprintf("amqptest: failed to listen on %v: %v", *serve, err))
		}
		return l
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("amqptest: failed to listen on a port: %v", err))
		}
	}
	return l
}

// When debugging a particular http server-based test,
// this flag lets you run
//	go test -run=BrokenTest -amqptest.serve=127.0.0.1:8000
// to start the broken server so you can interact with it manually.
var serve = flag.String("amqptest.serve", "", "if non-empty, amqptest.NewServer serves on this address and blocks")

func (s *Server) goServe() {
	go func() {
		for {
			conn, err := s.Listener.Accept()
			if err != nil {
				continue
			}

			w := bufio.NewWriter(conn)
			confirmingChannels := map[uint16]struct{}{}
			go func() {
				_, err := conn.Read(make([]byte, len("AMPQ0091")))
				if err != nil {
					return
				}
				defer conn.Close()
				s.mu.Lock()
				s.conns[conn] = w
				s.mu.Unlock()
				defer func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					delete(s.conns, conn)
				}()

				_, err = conn.Write([]byte("\x01\x00\x00\x00\x00\x01\xc8\x00\n\x00\n\x00\t\x00\x00\x01\xa3\fcapabilitiesF\x00\x00\x00\xb5\x12publisher_confirmst\x01\x1aexchange_exchange_bindingst\x01\nbasic.nackt\x01\x16consumer_cancel_notifyt\x01\x12connection.blockedt\x01\x13consumer_prioritiest\x01\x1cauthentication_failure_closet\x01\x10per_consumer_qost\x01\fcluster_nameS\x00\x00\x00\bsquirrel\tcopyrightS\x00\x00\x00.Copyright (C) 2007-2015 Pivotal Software, Inc.\vinformationS\x00\x00\x005Licensed under the MPL.  See http://www.rabbitmq.com/\bplatformS\x00\x00\x00\nErlang/OTP\aproductS\x00\x00\x00\bRabbitMQ\aversionS\x00\x00\x00\x053.5.7\x00\x00\x00\x0eAMQPLAIN PLAIN\x00\x00\x00\x05en_US\xce"))
				if err != nil {
					return
				}

				for _, m := range []string{
					"\x01\x00\x00\x00\x00\x00\f\x00\n\x00\x1e\x00\xc8\x00\x02\x00\x00\x00x\xce",
					"\x01\x00\x00\x00\x00\x00\x05\x00\n\x00)\x00\xce",
				} {
					_, err = ReadFrame(conn)
					if err != nil {
						return
					}

					_, err = conn.Write([]byte(m))
					if err != nil {
						return
					}
				}

				if f := s.Config.OnConnectionOpen; f != nil {
					f(conn)
				}

				frames := make(chan frame)
				go func() {
					for {
						frame, err := ReadFrame(conn)
						if err != nil {
							return
						}
						frames <- frame
					}
				}()

				type confirmation struct {
					channelId   uint16
					deliveryTag uint64
				}

				confirms := make(chan confirmation, 1)
				deliveryTags := map[uint16]uint64{}

				for {
					select {
					case c := <-confirms:
						WriteFrame(w, &methodFrame{ChannelId: c.channelId, Method: &basicAck{DeliveryTag: c.deliveryTag}})
					case frame := <-frames:
						switch frame := frame.(type) {
						case *heartbeatFrame:
							WriteFrame(w, frame)
						case *methodFrame:
							switch m := frame.Method.(type) {
							case *connectionClose:
								var err error
								if f := s.Config.OnConnectionClose; f != nil {
									err = f(conn)
								}
								if err == nil {
									WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &connectionCloseOk{}})
									conn.Close()
									break
								}
							case *channelOpen:
								var err error
								if f := s.Config.OnChannelOpen; f != nil {
									err = f(conn, frame.ChannelId)
								}
								if err == nil {
									WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &channelOpenOk{}})
								}
							case *channelClose:
								var err error
								if f := s.Config.OnChannelClose; f != nil {
									err = f(conn, frame.ChannelId)
								}
								if err == nil {
									WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &channelCloseOk{}})
								}
								delete(confirmingChannels, frame.ChannelId)
							case *QueueDeclare:
								var err error
								if f := s.Config.OnQueueDeclare; f != nil {
									err = f(conn, frame.ChannelId, m)
								}
								if err == nil {
									WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &queueDeclareOk{Queue: m.Queue}})
								}
							case *confirmSelect:
								WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &confirmSelectOk{}})
								confirmingChannels[frame.ChannelId] = struct{}{}
							case *BasicPublish:
								deliveryTags[frame.ChannelId]++
								deliveryTag := deliveryTags[frame.ChannelId]

								m.Properties = (<-frames).(*headerFrame).Properties
								m.Body = (<-frames).(*bodyFrame).Body

								if f := s.Config.OnPublish; f != nil {
									var confirmFunc func(bool)
									if _, ok := confirmingChannels[frame.ChannelId]; ok {
										frame := frame
										confirmFunc = func(confirm bool) {
											if confirm {
												confirms <- confirmation{frame.ChannelId, deliveryTag}
											}
										}
									} else {
										confirmFunc = func(bool) {}
									}
									f(conn, frame.ChannelId, m, confirmFunc)
								}
							case *BasicConsume:
								var err error
								if f := s.Config.OnConsume; f != nil {
									err = f(conn, frame.ChannelId, m)
								}
								if err == nil {
									WriteFrame(w, &methodFrame{ChannelId: frame.ChannelId, Method: &basicConsumeOk{ConsumerTag: m.ConsumerTag}})
								}
							}
						default:
							panic(fmt.Errorf("unhandled frame type: %T", frame))
						}
					}
				}
			}()
		}
	}()
}
