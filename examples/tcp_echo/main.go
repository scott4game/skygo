package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/scott4game/skygo/frame"
	"github.com/scott4game/skygo/tcpsync"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	go serve(listener)

	pool, err := tcpsync.New(listener.Addr().String(), tcpsync.Options{
		MaxSize:        2,
		RequestTimeout: time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	response, err := pool.Do(context.Background(), []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(response))
}

func serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			for {
				payload, err := frame.Read(conn)
				if err != nil {
					return
				}
				if err := frame.Write(conn, payload); err != nil {
					return
				}
			}
		}()
	}
}
