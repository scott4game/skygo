package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/scott4game/skygo/actor"
)

func main() {
	system := actor.NewSystem(actor.SystemOptions{})
	service, ref, err := system.Reserve("echo", actor.ServiceOptions{})
	if err != nil {
		log.Fatal(err)
	}

	echo := actor.NewMethod[string, string]("echo")
	if err := actor.Register(service, echo, func(_ context.Context, request string) (string, error) {
		return request, nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		log.Fatal(err)
	}

	response, err := echo.Call(context.Background(), ref, "hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := system.Stop(ctx); err != nil {
		log.Fatal(err)
	}
}
