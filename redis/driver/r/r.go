package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/cockroachdb/cockroach/redis/driver"
	"github.com/cockroachdb/cockroach/security"
)

func main() {
	c := driver.Command{
		User:    security.RootUser,
		Command: "SET",
		Arguments: []string{"A", "123"},
	}
	pb, err := c.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	r, err := http.Post("http://127.0.1.1:26257/redis/Execute", "application/x-protobuf", bytes.NewReader(pb))
	fmt.Println("ERR", err, r.Status)
	io.Copy(os.Stdout, r.Body)
	fmt.Println()
	r.Body.Close()
	
	c = driver.Command{
		User:    security.RootUser,
		Command: "GET",
		Arguments: []string{"A"},
	}
	pb, err = c.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	r, err = http.Post("http://127.0.1.1:26257/redis/Execute", "application/x-protobuf", bytes.NewReader(pb))
	fmt.Println("ERR", err, r.Status)
	io.Copy(os.Stdout, r.Body)
	fmt.Println()
	r.Body.Close()
}
