package main

import (
	"crypto/tls"
	"log"
	"net"

	"github.com/globalsign/mgo"
)

// var mongoURI = "mongodb://username:password@prefix1.mongodb.net,prefix2.mongodb.net,prefix3.mongodb.net/dbName?replicaSet=replName&authSource=admin"
// var mongoURI = "mongodb+srv://denny515:charanram@cluster0-1xul8.mongodb.net/test?retryWrites=true&w=majority"

func abc() error {
	dialInfo, err := mgo.ParseURL(mongoURI)

	//Below part is similar to above.
	tlsConfig := &tls.Config{}
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}
	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Printf("Successfully connected")
	}
	defer session.Close()
	return err
}

func main() {
	abc()
}
