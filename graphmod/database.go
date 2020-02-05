package graphmod

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var NewMangoClient *mongo.Client

func NewDatabase() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb+srv://denny515:charanram@cluster0-1xul8.mongodb.net/test?retryWrites=true&w=majority"))
	if err != nil {
		panic(err)
	}
	
	NewMangoClient = client
}
