package graphmod

import "go.mongodb.org/mongo-driver/bson/primitive"

type User struct {
	ID    primitive.ObjectID `bson:"_id"`
	Name  string             `json:"name"`
	Email string             `json:"email"`
	Pass  string             `json:"pass"`
}

type GQLBook struct {
	ID string
}

