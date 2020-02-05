package graphmod

import (
	"context"
	"fmt"
	"log"

	// bsno "github.com/globalsign/mgo/bson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
) // THIS CODE IS A STARTING POINT ONLY. IT WILL NOT BE UPDATED WITH SCHEMA CHANGES.

type Resolver struct{}

func (r *Resolver) Mutation() MutationResolver {
	return &mutationResolver{r}
}
func (r *Resolver) Query() QueryResolver {
	return &queryResolver{r}
}
func (r *Resolver) User() UserResolver {
	return &userResolver{r}
}

type mutationResolver struct{ *Resolver }

func (r *mutationResolver) CreateUser(ctx context.Context, input *NewUser) (*User, error) {

	newUser := &User{
		ID:    primitive.NewObjectID(),
		Name:  input.Name,
		Email: input.Email,
		Pass:  input.Pass,
	}

	cursor, err := NewMangoClient.Database("Account").Collection("users").InsertOne(ctx, newUser)
	if err != nil {
		log.Fatal(err)
	}
	// for cursor.Next(context.TODO()) {
	// 	var user User
	// 	err := cursor.Decode(&user)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	userArr = append(userArr, &user)
	// }
	// if err := cursor.Err(); err != nil {
	// 	log.Fatal(err)
	// }
	fmt.Printf("Print InsertedID %v", cursor.InsertedID)
	return newUser, nil
}

type queryResolver struct{ *Resolver }

func (r *queryResolver) User(ctx context.Context, limit *int) ([]*User, error) {
	// panic("not implemented")

	// Testing First

	/*var userArr []*User
	user := &User{
		ID:    primitive.ObjectID{1},
		Name:  "Raja",
		Email: "hello@xyz",
		Pass:  "1234",
	}

	userArr = append(userArr, user)

	return userArr, nil */

	findOptions := options.Find()
	if limit != nil {
		limit64 := int64(*limit)
		findOptions.SetLimit(limit64)
	}

	var userArr []*User
	cursor, err := NewMangoClient.Database("Account").Collection("users").Find(context.TODO(), bson.D{}, findOptions)
	// .Find(context.TODO(),bson.D{},findOptions)
	if err != nil {
		log.Fatal(err)
	}
	for cursor.Next(context.TODO()) {
		var user User
		err := cursor.Decode(&user)
		if err != nil {
			log.Fatal(err)
		}
		userArr = append(userArr, &user)
	}
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}

	cursor.Close(context.TODO())

	return userArr, nil
}

type userResolver struct{ *Resolver }

func (r *userResolver) ID(ctx context.Context, obj *User) (string, error) {
	// panic("not implemented")
	return obj.ID.Hex(), nil
}
