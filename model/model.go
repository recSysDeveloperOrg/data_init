package model

import (
	"context"
	"data_init/config"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io"
	"os"
	"strconv"
)

var (
	dbClient *mongo.Database
	client   *mongo.Client
)

const (
	CollectionUser           = "user"
	CollectionMovie          = "movie"
	CollectionRating         = "rating"
	CollectionTag            = "tag"
	CollectionTagUser        = "tag_user"
	CollectionTagMovie       = "tag_movie"
	CollectionUserRatingMeta = "user_rating_meta"
)

func GetClient() *mongo.Database {
	return dbClient
}

func InitModel() error {
	cfg := config.GetConfig().Mongo
	clientOps := options.Client().ApplyURI(cfg.Url)
	clientOps.Auth = &options.Credential{
		Username: cfg.User,
		Password: cfg.Password,
	}
	cli, err := mongo.Connect(context.Background(), clientOps)
	if err != nil {
		return err
	}

	if err = cli.Ping(context.Background(), readpref.Primary()); err != nil {
		return err
	}

	client = cli
	dbClient = cli.Database(cfg.DBName)
	return nil
}

func Disconnect() error {
	return client.Disconnect(context.Background())
}

func objectIDFromHexString(hex string) (primitive.ObjectID, error) {
	hexMovieID, err := strconv.ParseInt(hex, 16, 64)
	if err != nil {
		return [12]byte{}, err
	}
	movieObjectID, err := primitive.ObjectIDFromHex(fmt.Sprintf("%024x", hexMovieID))
	if err != nil {
		return [12]byte{}, err
	}

	return movieObjectID, nil
}

func emptyTMDBMovieSet(linkFile string) (map[string]struct{}, error) {
	file, err := os.Open(linkFile)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(file)
	if _, err = csvReader.Read(); err != nil {
		return nil, err
	}

	set := make(map[string]struct{})
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if row[2] == "" {
			set[row[0]] = struct{}{}
		}
	}

	return set, nil
}
