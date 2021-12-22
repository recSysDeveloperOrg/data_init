package model

import (
	"context"
	"data_init/config"
	"encoding/csv"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io"
	"os"
	"strings"
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

func fixLength(s string, fixLen int) string {
	if len(s) >= fixLen {
		return s
	}

	return "1" + strings.Repeat("0", fixLen-len(s)-1) + s
}

func objectIDFromHexString(hex string) (primitive.ObjectID, error) {
	movieObjectID, err := primitive.ObjectIDFromHex(fixLength(hex, 24))
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
