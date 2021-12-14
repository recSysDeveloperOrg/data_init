package model

import (
	"context"
	"encoding/csv"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"os"
	"strconv"
)

type Rating struct {
	UserID  primitive.ObjectID `bson:"user_id"`
	MovieID primitive.ObjectID `bson:"movie_id"`
	Rating  float64            `bson:"rating"`
}

func DoRatingModels(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(file)
	// skip header
	_, err = csvReader.Read()
	if err != nil {
		return err
	}

	res := make([]interface{}, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		userObjectID, err := objectIDFromHexString(row[0])
		if err != nil {
			return err
		}
		movieObjectID, err := objectIDFromHexString(row[1])
		if err != nil {
			return err
		}
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return err
		}

		res = append(res, &Rating{
			UserID:  userObjectID,
			MovieID: movieObjectID,
			Rating:  rating,
		})
	}

	if _, err := GetClient().Collection(CollectionRating).InsertMany(context.Background(), res); err != nil {
		return err
	}

	return nil
}
