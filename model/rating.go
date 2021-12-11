package model

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type Rating struct {
	UserID  string  `bson:"user_id"`
	MovieID string  `bson:"movie_id"`
	Rating  float64 `bson:"rating"`
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

		user, movie := row[0], row[1]
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return err
		}

		res = append(res, &Rating{
			UserID:  user,
			MovieID: movie,
			Rating:  rating,
		})
	}

	if _, err := GetClient().Collection(CollectionRating).InsertMany(context.Background(), res); err != nil {
		return err
	}

	return nil
}
