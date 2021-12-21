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

type RatingMeta struct {
	UserID      primitive.ObjectID `bson:"user_id"`
	TotalRating int64              `bson:"total_rating"`
}

func DoRatingModels(fileName, linkFile string) error {
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
	userID2RatingCnt := make(map[primitive.ObjectID]int64)
	emptyMovieSet, err := emptyTMDBMovieSet(linkFile)
	if err != nil {
		return err
	}
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
		// TMDB未收录该电影，需要跳过
		if _, ok := emptyMovieSet[row[1]]; ok {
			continue
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
		userID2RatingCnt[userObjectID]++
	}

	if _, err := GetClient().Collection(CollectionRating).InsertMany(context.Background(), res); err != nil {
		return err
	}

	res = make([]interface{}, 0)
	for userObjectID, ratingCnt := range userID2RatingCnt {
		res = append(res, &RatingMeta{
			UserID:      userObjectID,
			TotalRating: ratingCnt,
		})
	}
	if _, err := GetClient().Collection(CollectionUserRatingMeta).InsertMany(context.Background(), res); err != nil {
		return err
	}

	return nil
}
