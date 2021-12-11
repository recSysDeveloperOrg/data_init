package model

import (
	"context"
	"encoding/csv"
	"errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"os"
	"strconv"
)

type Tag struct {
	Content string `bson:"content"`
}

type UserTag struct {
	UserID    string   `bson:"user_id"`
	MovieIDs  []string `bson:"movie_ids"`
	TagID     string   `bson:"tag_id"`
	UpdatedAt uint64   `bson:"updated_at"`
}

type MovieTag struct {
	MovieID     string `bson:"movie_id"`
	TagID       string `bson:"tag_id"`
	UpdatedAt   uint64 `bson:"updated_at"`
	TaggedTimes uint64 `bson:"tagged_times"`
}

type TagRow struct {
	UserID     string
	MovieID    string
	TagContent string
	UpdatedAt  uint64
}

func DoTagModels(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(file)
	// skip first row
	if _, err = csvReader.Read(); err != nil {
		return err
	}

	rows := make([]*TagRow, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		userID, movieID, content := row[0], row[1], row[2]
		timestamp, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil {
			return err
		}

		rows = append(rows, &TagRow{
			UserID:     userID,
			MovieID:    movieID,
			TagContent: content,
			UpdatedAt:  uint64(timestamp),
		})
	}

	uniqueTags := map[string]struct{}{}
	for i := 0; i < len(rows); i++ {
		if _, ok := uniqueTags[rows[i].TagContent]; !ok {
			uniqueTags[rows[i].TagContent] = struct{}{}
		}
	}
	tags := make([]interface{}, 0)
	for content, _ := range uniqueTags {
		tags = append(tags, &Tag{
			Content: content,
		})
	}

	res, err := GetClient().Collection(CollectionTag).InsertMany(context.Background(), tags)
	if err != nil {
		return err
	}

	if len(res.InsertedIDs) != len(tags) {
		return errors.New("not equal element count")
	}

	// UserTag Collection

	tag2TagId := make(map[string]string)
	for i := 0; i < len(tags); i++ {
		if tag, ok := tags[i].(*Tag); ok {
			if tagID, ok := res.InsertedIDs[i].(primitive.ObjectID); ok {
				tag2TagId[tag.Content] = tagID.Hex()
			}
		}
	}

	userTag2Struct := make(map[string]map[string]*UserTag)
	for i := 0; i < len(rows); i++ {
		if _, ok := userTag2Struct[rows[i].UserID]; !ok {
			userTag2Struct[rows[i].UserID] = make(map[string]*UserTag)
		}

		if _, ok := userTag2Struct[rows[i].UserID][tag2TagId[rows[i].TagContent]]; !ok {
			userTag2Struct[rows[i].UserID][tag2TagId[rows[i].TagContent]] = &UserTag{
				UserID:    rows[i].UserID,
				TagID:     tag2TagId[rows[i].TagContent],
				UpdatedAt: uint64(0),
			}
		}

		s := userTag2Struct[rows[i].UserID][tag2TagId[rows[i].TagContent]]
		s.MovieIDs = append(s.MovieIDs, rows[i].MovieID)
		s.UpdatedAt = maxUint64(s.UpdatedAt, rows[i].UpdatedAt)
	}

	userTags := make([]interface{}, 0)
	for _, tag2Struct := range userTag2Struct {
		for _, userTag := range tag2Struct {
			userTags = append(userTags, userTag)
		}
	}

	if _, err = GetClient().Collection(CollectionTagUser).InsertMany(context.Background(), userTags); err != nil {
		return err
	}

	// Movie Tag Collection
	movieTag2Struct := make(map[string]map[string]*MovieTag)
	for i := 0; i < len(rows); i++ {
		if _, ok := movieTag2Struct[rows[i].MovieID]; !ok {
			movieTag2Struct[rows[i].MovieID] = make(map[string]*MovieTag)
		}

		if _, ok := movieTag2Struct[rows[i].MovieID][tag2TagId[rows[i].TagContent]]; !ok {
			movieTag2Struct[rows[i].MovieID][tag2TagId[rows[i].TagContent]] = &MovieTag{
				MovieID:     rows[i].MovieID,
				TagID:       tag2TagId[rows[i].TagContent],
				UpdatedAt:   uint64(0),
				TaggedTimes: uint64(0),
			}
		}

		m := movieTag2Struct[rows[i].MovieID][tag2TagId[rows[i].TagContent]]
		m.UpdatedAt = maxUint64(m.UpdatedAt, rows[i].UpdatedAt)
		m.TaggedTimes++
	}

	movieTags := make([]interface{}, 0)
	for _, tag2MovieTags := range movieTag2Struct {
		for _, movieTag := range tag2MovieTags {
			movieTags = append(movieTags, movieTag)
		}
	}

	if _, err = GetClient().Collection(CollectionTagMovie).InsertMany(context.Background(), movieTags); err != nil {
		return err
	}
	return nil
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
