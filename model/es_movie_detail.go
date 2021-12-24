package model

import (
	"context"
	"github.com/olivere/elastic/v7"
	"log"
)

type movieDetail struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

var index = "{\n  \"settings\": {\n    \"number_of_shards\": 3,\n    \"number_of_replicas\": 2\n  },\n  \"mapping\": {\n    \"_doc\": {\n      \"properties\": {\n        \"id\": {\n          \"type\": \"keyword\"\n        },\n        \"title\": {\n          \"type\": \"keyword\"\n        },\n        \"detail\": {\n          \"type\": \"text\"\n        }\n      } \n    }\n  }\n}"

func DoMovieDetailModels(tempMovieFile string) error {
	movies, err := recoverFromJson(tempMovieFile)
	if err != nil {
		return err
	}
	details := make([]*movieDetail, len(movies))
	for i, movie := range movies {
		details[i] = &movieDetail{
			ID:     movie.MovieID.String(),
			Title:  movie.Title,
			Detail: movie.Introduction,
		}
	}

	esClient, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://110.42.250.18:9200/"))
	if err != nil {
		return err
	}
	log.Printf("connected to es")
	bulkRequests := make([]elastic.BulkableRequest, len(details))
	for i, detail := range details {
		bulkRequests[i] = elastic.NewBulkIndexRequest().Index("movie").Doc(detail)
	}
	res, err := esClient.Bulk().Index("movie").Add(bulkRequests...).Do(context.Background())
	if err != nil {
		return err
	}

	log.Printf("%+v", res)
	return nil
}
