package main

import (
	"data_init/config"
	"data_init/model"
)

const (
	RatingsFile       = "ml-latest/ratings.csv"
	TagFile           = "ml-latest/tags.csv"
	LinkFile          = "ml-latest/links.csv"
	TempStore         = "temp_store.json"
	MissingMovieStore = "temp_store_missing_movie.json"
)

func main() {
	if err := config.InitConfig(); err != nil {
		panic(err)
	}
	if err := model.InitModel(); err != nil {
		panic(err)
	}
	//if err := model.DoRatingModels(RatingsFile); err != nil {
	//	panic(err)
	//}
	//if err := model.DoUserModels(RatingsFile); err != nil {
	//	panic(err)
	//}
	//if err := model.DoTagModels(TagFile); err != nil {
	//	panic(err)
	//}
	if err := model.DoMovieModels(LinkFile, RatingsFile, TempStore, MissingMovieStore); err != nil {
		panic(err)
	}
}
