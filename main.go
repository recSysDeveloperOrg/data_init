package main

import (
	"data_init/config"
	"data_init/model"
	"data_init/scrape"
)

const (
	RatingsFile = "ml-latest/ratings.csv"
	TagFile     = "ml-latest/tags.csv"
	LinkFile    = "ml-latest/links.csv"
)

func main() {
	if err := config.InitConfig(); err != nil {
		panic(err)
	}
	if err := model.InitModel(); err != nil {
		panic(err)
	}
	if err := model.DoRatingModels(RatingsFile); err != nil {
		panic(err)
	}
	if err := model.DoUserModels(RatingsFile); err != nil {
		panic(err)
	}
	if err := model.DoTagModels(TagFile); err != nil {
		panic(err)
	}
	if err := scrape.DoScrape(LinkFile, RatingsFile); err != nil {
		panic(err)
	}
}
