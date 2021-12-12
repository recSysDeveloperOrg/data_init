package model

import (
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

type movieResponse struct {
	OriginalTitle    string `json:"original_title"`
	OriginalLanguage string `json:"original_language"`
	Overview         string `json:"overview"`
	ReleaseDate      string `json:"release_date"`
}

type castResponse struct {
	Cast []*cast `json:"cast"`
}

type cast struct {
	Character string `json:"character"`
	Name      string `json:"name"`
}

type imgResponse struct {
	Posters []*img `json:"posters"`
}

type img struct {
	FilePath string `json:"file_path"`
	Height   int    `json:"height"`
	Width    int    `json:"width"`
	Language string `json:"iso_639_1"`
}

type participant struct {
	Character string `json:"character"`
	Name      string `json:"name"`
}

type movie struct {
	Title           string         `json:"title"`
	PicUrl          string         `json:"pic_url"`
	Introduction    string         `json:"introduction"`
	Participants    []*participant `json:"participants"`
	ReleaseDate     string         `json:"release_date"`
	Language        string         `json:"language"`
	UniqueRatingCnt uint64         `json:"unique_rating_cnt"`
	AverageRating   float64        `json:"average_rating"`
}

type movieTMDB struct {
	tmdbID string
	movie  *movie
}

const (
	localProxyURL  = "http://127.0.0.1:41091"
	movieDetailApi = "https://api.themoviedb.org/3/movie/%s?api_key=b05a4ee65e0bb190f80f0e2c56ffbc48&language=zh-CN"
	movieCastApi   = "https://api.themoviedb.org/3/movie/%s/credits?api_key=b05a4ee65e0bb190f80f0e2c56ffbc48"
	movieImgApi    = "https://api.themoviedb.org/3/movie/%s/images?api_key=b05a4ee65e0bb190f80f0e2c56ffbc48"
)

var tr *http.Transport

func initTransport() {
	proxy, _ := url.Parse(localProxyURL)
	tr = &http.Transport{
		Proxy:           http.ProxyURL(proxy),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
}

func DoMovieModels(linkFile, ratingFile, tempSaveFile string) error {
	initTransport()
	rows, err := getMovieLinkRows(linkFile)
	if err != nil {
		return err
	}
	movieID2UniqueCnt, movieID2TotalRating, err := getRatingStatus(ratingFile)
	if err != nil {
		return err
	}

	coroutineCnt := 20
	tmdbMovieChan, doneChan, errChan := make(chan *movieTMDB, len(rows)), make(chan struct{}), make(chan error, len(rows))
	for i := 0; i < coroutineCnt; i++ {
		go func() {
			for {
				tmdbMovie, ok := <-tmdbMovieChan
				if !ok {
					break
				}
				fillProperties(tmdbMovie.tmdbID, tmdbMovie.movie, errChan)
			}

			doneChan <- struct{}{}
		}()
	}

	movies := make([]*movie, 0)
	for _, row := range rows {
		movieID, tmdbID := row[0], row[2]
		movie := &movie{
			UniqueRatingCnt: movieID2UniqueCnt[movieID],
			AverageRating:   movieID2TotalRating[movieID] / float64(movieID2UniqueCnt[movieID]),
		}
		movies = append(movies, movie)
		tmdbMovie := &movieTMDB{
			tmdbID: tmdbID,
			movie:  movie,
		}
		tmdbMovieChan <- tmdbMovie
	}

	close(tmdbMovieChan)
	// wait all coroutines to finish
	for i := 0; i < coroutineCnt; i++ {
		select {
		case <-doneChan:
			log.Println("one coroutine finished")
		case err := <-errChan:
			return err
		}
	}

	close(errChan)
	if err, ok := <-errChan; ok {
		return err
	}
	close(doneChan)

	return saveMovies(movies, tempSaveFile)
}

func getMovieLinkRows(linkFile string) ([][]string, error) {
	file, err := os.Open(linkFile)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(file)
	if _, err = csvReader.Read(); err != nil {
		return nil, err
	}

	rows := make([][]string, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func getRatingStatus(ratingFile string) (map[string]uint64, map[string]float64, error) {
	file, err := os.Open(ratingFile)
	if err != nil {
		return nil, nil, err
	}
	csvReader := csv.NewReader(file)
	if _, err = csvReader.Read(); err != nil {
		return nil, nil, err
	}

	movieID2UniqueCnt, movieID2TotalRating := make(map[string]uint64), make(map[string]float64)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		movieID := row[1]
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, nil, err
		}
		movieID2UniqueCnt[movieID]++
		movieID2TotalRating[movieID] += rating
	}

	return movieID2UniqueCnt, movieID2TotalRating, nil
}

func saveMovies(movies []*movie, tempSaveFile string) error {
	bytes, err := json.Marshal(movies)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(tempSaveFile, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	nWrite, err := file.Write(bytes)
	if err != nil {
		return err
	}
	if nWrite != len(bytes) {
		return errors.New("write length not equal to mem byte slice length")
	}

	return nil
}

func fillProperties(id string, movie *movie, errChan chan<- error) {
	if err := getMovieDetail(id, movie); err != nil {
		errChan <- err
	}
	if err := getMovieCast(id, movie); err != nil {
		errChan <- err
	}
	if err := getMovieImg(id, movie); err != nil {
		errChan <- err
	}
}

func getProxyClient() *http.Client {
	return &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}
}

func request(url string) ([]byte, error) {
	resp, err := getProxyClient().Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bodyReader := resp.Body
	bytes, err := ioutil.ReadAll(bodyReader)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func getMovieDetail(id string, movie *movie) error {
	bytes, err := request(fmt.Sprintf(movieDetailApi, id))
	movieResp := movieResponse{}
	if err = json.Unmarshal(bytes, &movieResp); err != nil {
		return err
	}

	movie.Introduction = movieResp.Overview
	movie.Title = movieResp.OriginalTitle
	movie.Language = movieResp.OriginalLanguage
	movie.ReleaseDate = movieResp.ReleaseDate

	return nil
}

func getMovieCast(id string, movie *movie) error {
	bytes, err := request(fmt.Sprintf(movieCastApi, id))
	castResp := castResponse{}
	if err = json.Unmarshal(bytes, &castResp); err != nil {
		return err
	}

	for _, cast := range castResp.Cast {
		movie.Participants = append(movie.Participants, &participant{
			Character: cast.Character,
			Name:      cast.Name,
		})
	}

	return nil
}

func getMovieImg(id string, movie *movie) error {
	bytes, err := request(fmt.Sprintf(movieImgApi, id))
	imgResp := imgResponse{}
	if err = json.Unmarshal(bytes, &imgResp); err != nil {
		return err
	}

	for _, img := range imgResp.Posters {
		if img.Language == "en" {
			movie.PicUrl = img.FilePath
			break
		}
	}

	return nil
}
