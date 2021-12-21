package model

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type movieResponse struct {
	OriginalTitle    string `json:"original_title"`
	OriginalLanguage string `json:"original_language"`
	Overview         string `json:"overview"`
	ReleaseDate      string `json:"release_date"`
	PosterPath       string `json:"poster_path"`
}

type castResponse struct {
	Cast []*cast `json:"cast"`
}

type cast struct {
	Character string `json:"character"`
	Name      string `json:"name"`
}

type participant struct {
	Character string `json:"character" bson:"character"`
	Name      string `json:"name" bson:"name"`
}

type movie struct {
	MovieID         primitive.ObjectID `json:"movieID" bson:"_id"`
	Title           string             `json:"title" bson:"title"`
	PicUrl          string             `json:"pic_url" bson:"pic_url"`
	Introduction    string             `json:"introduction" bson:"introduction"`
	Participants    []*participant     `json:"participants" bson:"participants"`
	ReleaseDate     string             `json:"release_date" bson:"release_date"`
	Language        string             `json:"language" bson:"language"`
	UniqueRatingCnt uint64             `json:"unique_rating_cnt" bson:"unique_rating_cnt"`
	AverageRating   float64            `json:"average_rating" bson:"average_rating"`
}

type movieTMDB struct {
	tmdbID string
	movie  *movie
}

const (
	localProxyURL   = "http://127.0.0.1:41091"
	movieDetailApi  = "https://api.themoviedb.org/3/movie/%s?api_key=b05a4ee65e0bb190f80f0e2c56ffbc48&language=zh-CN"
	movieCastApi    = "https://api.themoviedb.org/3/movie/%s/credits?api_key=b05a4ee65e0bb190f80f0e2c56ffbc48"
	movieImgBaseURL = "https://www.themoviedb.org/t/p/w300_and_h450_bestv2"

	coroutineCnt = 10
	saveInterval = 100
)

var httpClients []*http.Client
var tr *http.Transport

var saveLock sync.Mutex

func initTransport() {
	proxy, _ := url.Parse(localProxyURL)
	tr = &http.Transport{
		Proxy:             http.ProxyURL(proxy),
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		DisableKeepAlives: true,
		IdleConnTimeout:   -1,
	}
}

func DoMovieModels(linkFile, ratingFile, tempSaveFile, missingIDFile string) error {
	initTransport()
	rows, err := getMovieLinkRows(linkFile)
	fmt.Printf("total movies:%d\n", len(rows))
	if err != nil {
		return err
	}
	movieID2UniqueCnt, movieID2TotalRating, err := getRatingStatus(ratingFile)
	if err != nil {
		return err
	}
	fetchedMovieSet, fetchedMovies, err := alreadyFetchMovies(tempSaveFile)
	if err != nil {
		return err
	}

	httpClients = make([]*http.Client, coroutineCnt)
	tmdbMovieChan, doneChan, errChan := make(chan *movieTMDB, len(rows)), make(chan struct{}), make(chan error, len(rows))
	movieChan := make(chan *movie)
	nFetched := int64(0)
	for i := 0; i < coroutineCnt; i++ {
		go func(routineID int) {
			defer func() {
				doneChan <- struct{}{}
			}()
			for {
				tmdbMovie, ok := <-tmdbMovieChan
				if !ok {
					break
				}
				fillProperties(tmdbMovie.tmdbID, tmdbMovie.movie, errChan, movieChan, routineID)
				if newVal := atomic.AddInt64(&nFetched, 1); newVal%saveInterval == 0 {
					if err := saveMovies(fetchedMovies, tempSaveFile); err != nil {
						panic(err)
					}
					log.Printf("fetched %d items", newVal)
				}
			}
		}(i)
	}

	nSkipped := 0
	missingMovieIDs := make([]primitive.ObjectID, 0)
	for _, row := range rows {
		movieID, tmdbID := row[0], row[2]
		movieObjectID, err := objectIDFromHexString(movieID)
		if err != nil {
			return err
		}

		if strings.TrimSpace(tmdbID) == "" {
			missingMovieIDs = append(missingMovieIDs, movieObjectID)
			continue
		}
		if _, ok := fetchedMovieSet[movieObjectID]; ok {
			nSkipped++
			continue
		}

		movie := &movie{
			MovieID:         movieObjectID,
			UniqueRatingCnt: movieID2UniqueCnt[movieID],
			AverageRating:   safeDivide(movieID2TotalRating[movieID], float64(movieID2UniqueCnt[movieID])),
		}
		tmdbMovie := &movieTMDB{
			tmdbID: tmdbID,
			movie:  movie,
		}
		tmdbMovieChan <- tmdbMovie
	}
	log.Printf("total already fetched:%d\n", nSkipped)

	close(tmdbMovieChan)
	go func() {
		// wait all coroutines to finish and close movie channel
		for i := 0; i < coroutineCnt; i++ {
			<-doneChan
			log.Println("one coroutine finished")
		}
		close(movieChan)
	}()
	// collect movie results from sub-routines
	for movie := range movieChan {
		fetchedMovies = append(fetchedMovies, movie)
	}

	if err := buildErr(errChan); err != nil {
		return err
	}
	if err := saveMovies(fetchedMovies, tempSaveFile); err != nil {
		return err
	}
	if err := saveMissingMovieID(missingIDFile, missingMovieIDs); err != nil {
		return err
	}
	if err := batchSendToMongo(fetchedMovies); err != nil {
		return err
	}

	log.Println("Finished")
	return nil
}

// 这里的missingID保存后需要自己手动清除
func saveMissingMovieID(missingMovieFile string, movieIDs []primitive.ObjectID) error {
	file, err := os.OpenFile(missingMovieFile, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}

	b, err := json.Marshal(movieIDs)
	if err != nil {
		return err
	}

	nWrite, err := file.Write(b)
	if err != nil {
		return err
	}
	if nWrite != len(b) {
		return errors.New("mem byte slice length not equal to write length")
	}

	return nil
}

func buildErr(errChan chan error) error {
	close(errChan)
	errBuilder := strings.Builder{}
	for {
		err, ok := <-errChan
		if !ok {
			break
		}
		errBuilder.WriteString(err.Error())
	}
	if errBuilder.Len() > 0 {
		return errors.New(errBuilder.String())
	}

	return nil
}

func alreadyFetchMovies(movieFile string) (map[primitive.ObjectID]struct{}, []*movie, error) {
	movieIDSet := make(map[primitive.ObjectID]struct{})
	movies, err := recoverFromJson(movieFile)
	if err != nil {
		return nil, nil, err
	}

	for _, movie := range movies {
		if _, ok := movieIDSet[movie.MovieID]; ok {
			return nil, nil, errors.New("duplicate movieID")
		}

		movieIDSet[movie.MovieID] = struct{}{}
	}

	return movieIDSet, movies, nil
}

func recoverFromJson(movieFile string) ([]*movie, error) {
	file, err := os.OpenFile(movieFile, os.O_CREATE|os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var movies []*movie
	if err := json.Unmarshal(bytes, &movies); err != nil {
		return nil, err
	}

	return movies, nil
}

func batchSendToMongo(movies []*movie) error {
	docs := make([]interface{}, 0)
	for _, movie := range movies {
		docs = append(docs, movie)
	}
	if _, err := GetClient().Collection(CollectionMovie).InsertMany(context.Background(), docs); err != nil {
		return err
	}
	return nil
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
	saveLock.Lock()
	defer saveLock.Unlock()
	b, err := json.Marshal(movies)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(tempSaveFile, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	nWrite, err := file.Write(b)
	if err != nil {
		return err
	}
	if nWrite != len(b) {
		return errors.New("write length not equal to mem byte slice length")
	}

	return nil
}

func fillProperties(id string, movie *movie, errChan chan<- error, movieChan chan<- *movie, routineID int) {
	suc := true
	if err := getMovieDetail(id, movie, routineID); err != nil {
		suc = false
		errChan <- errorWithID(err, id)
	}
	if err := getMovieCast(id, movie, routineID); err != nil {
		suc = false
		errChan <- errorWithID(err, id)
	}
	if suc {
		movieChan <- movie
	}
}

func errorWithID(err error, id string) error {
	return fmt.Errorf("%s:%w\n", id, err)
}

func getProxyClient(routineID int) *http.Client {
	if httpClients[routineID] == nil {
		httpClients[routineID] = &http.Client{
			Transport: tr,
			Timeout:   time.Minute,
		}
	}

	return httpClients[routineID]
}

func request(url string, routineID int) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	req.Close = true
	if err != nil {
		return nil, err
	}
	resp, err := getProxyClient(routineID).Do(req)
	if err != nil {
		return nil, err
	}
	bodyReader := resp.Body
	b, err := ioutil.ReadAll(bodyReader)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	return b, nil
}

func getMovieDetail(id string, movie *movie, routineID int) error {
	bytes, err := request(fmt.Sprintf(movieDetailApi, id), routineID)
	if err != nil {
		return err
	}
	movieResp := movieResponse{}
	if err = json.Unmarshal(bytes, &movieResp); err != nil {
		return err
	}

	movie.Introduction = movieResp.Overview
	movie.Title = movieResp.OriginalTitle
	movie.Language = movieResp.OriginalLanguage
	movie.ReleaseDate = movieResp.ReleaseDate
	movie.PicUrl = fmt.Sprintf("%s%s", movieImgBaseURL, movieResp.PosterPath)

	return nil
}

func getMovieCast(id string, movie *movie, routineID int) error {
	bytes, err := request(fmt.Sprintf(movieCastApi, id), routineID)
	if err != nil {
		return err
	}
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

func safeDivide(a, b float64) float64 {
	if b == 0 {
		return 0
	}

	return fixedFloat64(a/b, 2)
}

func fixedFloat64(v float64, bits int) float64 {
	formatStr := fmt.Sprintf("%%.%df", bits)
	s := fmt.Sprintf(formatStr, v)
	nv, _ := strconv.ParseFloat(s, 64)

	return nv
}
