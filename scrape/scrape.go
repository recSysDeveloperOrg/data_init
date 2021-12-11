package scrape

import (
	"encoding/csv"
	"fmt"
	"github.com/gocolly/colly"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	baseURL    = "https://www.themoviedb.org/movie/%s"
	imgBaseURL = "https://www.themoviedb.org"
)

type Movie struct {
	Title           string
	PicURL          string
	Introduction    string
	Participants    []string
	ReleaseDate     string
	Language        string
	UniqueRatingCnt uint64
	AverageRating   float64
}

func DoScrape(linkFile, ratingFile string) error {
	file, err := os.Open(linkFile)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(file)
	if _, err = csvReader.Read(); err != nil {
		return err
	}

	rows := make([][]string, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}

	// 从本地获取uniqueRatingCnt和AverageRating信息
	movieID2RatingCnt, movieID2TotalRating := make(map[string]uint64), make(map[string]float64)
	file, err = os.Open(ratingFile)
	if err != nil {
		return err
	}
	csvReader = csv.NewReader(file)
	if _, err = csvReader.Read(); err != nil {
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

		movieID := row[1]
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return err
		}
		movieID2RatingCnt[movieID]++
		movieID2TotalRating[movieID] += rating
	}

	log.Println("start scraping...")
	movies := make([]*Movie, 0)
	for _, row := range rows {
		movie := &Movie{
			UniqueRatingCnt: movieID2RatingCnt[row[0]],
			AverageRating:   movieID2TotalRating[row[0]] / float64(movieID2RatingCnt[row[0]]),
		}
		if err = doScrape(row[2], movie); err != nil {
			return err
		}
		movies = append(movies, movie)
	}

	return nil
}

func doScrape(tmdbID string, movie *Movie) error {
	c := colly.NewCollector()
	if err := c.Limit(&colly.LimitRule{
		DomainRegexp: ".*",
		Parallelism:  4,
		Delay:        1 * time.Second,
	}); err != nil {
		panic(err)
	}
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Cookie", "tmdb.prefs=%7B%22adult%22%3Afalse%2C%22i18n_fallback_language%22%3A%22en-US%22%2C%22locale%22%3A%22zh-CN%22%2C%22country_code%22%3A%22CN%22%2C%22timezone%22%3A%22Asia%2FShanghai%22%7D; tmdb._cookie_policy=false; _ga=GA1.2.1465198586.1639295550; _gid=GA1.2.818457904.1639295550; _dc_gtm_UA-2087971-10=1; _gali=original_header; tmdb.session=AVc5lJR-uDc3vdm1SmP6vlKeWkPYmu0fAqc-njBwcAG1bgDla1_j8-0baP2plZ-T12pMUrIvSjQ4rbAemw2sSyLfvERYKoxDT9rQegqQ2m0AqIUukDSSLAr1TaEPXr-kKauC6M-ABB6cjTK53LQbKSN8g4uDV--L3iGPcBmwrAumCL2HccwZTegAh5m_ZM1_8PqwbE1o6zs50Ld0v32mQa92hI0V27a7bmcpiRB6VprB4663sIco0f_VhV0vrwEhK7OtKjEiS9IiZUH8JqbfyFA%3D")
	})

	// 爬img
	c.OnHTML(".image_content", func(e *colly.HTMLElement) {
		for _, attr := range e.DOM.Children().Get(0).Attr {
			if attr.Key == "src" {
				movie.PicURL = fmt.Sprintf("%s%s\n", imgBaseURL, removeBlurMask(attr.Val))
				break
			}
		}
	})

	// 爬introduction
	c.OnHTML(".overview", func(e *colly.HTMLElement) {
		movie.Introduction = strings.TrimSpace(e.Text)
	})

	// 爬演员信息
	c.OnHTML("#cast_scroller", func(e *colly.HTMLElement) {
		cards := e.DOM.Children().Children()
		for i := 0; i < cards.Length()-1; i++ {
			card := cards.Get(i)
			movie.Participants = append(movie.Participants, card.FirstChild.
				NextSibling.NextSibling.NextSibling.
				FirstChild.FirstChild.Data)
		}
	})

	// 爬发行日期
	c.OnHTML(".release", func(e *colly.HTMLElement) {
		movie.ReleaseDate = strings.TrimSpace(e.Text)
	})

	// 爬语言信息
	c.OnHTML(".facts", func(e *colly.HTMLElement) {
		e.ForEach("p", func(i int, element *colly.HTMLElement) {
			if strings.Contains(element.Text, "原始语言") {
				movie.Language = strings.Split(element.Text, " ")[1]
			}
		})
	})

	if err := c.Visit(fmt.Sprintf(baseURL, tmdbID)); err != nil {
		return err
	}
	return nil
}

func removeBlurMask(url string) string {
	return strings.ReplaceAll(url, "_filter(blur)", "")
}
