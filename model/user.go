package model

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Gender string

const (
	MALE   Gender = "MALE"
	FEMALE Gender = "FEMALE"
	OTHER  Gender = "OTHER"
)

type User struct {
	UserID           string `bson:"user_id"`
	UserName         string `bson:"user_name"`
	Password         string `bson:"password"`
	Gender           Gender `bson:"gender"`
	LastRefreshToken string `bson:"last_refresh_token"`
}

var (
	UserNameGenPrefixCnt  = 0
	UserNameGenPostfixCnt = 0
	GenderGen             = 0
)

func DoUserModels(filename string) error {
	// 随机生成UserModel
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	csvReader := csv.NewReader(file)
	// skip first row
	if _, err := csvReader.Read(); err != nil {
		return err
	}

	maxUserID := int64(0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		userID, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return err
		}
		maxUserID = max(maxUserID, userID)
	}

	batches := make([]interface{}, maxUserID)
	for i := int64(1); i <= maxUserID; i++ {
		batches[i-1] = generate(fmt.Sprintf("%d", i))
	}

	if _, err := GetClient().Collection(CollectionUser).InsertMany(context.Background(), batches); err != nil {
		return err
	}

	return nil
}

func toChar(n int) int {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}

func generate(id string) *User {
	username := fmt.Sprintf("z%d%c", UserNameGenPrefixCnt, toChar(UserNameGenPostfixCnt))
	c := (UserNameGenPostfixCnt + 1) / 36
	UserNameGenPostfixCnt = (UserNameGenPostfixCnt + 1) % 36
	UserNameGenPrefixCnt += c
	gender := MALE
	if GenderGen%2 == 1 {
		gender = FEMALE
	}
	GenderGen++

	return &User{
		UserID:           id,
		UserName:         username,
		Password:         username,
		Gender:           gender,
		LastRefreshToken: "",
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
