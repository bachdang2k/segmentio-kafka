package main

import (
	"encoding/json"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main() {

	posts := []Post{
		{
			ID:     1,
			AppId:  1,
			Action: 1,
		},
		{
			ID:     12,
			AppId:  1,
			Action: 1,
		},
		{
			ID:     13,
			AppId:  1,
			Action: 1,
		},
		{
			ID:     14,
			AppId:  2,
			Action: 2,
		},
		{
			ID:     15,
			AppId:  2,
			Action: 2,
		},
		{
			ID:     16,
			AppId:  2,
			Action: 1,
		},
		{
			ID:     17,
			AppId:  1,
			Action: 1,
		},
		{
			ID:     18,
			AppId:  1,
			Action: 1,
		},
		{
			ID:     19,
			AppId:  2,
			Action: 1,
		},
		{
			ID:     21,
			AppId:  2,
			Action: 2,
		},
	}

	// productTopic := "producer-product-table-testing"

	postHndTopic := "post_hnd"
	postTnvnTopic := "post_tnvn"

	conn, err := kafka.Dial("tcp", net.JoinHostPort("10.84.86.42", "9092"))

	if err != nil {
		panic(err.Error())
	}

	postHndTopicConfig := kafka.TopicConfig{Topic: postHndTopic, NumPartitions: 1, ReplicationFactor: 1}
	postTnvnTopicConfig := kafka.TopicConfig{Topic: postTnvnTopic, NumPartitions: 1, ReplicationFactor: 1}

	err = conn.CreateTopics(postHndTopicConfig, postTnvnTopicConfig)

	if err != nil {
		panic(err.Error())
	}

	producer := NewProducer()

	for _, post := range posts {

		if post.AppId == 1 {
			marsheledPost, _ := json.Marshal(post)
			Produce([]byte(strconv.Itoa(post.ID)), marsheledPost, postHndTopic, producer)
		}

		if post.AppId == 2 {
			marsheledPost, _ := json.Marshal(post)
			Produce([]byte(strconv.Itoa(post.ID)), marsheledPost, postTnvnTopic, producer)
		}
	}


}
