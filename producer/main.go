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
			ID: 1,
			AppId: 1,
			Action: 1,
		}, 
		{
			ID: 12,
			AppId: 1,
			Action: 1,
		},
		{
			ID: 13,
			AppId: 1,
			Action: 1,
		},
		{
			ID: 14,
			AppId: 2,
			Action: 2,
		},
		{
			ID: 15,
			AppId: 2,
			Action: 2,
		},
		{
			ID: 16,
			AppId: 2,
			Action: 1,
		},
		{
			ID: 17,
			AppId: 1,
			Action: 1,
		},
		{
			ID: 18,
			AppId: 1,
			Action: 1,
		},
		{
			ID: 19,
			AppId: 2,
			Action: 1,
		},
		{
			ID: 21,
			AppId: 2,
			Action: 2,
		},
	}


	products := []Product{
		{
			ID:            0,
			Name:          "product1",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    1,
		},
		{
			ID:            2,
			Name:          "product2",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    0,
		},
		{
			ID:            3,
			Name:          "product3",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    3,
		},
		{
			ID:            4,
			Name:          "product4",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    0,
		},
		{
			ID:            5,
			Name:          "product5",
			Price:         23,
			OriginalPrice: 34,
			CategoryID:    2,
		},
	}
	categories := []Category{
		{
			ID:   0,
			Name: "Category1",
		},
		{
			ID:   2,
			Name: "Category2",
		},
		{
			ID:   3,
			Name: "Category3",
		},
	}
	images := []Image{
		{
			ID:        0,
			URL:       "http://google.com/image1.jpg",
			ProductID: 0,
		},
		{
			ID:        1,
			URL:       "http://google.com/image2.jpg",
			ProductID: 0,
		},
		{
			ID:        2,
			URL:       "http://google.com/image3.jpg",
			ProductID: 1,
		},
		{
			ID:        3,
			URL:       "http://google.com/image4.jpg",
			ProductID: 2,
		},
		{
			ID:        4,
			URL:       "http://google.com/image5.jpg",
			ProductID: 3,
		},
		{
			ID:        5,
			URL:       "http://google.com/image6.jpg",
			ProductID: 3,
		},
	}

	productTopic := "producer-product-table-testing"
	categoryTopic := "producer-category-table-testing"
	imageTopic := "producer-image-table-testing"


	postHndTopic := "post_hnd"
	postTnvnTopic := "post_tnvn"

	conn, err := kafka.Dial("tcp", net.JoinHostPort("10.84.86.42", "9092"))

	if err != nil {
		panic(err.Error())
	}

	productTopicConfig := kafka.TopicConfig{Topic: productTopic, NumPartitions: 1, ReplicationFactor: 1}
	categoryTopicConfig := kafka.TopicConfig{Topic: categoryTopic, NumPartitions: 1, ReplicationFactor: 1}
	imageTopicConfig := kafka.TopicConfig{Topic: imageTopic, NumPartitions: 1, ReplicationFactor: 1}

	err = conn.CreateTopics(productTopicConfig, categoryTopicConfig, imageTopicConfig)

	if err != nil {
		panic(err.Error())
	}

	producer := NewProducer()

	for _, product := range products {
		marsheledProduct, _ := json.Marshal(product)
		Produce([]byte(strconv.Itoa(product.ID)), marsheledProduct, productTopic, producer)
	}

	for _, category := range categories {
		marsheledProduct, _ := json.Marshal(category)
		Produce([]byte(strconv.Itoa(category.ID)), marsheledProduct, categoryTopic, producer)
	}

	for _, image := range images {
		marsheledProduct, _ := json.Marshal(image)
		Produce([]byte(strconv.Itoa(image.ID)), marsheledProduct, imageTopic, producer)
	}

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