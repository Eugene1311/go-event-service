package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go-event-service/model"
	"log"
)

type ElasticEventRepository struct {
	elasticSearchClient *elasticsearch.Client
	eventsIndex         string
}

func NewElasticEventRepository(elasticSearchClient *elasticsearch.Client, eventsIndex string) ElasticEventRepository {
	return ElasticEventRepository{
		elasticSearchClient: elasticSearchClient,
		eventsIndex:         eventsIndex,
	}
}

func (repository ElasticEventRepository) Save(event model.Event) (*string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	request := esapi.IndexRequest{
		Index:   repository.eventsIndex,
		Body:    bytes.NewReader(data),
		Refresh: "true",
	}
	res, err := request.Do(context.Background(), repository.elasticSearchClient)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document", res.Status())
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
			return nil, err
		} else {
			log.Printf("Saved Event with document id %s, version=%d", r["_id"], int(r["_version"].(float64)))
			documentId := r["_id"].(string)
			return &documentId, nil
		}
	}
	return nil, nil
}
