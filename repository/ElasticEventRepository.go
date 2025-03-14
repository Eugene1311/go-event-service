package repository

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
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
	res, err := repository.elasticSearchClient.Index(repository.eventsIndex, bytes.NewReader(data))

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		errorMessage := fmt.Sprintf("[%s] Error indexing document", res.Status())
		return nil, errors.New(errorMessage)
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
}
