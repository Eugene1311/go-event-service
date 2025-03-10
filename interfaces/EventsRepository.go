package interfaces

import "go-event-service/model"

type EventsRepository interface {
	Save(event model.Event) (*string, error)
}
