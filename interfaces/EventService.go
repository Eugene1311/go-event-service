package interfaces

import "go-event-service/model"

type EventService interface {
	Save(event model.Event) (model.Event, error)
}
