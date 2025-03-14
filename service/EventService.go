package service

import (
	"go-event-service/interfaces"
	"go-event-service/model"
	"log"
)

type DomainEventService struct {
	eventRepository interfaces.EventsRepository
}

func NewDomainEventService(eventRepository interfaces.EventsRepository) DomainEventService {
	return DomainEventService{
		eventRepository: eventRepository,
	}
}

func (service DomainEventService) Save(event model.Event) (model.Event, error) {
	log.Printf("Saving event %+v", event)
	_, err := service.eventRepository.Save(event)
	if err != nil {
		log.Printf("Error while savind event %v, %s", event, err.Error())
		return model.Event{}, err
	}

	return event, nil
}
