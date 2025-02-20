package model

import "github.com/google/uuid"

type Event struct {
	Id      uuid.UUID `json:"id"`
	Payload Payload   `json:"payload"`
}

type Payload struct {
	Data string `json:"data"`
}
