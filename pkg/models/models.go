package models

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Notification struct {
	To      int    `json:"to"`
	Message string `json:"message"`
}
