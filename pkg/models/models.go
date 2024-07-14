package models

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Notification struct {
	To      int    `json:"to"  db:"user_id"`
	Message string `json:"message" db:"message"`
}
