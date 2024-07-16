package storage

import (
	"context"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"

	"github.com/jackc/pgx/v5"
)

type PgDB struct {
	ctx    context.Context
	dbConn *pgx.Conn
}

/*
	TODO: refactor project structure
*/

func InitDB(ctx context.Context, psqlInfo string) (*PgDB, error) {
	conn, err := pgx.Connect(ctx, psqlInfo)
	if err != nil {
		return nil, err
	}
	return &PgDB{ctx, conn}, nil
}

func (p *PgDB) Close() {
	p.dbConn.Close(p.ctx)
}

func (p *PgDB) InsertMessages(msgs []models.Notification) ([]int, error) {
	insertSQL := "INSERT INTO notifications (user_id, message) VALUES($1,$2) RETURNING id"
	tx, err := p.dbConn.Begin(p.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(p.ctx)

	ids := make([]int, 0, len(msgs))
	for _, msg := range msgs {
		var id int
		if err := p.dbConn.QueryRow(p.ctx, insertSQL, msg.To, msg.Message).Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	if err = tx.Commit(p.ctx); err != nil {
		return nil, err
	}
	//TODO: return id
	return ids, nil
}

func (p *PgDB) GetMessage(id int) (*models.Notification, error) {
	getSQL := "SELECT user_id,message from notifications WHERE id = $1 LIMIT 1"
	note := &models.Notification{}
	if err := p.dbConn.QueryRow(p.ctx, getSQL, id).Scan(&note.To, &note.Message); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("notification %d: unknown notification", id)
		}
		return nil, fmt.Errorf("DB error GetMessage by id %d: %v", id, err)
	}
	return note, nil
}

func (p *PgDB) GetAllMessages() ([]models.DBNotification, error) {
	rows, err := p.dbConn.Query(p.ctx, "SELECT * FROM notifications LIMIT 1000")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// An album slice to hold data from returned rows.
	msgs := make([]models.DBNotification, 0, 10)

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var msg models.DBNotification
		if err := rows.Scan(&msg.ID, &msg.To, &msg.Message); err != nil {
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
	if err = rows.Err(); err != nil {
		return msgs, err
	}
	return msgs, nil
}
