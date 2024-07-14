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
	insertSQL := "INSERT INTO notifications VALUES($1,$2) RETURNING id"
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
	getSQL := "SELECT * from notifications WHERE id = $1"
	note := &models.Notification{}
	if err := p.dbConn.QueryRow(p.ctx, getSQL, id).Scan(note); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("notification %d: unknown notification", id)
		}
		return nil, fmt.Errorf("canPurchase %d: %v", id, err)
	}
	return note, nil
}

// func main() {
// 	//TODO: move configs to common file
// 	connStr := "postgres://puser:ppassword@localhost:6432/notifyDB?sslmode=disable"
// 	_, err := InitDB(context.Background(), connStr)
// 	if err != nil {
// 		fmt.Println(err)
// 		return
// 	}
// 	fmt.Println("success")
// }
