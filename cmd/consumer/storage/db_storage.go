package storage

import (
	"context"
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

func (p *PgDB) InsertMessages(msgs []models.Notification) error {
	insertSQL := "INSERT INTO notifications VALUES($1,$2)"
	tx, err := p.dbConn.Begin(p.ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(p.ctx)
	for _, msg := range msgs {
		if _, err := p.dbConn.Exec(p.ctx, insertSQL, msg.To, msg.Message); err != nil {
			return err
		}
	}

	if err = tx.Commit(p.ctx); err != nil {
		return err
	}

	return nil
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
