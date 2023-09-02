package pgsql

import (
	"database/sql"
	"errors"
	"github.com/zincsearch/wal"
)

type Redo struct {
	name string
}

func Open(name string) *Redo {
	return &Redo{name: name}
}

func (p *Redo) Write(index uint64, data []byte) error {
	tx, err := pgsql.Begin()
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		return err
	}
	row := tx.QueryRow(`SELECT COUNT(1) FROM "redo" WHERE "name"=$1 AND "index"=$2;`, p.name, index)

	var count uint64
	err = row.Scan(&count)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if count == 0 {
		_, err = tx.Exec(`INSERT INTO "redo"("index","data","name") values($1,$2,$3);`, index, data, p.name)
	} else {
		_, err = tx.Exec(`UPDATE "redo" SET "data"= $1 WHERE "name"=$2 AND "index"=$3`, data, p.name, index)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (p *Redo) Read(index uint64) ([]byte, error) {
	sqlStr := `SELECT "data" FROM "redo" WHERE "name"=$1 AND "index"=$2 ;`
	row := pgsql.QueryRow(sqlStr, p.name, index)

	var res []byte
	err := row.Scan(&res)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, wal.ErrNotFound
		}
	}
	return res, err
}

func (p *Redo) Close() error {
	return nil
}
