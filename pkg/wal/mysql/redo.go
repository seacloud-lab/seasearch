package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zincsearch/wal"
	"github.com/zincsearch/zincsearch/pkg/errors"
)

type Redo struct {
	name string
}

func NewRedo(name string) *Redo {
	return &Redo{name: name}
}

func (m *Redo) Write(index uint64, data []byte) error {
	tx, err := mysql.Begin()
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		return err
	}
	row := tx.QueryRow("SELECT COUNT(1) FROM `redo` WHERE `name`=? AND `index`=?", m.name, index)

	var count uint64
	err = row.Scan(&count)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if count == 0 {
		_, err = tx.Exec("INSERT INTO `redo`(`index`,`data`,`name`) values(?,?,?);", index, data, m.name)
	} else {
		_, err = tx.Exec("UPDATE `redo` SET `data`= ? WHERE `name`=? AND `index`=?", data, m.name, index)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (m *Redo) Read(index uint64) ([]byte, error) {
	sqlStr := "SELECT `data` FROM `redo` WHERE `name`=? AND `index`=? "
	row := mysql.QueryRow(sqlStr, m.name, index)

	var res []byte
	err := row.Scan(&res)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, wal.ErrNotFound
		}
	}
	return res, err
}

func (m *Redo) Close() error {
	return nil
}
