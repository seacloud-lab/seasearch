package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/wal"
	"github.com/zincsearch/zincsearch/pkg/config"
	"sync"
)

var (
	mysql      *sql.DB
	walMap     map[string]*Wal
	readyMap   map[string]chan struct{}
	readyMutex sync.Mutex
)

func Init() {
	var err error
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		config.Global.WalConfig.User, config.Global.WalConfig.Password, config.Global.WalConfig.Host,
		config.Global.WalConfig.Port, config.Global.WalConfig.Db)
	mysql, err = sql.Open("mysql", connStr)

	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to mysql")
	}
	if err := mysql.Ping(); err != nil {
		log.Fatal().Err(err).Msg("cannot ping to mysql")
	}

	walMap = make(map[string]*Wal)
	readyMap = make(map[string]chan struct{})
}

func ShutDown() {
	err := mysql.Close()
	if err != nil {
		log.Error().Err(err).Msg("close mysql connection error")
	}
}

type Wal struct {
	name      string
	nextIndex uint64
	lock      sync.Mutex
}

func OpenWal(name string) (*Wal, error) {
	for {
		readyMutex.Lock()
		if w, ok := walMap[name]; ok {
			readyMutex.Unlock()
			return w, nil
		}
		if ch, ok := readyMap[name]; ok {
			readyMutex.Unlock()
			<-ch
			continue
		}
		ch := make(chan struct{})
		readyMap[name] = ch
		readyMutex.Unlock()

		w := &Wal{name: name}
		lastIndex, err := w.LastIndex()
		if err != nil {
			readyMutex.Lock()
			close(ch)
			delete(readyMap, name)
			readyMutex.Unlock()
			return nil, err
		}
		w.nextIndex = lastIndex + 1

		readyMutex.Lock()
		close(ch)
		delete(readyMap, name)
		walMap[name] = w
		readyMutex.Unlock()

		return w, nil
	}
}

func (m *Wal) Len() (uint64, error) {
	sqlStr := "SELECT COUNT(*) FROM `wal` WHERE  `name`=?;"
	row := mysql.QueryRow(sqlStr, m.name)

	var count uint64
	err := row.Scan(&count)
	return count, err
}

func (m *Wal) FirstIndex() (uint64, error) {
	sqlStr := "SELECT `index` FROM `wal` WHERE `name`=? ORDER BY `index` limit 1;"
	row := mysql.QueryRow(sqlStr, m.name)

	var id uint64
	err := row.Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// if no rows, we return zero
			return 0, nil
		}
	}
	return id, err
}

func (m *Wal) LastIndex() (uint64, error) {
	sqlStr := "SELECT `index` FROM `wal` WHERE `name`=? ORDER BY `index` DESC  limit 1;"
	row := mysql.QueryRow(sqlStr, m.name)

	var id uint64
	err := row.Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// if no rows, we return zero
			return 0, nil
		}
	}
	return id, err
}

func (m *Wal) Write(index uint64, data []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if index == 0 {
		index = m.nextIndex
	} else {
		if index != m.nextIndex {
			return wal.ErrOutOfOrder
		}
	}
	_, err := mysql.Exec("INSERT INTO `wal`(`index`,`data`,`name`) values(?,?,?);", index, data, m.name)
	if err == nil {
		m.nextIndex++
	}
	return err
}

func (m *Wal) Read(id uint64) ([]byte, error) {
	sqlStr := "SELECT `data` FROM `wal` WHERE `name`=? AND `index`= ? ;"
	row := mysql.QueryRow(sqlStr, m.name, id)

	var res []byte
	err := row.Scan(&res)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, wal.ErrNotFound
		}
	}
	return res, err
}

func (m *Wal) TruncateFront(id uint64) error {
	sqlStr := "DELETE FROM `wal`  WHERE `name`=? AND `index` < ?;"

	_, err := mysql.Exec(sqlStr, m.name, id)
	return err
}

func (m *Wal) Close() error {
	return nil
}

func (m *Wal) Sync() error {
	return nil
}
