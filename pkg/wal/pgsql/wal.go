package pgsql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/zincsearch/wal"
	"github.com/zincsearch/zincsearch/pkg/config"
	"sync"
)

var (
	pgsql      *sql.DB
	walMap     map[string]*Wal
	readyMap   map[string]chan struct{}
	readyMutex sync.Mutex
)

func Init() {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", config.Global.WalConfig.User, config.Global.WalConfig.Password,
		config.Global.WalConfig.Host, config.Global.WalConfig.Port, config.Global.WalConfig.Db)
	var err error
	pgsql, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("cannot connect to postgresql: %s", err)
	}

	err = pgsql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	walMap = make(map[string]*Wal)
	readyMap = make(map[string]chan struct{})
}

func ShutDown() {
	err := pgsql.Close()
	if err != nil {
		log.Errorf("close pgsql connection error: %s", err)
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
			delete(readyMap, name)
			readyMutex.Unlock()
			return nil, err
		}
		w.nextIndex = lastIndex + 1

		readyMutex.Lock()
		delete(readyMap, name)
		walMap[name] = w
		readyMutex.Unlock()

		return w, nil
	}

}

func (p *Wal) Len() (uint64, error) {
	sqlStr := `SELECT COUNT(*) FROM  "wal" WHERE "name" =$1 ;`
	row := pgsql.QueryRow(sqlStr, p.name)

	var res uint64
	err := row.Scan(&res)
	return res, err
}

func (p *Wal) FirstIndex() (uint64, error) {
	sqlStr := `SELECT "index" FROM "wal" WHERE "name"=$1 ORDER BY "index" LIMIT 1;`
	row := pgsql.QueryRow(sqlStr, p.name)

	var res uint64
	err := row.Scan(&res)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return res, err
}

func (p *Wal) LastIndex() (uint64, error) {
	sqlStr := `SELECT "index" FROM "wal" WHERE "name"=$1 ORDER BY "index" DESC LIMIT 1;`
	row := pgsql.QueryRow(sqlStr, p.name)

	var res uint64
	err := row.Scan(&res)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return res, err
}

func (p *Wal) Write(index uint64, data []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if index == 0 {
		index = p.nextIndex
	} else {
		if index != p.nextIndex {
			return wal.ErrOutOfOrder
		}
	}
	_, err := pgsql.Exec(`INSERT INTO "wal"("name","index","data") values($1,$2,$3);`, p.name, index, data)
	if err == nil {
		p.nextIndex++
	}

	return err
}

func (p *Wal) Read(id uint64) ([]byte, error) {
	sqlStr := `SELECT "data" FROM "wal" WHERE "name"=$1 AND "index"= $2;`
	row := pgsql.QueryRow(sqlStr, p.name, id)

	var res []byte
	err := row.Scan(&res)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, wal.ErrNotFound
		}
	}
	return res, err
}

func (p *Wal) TruncateFront(id uint64) error {
	sqlStr := `DELETE FROM "wal" WHERE "name"=$1 AND "index" < $2;`

	_, err := pgsql.Exec(sqlStr, p.name, id)
	return err
}

func (p *Wal) Sync() error {
	return nil
}

func (p *Wal) Close() error {
	return nil
}
