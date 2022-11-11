package main

import (
	"errors"

	"github.com/sunvim/coqdb"
)

type CoqDB struct {
	DB     *coqdb.DB
	Getnum uint64
	Setnum uint64
	Delnum uint64
	Bsync  bool
}

func NewCoqDB() DBInterface {
	return &CoqDB{}
}

func (db *CoqDB) Open(path string, sync bool) error {

	opts := &coqdb.Options{}
	if sync {
		opts.BackgroundSyncInterval = -1
	}
	database, err := coqdb.Open(path, opts)
	if err != nil {
		return err
	}
	db.DB = database
	db.Bsync = sync
	return nil
}

func (db *CoqDB) Close() error {
	return db.DB.Close()
}

func (db *CoqDB) Get(key []byte) ([]byte, error) {
	db.Getnum++
	v, err := db.DB.Get(key, nil)
	if errors.Is(err, coqdb.ErrNotFound) {
		err = errors.New("keyNotFound")
	}

	return v, err
}

func (db *CoqDB) Set(key, val []byte) error {
	db.Setnum++
	return db.DB.Put(key, val, nil)
}

func (db *CoqDB) Del(key []byte) error {
	db.Delnum++
	return db.DB.Delete(key, nil)
}

func (db *CoqDB) GetAll() (int, error) {
	var cout int
	for k, _, err := db.DB.Items().Next(); err != nil; k, _, err = db.DB.Items().Next() {
		if k != nil {
			cout++
		}
	}
	return cout, nil
}
