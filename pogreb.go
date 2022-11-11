package main

import (
	"errors"

	"github.com/akrylysov/pogreb"
)

type PogrebDB struct {
	DB     *pogreb.DB
	Getnum uint64
	Setnum uint64
	Delnum uint64
	Bsync  bool
}

func NewPogrebDB() DBInterface {
	return &PogrebDB{}
}

func (db *PogrebDB) Open(path string, sync bool) error {

	opts := &pogreb.Options{}
	if sync {
		opts.BackgroundSyncInterval = -1
	}
	database, err := pogreb.Open(path, opts)
	if err != nil {
		return err
	}
	db.DB = database
	db.Bsync = sync
	return nil
}

func (db *PogrebDB) Close() error {
	return db.DB.Close()
}

func (db *PogrebDB) Get(key []byte) ([]byte, error) {
	db.Getnum++
	v, err := db.DB.Get(key)
	if v == nil {
		return nil, errors.New("keynotfound")
	}

	return v, err
}

func (db *PogrebDB) Set(key, val []byte) error {
	db.Setnum++
	return db.DB.Put(key, val)
}

func (db *PogrebDB) Del(key []byte) error {
	db.Delnum++
	return db.DB.Delete(key)
}

func (db *PogrebDB) GetAll() (int, error) {
	var cout int
	for k, _, err := db.DB.Items().Next(); err != nil; k, _, err = db.DB.Items().Next() {
		if k != nil {
			cout++
		}
	}
	return cout, nil
}
