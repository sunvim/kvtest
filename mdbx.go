package main

import (
	"log"

	"github.com/torquem-ch/mdbx-go/mdbx"
)

type MDBX struct {
	DB      *mdbx.Env
	dbi     mdbx.DBI
	dbiName string
	Getnum  uint64
	Setnum  uint64
	Delnum  uint64
	Bsync   bool
}

func NewMDBX() *MDBX {
	return &MDBX{}
}

func (m *MDBX) Open(path string, sync bool) error {
	env, err := mdbx.NewEnv()
	if err != nil {
		return err
	}
	env.SetGeometry(1<<30, 1<<30, 1<<34, 1<<30, -1, 1<<16)
	env.SetOption(mdbx.OptMaxDB, 100)
	if err = env.Open(path, mdbx.Create, 0664); err != nil {
		log.Println(err)
		return err
	}

	m.DB = env
	m.dbiName = "default"
	env.Update(func(txn *mdbx.Txn) error {
		var err error
		m.dbi, err = txn.OpenDBI(m.dbiName, mdbx.Create, nil, nil)
		if err != nil {
			return err
		}
		return nil
	})

	return nil
}

func (m *MDBX) Set(key []byte, val []byte) error {
	return m.DB.Update(func(txn *mdbx.Txn) error {
		return txn.Put(m.dbi, key, val, mdbx.Upsert)
	})
}

func (m *MDBX) Get(key []byte) ([]byte, error) {
	var (
		rs  []byte
		err error
	)
	m.DB.View(func(txn *mdbx.Txn) error {
		rs, err = txn.Get(m.dbi, key)
		return nil
	})
	return rs, err
}

func (m *MDBX) Del(key []byte) error {
	return m.DB.Update(func(txn *mdbx.Txn) error {
		return txn.Del(m.dbi, key, nil)
	})
}

func (m *MDBX) GetAll() (int, error) {
	var (
		cnt int
		err error
	)
	err = m.DB.View(func(txn *mdbx.Txn) error {
		c, err := txn.OpenCursor(m.dbi)
		if err != nil {
			return err
		}
		for k, _, err := c.Get(nil, nil, mdbx.First); k != nil && err != nil; k, _, err = c.Get(nil, nil, mdbx.Next) {
			cnt++
		}
		return nil
	})
	return cnt, err
}

func (m *MDBX) Close() error {
	m.DB.CloseDBI(m.dbi)
	m.DB.Close()
	return nil
}
