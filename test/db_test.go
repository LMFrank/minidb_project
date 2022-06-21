package test

import (
	"github.com/LMFrank/minidb_project"
	"testing"
)

func TestOpen(t *testing.T) {
	db, err := minidb_project.Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestMiniDB_Merge(t *testing.T) {
	db, err := minidb_project.Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}
