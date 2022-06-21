package minidb_project

import (
	"io"
	"os"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*MiniDB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: dirPath,
	}

	// 加载索引
	db.loadIndexesFromFile()
	return db, nil
}

// 从文件当中加载索引
func (db *MiniDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}

	return
}

func (db *MiniDB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()

		if len(validEntries) > 0 {
			// 新建临时文件
			mergeDBFile, err := NewMergeDBFile(db.dirPath)
			if err != nil {
				return err
			}
			defer os.Remove(mergeDBFile.File.Name())

			// 重新写入有效的 entry
			for _, entry := range validEntries {
				writeOff := mergeDBFile.Offset
				err := mergeDBFile.Write(entry)
				if err != nil {
					return err
				}

				// 更新索引
				db.indexes[string(entry.Key)] = writeOff
			}

			// 获取文件名
			dbFileName := db.dbFile.File.Name()
			// 关闭文件
			db.dbFile.File.Close()
			// 删除旧的数据文件
			os.Remove(dbFileName)

			// 获取文件名
			mergeDBFileName := mergeDBFile.File.Name()
			// 关闭文件
			mergeDBFile.File.Close()
			// 临时文件变更为新的数据文件
			os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+FileName)

			db.dbFile = mergeDBFile
		}
	}

	return nil
}
