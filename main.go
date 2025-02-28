package main

import (
	"database/sql"
	"expvar"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	_ "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func runPPROF() {
	http.ListenAndServe("localhost:6060", nil)
}
func init() {
	go runPPROF()
}

func run() {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			err := createAndTestDb(10000, 10)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func main() {
	//preallocateCache()
	run()
}

func createAndTestDb(insertsN int, parallelSelects int) error {
	dir, err := os.MkdirTemp("", "test-*")
	if err != nil {
		return err
	}

	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "db")

	db, err := sql.Open("sqlite", fn)
	if err != nil {
		return err
	}

	if _, err = db.Exec(`
drop table if exists t;
create table t(i int, str text);
`); err != nil {
		return err
	}

	if err = inserts(db, insertsN, 100, 10, 1000); err != nil {
		return err
	}
	fmt.Println("inserts done")
	expvar.Do(func(kv expvar.KeyValue) {
		if strings.HasPrefix(kv.Key, "allocator") {
			fmt.Println(kv.Value.String())
		}
	})
	var roDbs []*sql.DB
	wg := sync.WaitGroup{}
	for i := 0; i < parallelSelects; i++ {
		wg.Add(1)
		roDb, err := sql.Open("sqlite", fn+"?mode=ro")
		if err != nil {
			return err
		}
		roDbs = append(roDbs, roDb)
		go func() {
			defer wg.Done()
			if err = selects(roDb, insertsN); err != nil {
				panic(err)
			}
			fmt.Println("selects done")
		}()
	}
	wg.Wait()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	for _, roDb := range roDbs {
		roDb.Close()
	}
	db.Close()

	return nil
}

// create a lot of inserts
func inserts(db *sql.DB, n, commitEvery, minStringSize, maxStringSize int) error {
	for i := 0; i < n; {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare("insert into t values(?, ?)")
		if err != nil {
			tx.Rollback()
			return err
		}
		// Insert up to commitEvery rows or until n is reached.
		for j := 0; j < commitEvery && i < n; j++ {
			if _, err = stmt.Exec(i, randomString(rand.Intn(maxStringSize-minStringSize)+minStringSize)); err != nil {
				stmt.Close()
				tx.Rollback()
				return err
			}
			i++
		}
		stmt.Close()
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// do a lot of selects
func selects(db *sql.DB, maxValue int) error {
	rows, err := db.Query("select * from t WHERE i < ?", maxValue)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var i int
		var s string
		if err = rows.Scan(&i, &s); err != nil {
			return err
		}
	}
	return nil
}

func randomString(l int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, l)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func preallocateCache() {
	var pageCacheSize int32 = 1024 * 1024
	tls := libc.NewTLS()
	if sqlite3.Xsqlite3_threadsafe(tls) == 0 {
		panic(fmt.Errorf("sqlite: thread safety configuration error"))
	}

	varArgs := libc.Xmalloc(tls, types.Size_t(unsafe.Sizeof(uintptr(0))))
	if varArgs == 0 {
		panic(fmt.Errorf("cannot allocate memory"))
	}

	p := libc.Xmalloc(tls, types.Size_t(pageCacheSize))
	if p == 0 {
		panic(fmt.Errorf("cannot allocate memory"))
	}

	var headerSize int32 // This will receive the header size from SQLite

	// Create a va_list containing the pointer to headerSize.
	// Unlike SQLITE_CONFIG_SMALL_MALLOC (which takes an int value),
	// SQLITE_CONFIG_PCACHE_HDRSZ expects a pointer to an int.
	varArgs2 := libc.NewVaList(uintptr(unsafe.Pointer(&headerSize)))
	if varArgs2 == 0 {
		panic(fmt.Errorf("sqlite: get page cache header size: cannot allocate memory"))
	}
	defer libc.Xfree(tls, varArgs2)

	// Call sqlite3_config with SQLITE_CONFIG_PCACHE_HDRSZ.
	rc := sqlite3.Xsqlite3_config(
		tls,
		sqlite3.SQLITE_CONFIG_PCACHE_HDRSZ,
		varArgs2,
	)
	if rc != sqlite3.SQLITE_OK {
		p := sqlite3.Xsqlite3_errstr(tls, rc)
		str := libc.GoString(p)
		panic(fmt.Errorf("sqlite: failed to configure mutex methods: %v", str))
	}

	var sqlitePageSize int32 = 4096            // or your chosen SQLite page size
	var sz int32 = sqlitePageSize + headerSize // 4104 bytes
	var n int32 = pageCacheSize / sz           // number of cache lines

	list := libc.NewVaList(p, sz, n)
	rc = sqlite3.Xsqlite3_config(
		tls,
		sqlite3.SQLITE_CONFIG_PAGECACHE,
		list,
	)
	if rc != sqlite3.SQLITE_OK {
		p := sqlite3.Xsqlite3_errstr(tls, rc)
		str := libc.GoString(p)
		panic(fmt.Errorf("sqlite: failed to configure SQLITE_CONFIG_PAGECACHE: %v", str))
	}
	libc.Xfree(tls, varArgs)
}
