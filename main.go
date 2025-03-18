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
	"reflect"
	"strconv"
	"sync"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	"modernc.org/sqlite"
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
	mu := sync.Mutex{}
	var conns []uintptr

	driver := sqlite.Driver{}
	driver.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		// extract db from conn with reflection
		dbPtr := uintptr(reflect.ValueOf(conn).Elem().FieldByName("db").Uint())
		mu.Lock()
		defer mu.Unlock()
		conns = append(conns, dbPtr)
		return nil
	})
	sql.Register("sqlite2", &driver)

	tls := libc.NewTLS()

	wg := sync.WaitGroup{}
	var closeFuncs []func() error
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			err, closeFunc := createAndTestDb(10000, 10)
			if err != nil {
				panic(err)
			}
			mu.Lock()
			closeFuncs = append(closeFuncs, closeFunc)
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	printSqliteMemoryUsageForAllDbs(tls, conns)

	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == "memory.allocator" {
			fmt.Println(kv.Value.String())
		}
	})
	
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch
	for _, closeFunc := range closeFuncs {
		if err := closeFunc(); err != nil {
			panic(err)
		}
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "preallocate" {
		if len(os.Args) != 3 {
			fmt.Println("usage: preallocate <page-cache-size-bytes>")
			os.Exit(1)
		}
		pageCacheSizeBytes, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		preallocateCache(int32(pageCacheSizeBytes))
	}
	run()
}

func createAndTestDb(insertsN int, parallelSelects int) (err error, close func() error) {
	dir, err := os.MkdirTemp("", "test-*")
	if err != nil {
		return err, nil
	}

	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "db")

	db, err := sql.Open("sqlite2", fn)
	if err != nil {
		return err, nil
	}

	if _, err = db.Exec(`
drop table if exists t;
create table t(i int, str text);
`); err != nil {
		return err, nil
	}

	if err = inserts(db, insertsN, 100, 10, 1000); err != nil {
		return err, nil
	}
	//fmt.Println("inserts done")

	var roDbs []*sql.DB
	wg := sync.WaitGroup{}
	for i := 0; i < parallelSelects; i++ {
		wg.Add(1)
		roDb, err := sql.Open("sqlite2", fn+"?mode=ro")
		if err != nil {
			return err, nil
		}
		roDbs = append(roDbs, roDb)
		go func() {
			defer wg.Done()
			if err = selects(roDb, insertsN); err != nil {
				panic(err)
			}
			//	fmt.Println("selects done")

		}()
	}
	wg.Wait()

	return nil, func() error {
		for _, roDb := range roDbs {
			err = roDb.Close()
			if err != nil {
				return err
			}
		}
		return db.Close()

	}
}

func printSqliteMemoryUsageForAllDbs(tls *libc.TLS, conns []uintptr) {
	totalPerOp := make(map[int32]int64)

	type dbStats struct {
		current   int32
		highwater int32
	}

	memPtr := libc.Xmalloc(tls, types.Size_t(unsafe.Sizeof(dbStats{})))
	if memPtr == 0 {
		panic(fmt.Errorf("sqlite: cannot allocate memory"))
	}
	stats := (*dbStats)(unsafe.Pointer(memPtr))
	defer func() {
		stats = nil
		libc.Xfree(tls, memPtr)
	}()

	for _, db := range conns {
		var ops = []int32{
			sqlite3.SQLITE_DBSTATUS_CACHE_USED,
			sqlite3.SQLITE_DBSTATUS_LOOKASIDE_USED,
			sqlite3.SQLITE_DBSTATUS_SCHEMA_USED,
			sqlite3.SQLITE_DBSTATUS_STMT_USED,
			sqlite3.SQLITE_DBSTATUS_CACHE_SPILL,
		}
		for _, op := range ops {
			stats.current = 0
			stats.highwater = 0
			retCode := sqlite3.Xsqlite3_db_status(tls, db, op, uintptr(unsafe.Pointer(&stats.current)),
				uintptr(unsafe.Pointer(&stats.highwater)), 0)
			if retCode != sqlite3.SQLITE_OK {
				panic(fmt.Errorf("sqlite: db status: %v", retCode))
			}

			//fmt.Printf("sqlite: db status: %v: current=%v, highwater=%v\n", op, current, highwater)
			totalPerOp[op] += int64(stats.current)
		}
	}
	fmt.Println("sqlite: all connections aggregated statuses:")
	for op, total := range totalPerOp {
		var opStr string
		switch op {
		case sqlite3.SQLITE_DBSTATUS_CACHE_USED:
			opStr = "CACHE_USED"
		case sqlite3.SQLITE_DBSTATUS_LOOKASIDE_USED:
			opStr = "LOOKASIDE_USED"
		case sqlite3.SQLITE_DBSTATUS_SCHEMA_USED:
			opStr = "SCHEMA_USED"
		case sqlite3.SQLITE_DBSTATUS_STMT_USED:
			opStr = "STMT_USED"
		case sqlite3.SQLITE_DBSTATUS_CACHE_SPILL:
			opStr = "CACHE_SPILL"
		default:
			opStr = fmt.Sprintf("%v", op)

		}
		fmt.Printf("%v: %v\n", opStr, total)
	}
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

func preallocateCache(pageCacheSize int32) {
	tls := libc.NewTLS()
	if sqlite3.Xsqlite3_threadsafe(tls) == 0 {
		panic(fmt.Errorf("sqlite: thread safety configuration error"))
	}

	p := libc.Xmalloc(tls, types.Size_t(pageCacheSize))
	if p == 0 {
		panic(fmt.Errorf("cannot allocate memory"))
	}

	headerSizeMem := libc.Xmalloc(tls, 4)
	if headerSizeMem == 0 {
		panic(fmt.Errorf("sqlite: cannot allocate memory for header size"))
	}
	defer libc.Xfree(tls, headerSizeMem)

	*(*int32)(unsafe.Pointer(headerSizeMem)) = 0

	// Create a va_list containing the pointer to headerSize.
	// Unlike SQLITE_CONFIG_SMALL_MALLOC (which takes an int value),
	// SQLITE_CONFIG_PCACHE_HDRSZ expects a pointer to an int.
	varArgs2 := libc.NewVaList(headerSizeMem)
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

	headerSize := *(*int32)(unsafe.Pointer(headerSizeMem))
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
}
