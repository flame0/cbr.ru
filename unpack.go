package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mholt/archiver"
	"go-dbf/godbf"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func prepare101(file archiver.File, db *sql.DB) {
	if strings.HasSuffix(file.Name(), "B1.DBF") {
		// open output file
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}
		// insert
		stmt, err := db.Prepare("INSERT or IGNORE INTO f101(REGN,PLAN,NUM_SC,A_P,VR,VV,VITG,ORA,OVA,OITGA,ORP,OVP,OITGP,IR,IV,IITG,DT,PRIZ) values(?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?)")
		checkErr(err)

		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)

			t, err := time.Parse("20060102", data[16])
			data[16] = t.Format("2006-01-02")

			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err)
			}

		}

	}
}
func prepare102(file archiver.File, db *sql.DB) {
	if file.Name() == "SPRAV1.DBF" {
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}
		// insert

		stmt, err := db.Prepare("INSERT or IGNORE INTO f102_SPRAV(NOM,PRSTR,CODE,NAME,NAME1) values(?, ?, ?, ?, ?)")
		checkErr(err)

		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			if len(data) == 4 {
				data = append(data, "")
			}
			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err, data)
			}

		}

	} else if strings.HasSuffix(file.Name(), "_P1.DBF") {
		// open output file
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		checkErr(err)
		// insert

		stmt, err := db.Prepare("INSERT or IGNORE INTO f102(REGN,CODE,SIM_R,SIM_V,SIM_ITOGO,DT) values(?, ?, ?, ?, ?, ?)")
		checkErr(err)

		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			if len(data) == 5 {
				data = append(data, "")
			} else {
				t, err := time.Parse("20060102", data[5])
				if err == nil {
					data[5] = t.Format("2006-01-02")
				}
			}

			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err)
			}

		}

	}
}
func prepare123(file archiver.File, db *sql.DB) {
	// Файл mmyyyy_123D.DBF значения показателя
	// Файл mmyyyy_123N.DBF Номенклатура
	// остальные файлы игнорим
	if strings.HasSuffix(file.Name(), "123D.DBF") {
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}
		// insert

		stmt, err := db.Prepare("INSERT or IGNORE INTO f123(REGN,C1,C3,DT) values(?, ?, ?, ?)")
		checkErr(err)
		tmpFileName := strings.Split(file.Name(), "_")
		t, err := time.Parse("012006", tmpFileName[0])
		checkErr(err)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			data = append(data, t.Format("2006-01-02"))
			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err, data)
			}

		}
	} else if strings.HasSuffix(file.Name(), "_123N.DBF") {
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := db.Prepare("INSERT or IGNORE INTO f123N(C1,C2_1,C2_2,C2_3) values(?, ?, ?, ?)")
		checkErr(err)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			dataLen := len(data)
			if dataLen < 4 {
				tmp := make([]string, 4)
				copy(tmp, data)
				data = tmp

			}
			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err, data)
			}

		}
	}

}
func prepare134(file archiver.File, db *sql.DB) {
	if strings.HasSuffix(file.Name(), "134D.dbf") {

		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}
		// insert

		stmt, err := db.Prepare("INSERT or IGNORE INTO f134(REGN,C1,C3,DT) values(?, ?, ?, ?)")
		checkErr(err)
		tmpFileName := strings.Split(file.Name(), "_")
		t, err := time.Parse("012006", tmpFileName[0])
		checkErr(err)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			data = append(data, t.Format("2006-01-02"))
			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err, data)
			}

		}
	} else if strings.HasSuffix(file.Name(), "_134N.dbf") {
		buf := new(bytes.Buffer)
		buf.ReadFrom(file)

		dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := db.Prepare("INSERT or IGNORE INTO f134N(C1,C2_1,C2_2) values(?, ?, ?)")
		checkErr(err)
		for i := 0; i < dbfTable.NumberOfRecords(); i++ {
			data := dbfTable.GetRowAsSlice(i)
			dataLen := len(data)
			if dataLen < 3 {
				tmp := make([]string, 3)
				copy(tmp, data)
				data = tmp

			}
			s := make([]interface{}, len(data))
			for i, v := range data {
				s[i] = v
			}
			_, err = stmt.Exec(s...)
			if err != nil {
				log.Fatalln(err, data)
			}

		}
	} else {
		fmt.Println(file.Name())
	}
}
func stringToInterface(data []string) []interface{} {
	s := make([]interface{}, len(data))
	for i, v := range data {
		s[i] = v
	}
	return s
}
func prepareFile(file archiver.File, db *sql.DB, query string, filterData func([]string) []string) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(file)

	dbfTable, err := godbf.NewFromByteArray(buf.Bytes(), "cp866")
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare(query)
	checkErr(err)
	for i := 0; i < dbfTable.NumberOfRecords(); i++ {
		data := dbfTable.GetRowAsSlice(i)
		if filterData != nil {
			data = filterData(data)
		}
		s := stringToInterface(data)
		_, err = stmt.Exec(s...)
		if err != nil {
			log.Fatalln(err, data)
		}

	}
}
func prepare135(file archiver.File, db *sql.DB) {
	fileName := strings.Split(file.Name(), "_") // mmyyyy_135_1.DBF
	date, err := time.Parse("012006", fileName[0])
	checkErr(err)
	if fileName[1] != "135" {
		return
	}
	var query string
	var filterData func([]string) []string
	makeFilterData := func(num int) func([]string) []string {

		return func(data []string) []string {
			if len(data) < num {
				tmpData := make([]string, num)
				copy(tmpData, data)
				data = tmpData
			}
			data[num-1] = date.Format("2006-01-02")
			return data
		}
	}

	//fmt.Println(fileName)
	switch fileName[2] {
	case "1.dbf":
		query = "INSERT or IGNORE INTO f135_1(REGN, C1_1, C2_1, DT) values(?, ?, ?, ?)"
		filterData = makeFilterData(4)
	case "2.dbf":
		query = "INSERT or IGNORE INTO f135_2(REGN, C1_2, C2_2, DT) values(?, ?, ?, ?)"
		filterData = makeFilterData(4)
	case "3.dbf":
		query = "INSERT or IGNORE INTO f135_3(REGN, C1_3, C2_3, C3_3, C4_3, DT) values(?, ?, ?, ?, ?, ?)"
		filterData = makeFilterData(6)
	case "4.dbf":
		query = "INSERT or IGNORE INTO f135_4(REGN, C1_4, C2_4, C3_4, C4_4, DT) values(?, ?, ?, ?, ?, ?)"
		filterData = makeFilterData(6)
	default:
		fmt.Println(fileName[2])
		return
	}
	if query != "" {
		prepareFile(file, db, query, filterData)
	}

}
func unpack() {
	files, err := ioutil.ReadDir("./data")
	db, err := sql.Open("sqlite3", "./forms.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.Exec("PRAGMA journal_mode=WAL;")

	createStmt := `
 	     CREATE TABLE if not exists f101 (
			REGN	NUMERIC,
			PLAN	VARCHAR(1),
			NUM_SC	VARCHAR(5),
			A_P	 	VARCHAR(1),
			VR		NUMERIC,
			VV		NUMERIC,
			VITG	NUMERIC,
			ORA		NUMERIC,
			OVA		NUMERIC,
			OITGA	NUMERIC,
			ORP		NUMERIC,
			OVP		NUMERIC,
			OITGP	NUMERIC,
			IR		NUMERIC,
			IV		NUMERIC,
			IITG	NUMERIC,
			DT		DATE,
			PRIZ	NUMERIC
		);

		CREATE UNIQUE INDEX if not exists f101_index
		on f101 (REGN, NUM_SC, DT);



		CREATE TABLE if not exists f102_SPRAV (
			  NOM	NUMERIC,
			  PRSTR	NUMERIC,
			  CODE	TEXT,
			  NAME	TEXT,
			  NAME1	TEXT
		);
 	    CREATE TABLE if not exists f102 (
			REGN	NUMERIC,
			CODE	TEXT,
			SIM_R	NUMERIC,
			SIM_V	NUMERIC,
			SIM_ITOGO	NUMERIC,
			DT	DATE
		);




		CREATE TABLE if not exists f123 (
			REGN	NUMERIC,
			C1 VARCHAR(15),
			C3 NUMERIC,
			DT date
		);
		CREATE TABLE if not exists f123N (
			C1 VARCHAR(15),
			C2_1 VARCHAR(240),
			C2_2 VARCHAR(240),
			C2_3 VARCHAR(240)
		);


		CREATE TABLE if not exists f134 (
			REGN	NUMERIC,
			C1 VARCHAR(15),
			C3 NUMERIC,
			DT date
		);
		CREATE TABLE if not exists f134N (
			C1 VARCHAR(15),
			C2_1 VARCHAR(240),
			C2_2 VARCHAR(240),
			C2_3 VARCHAR(240)
		);

CREATE TABLE if not exists f135_1 (
	REGN	NUMERIC,
	C1_1 VARCHAR(10),
	C2_1 NUMERIC,
	DT date
);
CREATE TABLE if not exists f135_2 (
	REGN	NUMERIC,
	C1_2 VARCHAR(10),
	C2_2 NUMERIC,
	DT date
);

CREATE TABLE if not exists f135_3 (
	REGN	NUMERIC,
	C1_3 VARCHAR(6),
	C2_3 NUMERIC,
	C3_3 NUMERIC,
	C4_3 VARCHAR(12),
	DT date
);

CREATE TABLE if not exists f135_4 (
	REGN	NUMERIC,
	C1_4 NUMERIC,
	C2_4 VARCHAR(5),
	C3_4 NUMERIC,
	C4_4 DATE,
	DT date
);

	`
	_, err = db.Exec(createStmt)
	checkErr(err)
	wg := &sync.WaitGroup{}

	maxGoroutines := 10
	guard := make(chan struct{}, maxGoroutines)

	for _, f := range files {
		guard <- struct{}{}
		log.Println(f.Name())
		wg.Add(1)
		go func() {
			unpackFile(f, db)
			<-guard
			wg.Done()
		}()
	}
	wg.Wait()

}

func unpackFile(f os.FileInfo, db *sql.DB) {

	err := archiver.Walk("./data/"+f.Name(), func(file archiver.File) error {
		switch {
		case strings.HasPrefix(f.Name(), "101"):
			prepare101(file, db)
		case strings.HasPrefix(f.Name(), "102"):
			prepare102(file, db)
		case strings.HasPrefix(f.Name(), "123"):
			prepare123(file, db)
		case strings.HasPrefix(f.Name(), "134"):
			prepare134(file, db)
		case strings.HasPrefix(f.Name(), "135"):
			prepare135(file, db)
		default:
			return nil
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
