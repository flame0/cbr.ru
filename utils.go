package main

import (
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"gopkg.in/cheggaaa/pb.v1"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

func getDomain(urlData string) (string, string) {
	u, err := url.Parse(urlData)
	if err != nil {
		panic(err)
	}
	return u.Host, u.Scheme
}

func getAllLinks() []string {
	cookieJar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar: cookieJar,
	}

	// Make HTTP request
	response, err := client.Get(formsUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()

	// Create a goquery document from the HTTP response
	document, err := goquery.NewDocumentFromReader(response.Body)
	if err != nil {
		log.Fatal("Error loading HTTP response body. ", err)
	}

	// Find all links and process them with the function
	// defined earlier
	items := document.Find("a")
	var paths []string
	for i := range items.Nodes {
		element := items.Eq(i)
		href, exists := element.Attr("href")
		if exists {
			paths = append(paths, href)
		}
	}
	return paths
}
func download(pathUrl string, client *http.Client, scheme, domain string) error {
	fileUrl := scheme + "://" + domain + "/" + pathUrl
	var response *http.Response
	var err error
	// Make HTTP request
	retry := 5
	for {
		response, err = client.Get(fileUrl)
		if err != nil && retry == 0 {
			return err
		} else if err != nil && retry > 0 {
			retry = retry - 1
			time.Sleep(5 * time.Second)
			continue
		} else {
			break
		}
	}
	defer response.Body.Close()

	fileName := path.Base(pathUrl)
	out, err := os.Create("data/" + fileName)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, response.Body)
	if err != nil {
		return err
	}
	defer out.Close()

	return nil
}
func downloadFiles(pathUrl string, wg *sync.WaitGroup, ready chan<- string, client *http.Client, scheme, domain string, limit chan struct{}) {
	limit <- struct{}{}
	err := download(pathUrl, client, scheme, domain)
	if err != nil {
		log.Println("Error downloading file ", err)
	}
	fileName := path.Base(pathUrl)
	ready <- fileName
	<-limit
	wg.Done()

}
func downloadAll(gte, lte string) {

	prefixes := []string{
		"101",
		"102",
		"123",
		"134",
		"135",
	}
	paths := getAllLinks()
	sort.Strings(paths)

	filteredPath := make([]string, 0, 200)
	for _, pathUrl := range paths {
		fileName := path.Base(pathUrl)
		for _, prefix := range prefixes {
			if strings.HasPrefix(fileName, prefix) {
				s := strings.Split(fileName, ".")
				onlyName := s[0]
				s = strings.Split(onlyName, "-")
				dateName := s[1]
				//println(dateName)
				if dateName >= gte {
					filteredPath = append(filteredPath, pathUrl)
				}
			}

		}
	}

	ready := make(chan string, 50)
	bar := pb.StartNew(len(filteredPath))

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := *defaultTransportPointer // dereference it to get a copy of the struct that the pointer points to
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100

	cookieJar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:       cookieJar,
		Transport: &defaultTransport,
	}
	go func() {
		wg := &sync.WaitGroup{}
		limit := make(chan struct{}, 20)
		domain, scheme := getDomain(formsUrl)

		for _, path := range filteredPath {
			wg.Add(1)
			go downloadFiles(path, wg, ready, client, scheme, domain, limit)
		}
		wg.Wait()
		close(ready)
	}()

	for i := range ready {
		func(i string) {}(i)
		bar.Increment()
	}
	bar.FinishPrint("The End!")
}
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
