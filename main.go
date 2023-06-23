package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

const metaDirPath = "../json/base/meta"

// const qBufferSize = 1_000_000
const threads = 8 // go routines

type Field struct {
	File   string
	Key    string
	GoType string
	Value  string
}

var fields []Field

func main() {
	var cached *bool
	var query *string

	cached = flag.Bool("cached", false, "load parsed cache from .gob")
	query = flag.String("query", "", "search query")
	flag.Parse()

	if *query == "" {
		log.Fatal("Need -query")
	}

	if !*cached {
		parseJsons()
		writeCache()
	} else {
		readCache()
	}

	// Build graph
	g := graph.New(graph.StringHash)

	var interestingFields []Field
	for _, field := range fields {
		if !strings.Contains(strings.ToLower(field.File), strings.ToLower(*query)) &&
			!strings.Contains(strings.ToLower(field.Key), strings.ToLower(*query)) &&
			!strings.Contains(
				strings.ToLower(field.Value),
				strings.ToLower(*query),
			) {
			continue
		}

		interestingFields = append(interestingFields, field)
	}

	for _, field := range interestingFields {
		// Check if vertex of file name exists
		_, err := g.Vertex(field.File)
		if err != nil {
			g.AddVertex(field.File)
		}
		name := fmt.Sprintf("%s: %s", field.Key, field.Value)
		g.AddVertex(
			name,
		)
		g.AddEdge(field.File, name)
	}

	// Connect keys and values
	for _, fieldA := range interestingFields {
		// Only compare strings/ids/hashs
		if fieldA.GoType != "string" {
			continue
		}
		for _, fieldB := range interestingFields {
			if fieldA.Value == fieldB.Key {
				log.Println("hit")
				nameA := fmt.Sprintf("%s: %s", fieldA.Key, fieldA.Value)
				nameB := fmt.Sprintf("%s: %s", fieldB.Key, fieldB.Value)
				g.AddEdge(nameA, nameB)
			}
		}
	}

	file, err := os.Create("./out.gv")
	if err != nil {
		panic(err)
	}
	err = draw.DOT(g, file)
	if err != nil {
		panic(err)
	}
}

func parseJsons() {
	workC := make(chan string /*, qBufferSize*/)
	for i := 0; i < threads; i++ {
		go worker(workC, i)
	}

	metaDirs, err := os.ReadDir(metaDirPath)
	if err != nil {
		panic(err)
	}
	for _, metaDir := range metaDirs {
		// Skip files
		if !metaDir.IsDir() {
			continue
		}
		// Explore each meta dir
		files, err := os.ReadDir(fmt.Sprintf("%s/%s", metaDirPath, metaDir.Name()))
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			// Skip non jsons
			if !strings.HasSuffix(file.Name(), ".json") {
				continue
			}

			// Enqueue file
			path := fmt.Sprintf("%s/%s/%s", metaDirPath, metaDir.Name(), file.Name())
			workC <- path
		}
	}
}

func writeCache() {
	log.Println("Writing Cache...")
	cacheFile, err := os.Create("cache.gob")
	if err != nil {
		panic(err)
	}
	cacheEncoder := gob.NewEncoder(cacheFile)
	err = cacheEncoder.Encode(fields)
	if err != nil {
		panic(err)
	}

	log.Println("Parsing JSONs Done")
}

func readCache() {
	log.Println("Reading Cache...")
	cacheFile, err := os.Open("cache.gob")
	if err != nil {
		panic(err)
	}
	cacheDecoder := gob.NewDecoder(cacheFile)
	err = cacheDecoder.Decode(&fields)
	if err != nil {
		panic(err)
	}
}

func worker(workC chan string, id int) {
	for file := range workC {
		log.Println("Parsing", file)

		// Read json file
		b, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}

		// Parse json into map
		var data map[string]interface{}
		err = json.Unmarshal(b, &data)
		if err != nil {
			panic(err)
		}

		walkMap(file, "", data)
	}
}

// Recursive walk a map
func walkMap(file string, path string, m map[string]interface{}) {
	for key, value := range m {
		var delimiter string
		if path != "" {
			delimiter = "->"
		}
		fullKey := fmt.Sprintf("%s%s%s", path, delimiter, key)

		switch v := m[key].(type) {
		case map[string]interface{}:
			walkMap(file, fullKey, v)
			return
		}

		//fmt.Println(key, value)
		fields = append(fields, Field{
			file,
			fullKey,
			fmt.Sprintf("%T", value),
			fmt.Sprintf("%v", value),
		})
	}
}
