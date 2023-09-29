package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Gender           int
	AgeGroup         int
	MedicalAttention int
}

var ageGroupLabels = map[int]string{
	1: "00 - 04 Años",
	2: "05 - 11 Años",
	3: "12 - 17 Años",
	4: "18 - 29 Años",
	5: "30 - 59 Años",
	6: "60 - más Años",
}

func main() {
	file, err := os.Open("output2.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)

	femeninoStats := make(map[string]struct {
		TotalMedicalAttention int
		Count                 int
	})
	masculinoStats := make(map[string]struct {
		TotalMedicalAttention int
		Count                 int
	})

	femeninoMutex := &sync.Mutex{}
	masculinoMutex := &sync.Mutex{}

	var wg sync.WaitGroup

	chunkSize := 10000 
	chunk := make([]Entry, 0, chunkSize)
	linesProcessed := 0
	startTime := time.Now()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		entry := parseCSVRecord(record)

		chunk = append(chunk, entry)
		linesProcessed++

		if len(chunk) >= chunkSize {
			wg.Add(1)
			go func(chunk []Entry) {
				defer wg.Done()
				processChunk(chunk, femeninoStats, masculinoStats, femeninoMutex, masculinoMutex)
			}(chunk)

			chunk = make([]Entry, 0, chunkSize)
		}
	}


	wg.Wait()

	if len(chunk) > 0 {
		processChunk(chunk, femeninoStats, masculinoStats, femeninoMutex, masculinoMutex)
	}


	elapsedTime := time.Since(startTime)
	printGenderStats("FEMENINO", femeninoStats)

	printGenderStats("MASCULINO", masculinoStats)

	fmt.Printf("%d lines processed in %s\n", linesProcessed, elapsedTime)
}

func parseCSVRecord(record []string) Entry {
	if len(record) < 3 {
		return Entry{}
	}

	gender, err := strconv.Atoi(record[0])
	if err != nil {
		return Entry{}
	}

	ageGroup, err := strconv.Atoi(record[1])
	if err != nil {
		return Entry{}
	}

	medicalAttention, err := strconv.Atoi(record[2])
	if err != nil {
		return Entry{}
	}

	return Entry{
		Gender:           gender,
		AgeGroup:         ageGroup,
		MedicalAttention: medicalAttention,
	}
}

func processChunk(chunk []Entry, femeninoStats, masculinoStats map[string]struct {
	TotalMedicalAttention int
	Count                 int
}, femeninoMutex, masculinoMutex *sync.Mutex) {
	for _, entry := range chunk {
		key := fmt.Sprintf("%d, %d", entry.Gender, entry.AgeGroup)
		if entry.Gender == 1 {
			// MASCULINO
			masculinoMutex.Lock()
			stats := masculinoStats[key]
			stats.TotalMedicalAttention += entry.MedicalAttention
			stats.Count++
			masculinoStats[key] = stats
			masculinoMutex.Unlock()
		} else {
			// FEMENINO
			femeninoMutex.Lock()
			stats := femeninoStats[key]
			stats.TotalMedicalAttention += entry.MedicalAttention
			stats.Count++
			femeninoStats[key] = stats
			femeninoMutex.Unlock()
		}
	}
}

func printGenderStats(gender string, genderStats map[string]struct {
	TotalMedicalAttention int
	Count                 int
}) {
	fmt.Printf("%s:\n", gender)

	keys := make([]string, 0, len(genderStats))
	for key := range genderStats {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		val := genderStats[key]
		parts := strings.Split(key, ", ")
		ageGroupID, _ := strconv.Atoi(parts[1])
		ageGroupLabel, found := ageGroupLabels[ageGroupID]
		if !found {
			ageGroupLabel = "Unknown"
		}
		if ageGroupID != 0 {
			average := float64(val.TotalMedicalAttention) / float64(val.Count)
			fmt.Printf("Age Group: %s, Average Medical Attention: %.2f\n", ageGroupLabel, average)
		}
	}
	fmt.Println()
}
